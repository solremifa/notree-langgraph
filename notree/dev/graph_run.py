"""과목 노트 그래프만 따로 돌려 보는 실행기 (Redis·worker 없이).

    uv run python -m notree.dev.graph_run                         # 정상 경로
    uv run python -m notree.dev.graph_run --sloppy once           # validate 실패 → merge 재시도
    uv run python -m notree.dev.graph_run --thread op1 --crash-at extract:world
    uv run python -m notree.dev.graph_run --thread op1 --resume   # 체크포인트에서 이어감
    uv run python -m notree.dev.graph_run --graph                 # Mermaid
    uv run python -m notree.dev.graph_run --help
"""
import argparse
import asyncio
import os
import time
from collections import Counter
from contextlib import asynccontextmanager
from pathlib import Path

from notree.graphs.course_update import build_graph, llm

OUT = Path("out")


@asynccontextmanager
async def open_backend(kind: str):
    """(checkpointer, 발행 저장소, MCP 클라이언트 또는 None)을 연다."""
    OUT.mkdir(exist_ok=True)
    if kind == "mcp":
        # A안: worker는 DB를 열지 않는다. MCP 서버 프로세스만 out/notree.db를 연다.
        from notree.worker.checkpointer import McpCheckpointSaver, McpNoteStore
        from notree.worker.mcp_client import McpClient
        env = {**os.environ, "NOTREE_DB": str((OUT / "notree.db").resolve())}
        async with McpClient(env=env) as client:
            yield McpCheckpointSaver(client), McpNoteStore(client), client
    else:
        # 비교용: worker가 SQLite 파일에 직접 붙는 방식 (B안과 같은 모양)
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
        async with AsyncSqliteSaver.from_conn_string(str(OUT / "checkpoints.sqlite")) as saver:
            yield saver, None, None


async def main() -> None:
    p = argparse.ArgumentParser(description="Notree 과목 노트 업데이트 모양의 LangGraph hello world")
    p.add_argument("--saver", choices=["mcp", "sqlite"], default="mcp",
                   help="mcp=체크포인트·발행을 MCP 도구로만(A안, 기본) / sqlite=worker가 파일 DB에 직접")
    p.add_argument("--thread", default=None, help="operation_id 역할. 같은 값으로 --resume 하면 이어서 실행")
    p.add_argument("--lectures", default="hello,world", help="출처 강의(쉼표 구분). 개수만큼 extract가 병렬로 뜸")
    p.add_argument("--llm", choices=["fake", "openai"], default="fake")
    p.add_argument("--sloppy", choices=["once", "always"], default=None,
                   help="fake LLM의 merge가 단어를 빠뜨림. once=재시도 분기, always=확인 필요 분기")
    p.add_argument("--crash-at", default=None, help="extract:<강의> 또는 publish")
    p.add_argument("--resume", action="store_true", help="마지막 체크포인트부터 이어서 실행")
    p.add_argument("--history", action="store_true", help="이 thread의 체크포인트 이력만 출력")
    p.add_argument("--graph", action="store_true", help="그래프 구조(Mermaid)만 출력")
    a = p.parse_args()

    if a.graph:
        mmd = build_graph().get_graph().draw_mermaid()
        OUT.mkdir(exist_ok=True); (OUT / "graph.mmd").write_text(mmd, "utf-8")
        print(mmd); print(f"→ {OUT/'graph.mmd'} 저장 (mermaid.live 에 붙여 넣으면 그림으로 보임)")
        return

    thread = a.thread or f"op-{int(time.time())}"
    async with open_backend(a.saver) as (saver, notes, client):
        app = build_graph(saver)
        config = {"configurable": {
            "thread_id": thread, "llm": a.llm, "sloppy": a.sloppy, "notes": notes,
            "crash_at": None if a.resume else a.crash_at, "out_dir": str(OUT),
        }}

        if a.history:
            print(f"thread {thread} 체크포인트 이력 (최신 → 과거)  saver={a.saver}")
            async for s in app.aget_state_history(config):
                m = s.metadata or {}
                print(f"  step {m.get('step'):>2}  source={m.get('source'):<6} 다음={list(s.next) or '끝'}")
            return

        if a.resume:
            snap = await app.aget_state(config)
            if not snap.next:
                print(f"thread {thread}: 이어서 할 작업이 없습니다(이미 끝났거나 없는 thread)."); return
            print(f"▶ 이어하기 thread={thread}  saver={a.saver}  남은 노드={list(snap.next)}")
            inp = None                                   # None = 마지막 체크포인트부터 계속
        else:
            print(f"▶ 시작 thread={thread}  saver={a.saver}  llm={a.llm}  lectures={a.lectures}"
                  + (f"  crash-at={a.crash_at}" if a.crash_at else ""))
            inp = {"course": "자료구조", "sources": [s.strip() for s in a.lectures.split(",") if s.strip()]}

        mark = len(client.calls) if client else 0
        try:
            async for mode, chunk in app.astream(inp, config, stream_mode=["updates", "checkpoints"],
                                                 durability="sync"):
                if mode == "checkpoints":
                    step = chunk["metadata"].get("step")
                    print(f"      💾 checkpoint step={step}  다음={chunk['next'] or '끝'}")
        except RuntimeError as e:
            snap = await app.aget_state(config)
            print(f"\n✖ {e}\n  남은 노드={list(snap.next)}  저장된 extract={len(snap.values.get('extracts', []))}개")
            print(f"  이어서 실행: uv run python -m notree.dev.graph_run --saver {a.saver} --thread {thread} --resume")
            return
        finally:
            print(f"  LLM 호출 {len(llm.CALLS)}회: {llm.CALLS}")
            if client:
                print(f"  MCP 도구 호출: {dict(Counter(client.calls[mark:]))}")

        final = (await app.aget_state(config)).values
        where = "MCP note_publish → out/notree.db" if a.saver == "mcp" else str(OUT / "published.json")
        print(f"■ 결과: {final.get('draft')!r} [{final.get('status')}]  → {where}")


if __name__ == "__main__":
    asyncio.run(main())
