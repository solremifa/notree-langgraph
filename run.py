"""실행기.  python run.py --help"""
import argparse
import time
from pathlib import Path

from langgraph.checkpoint.sqlite import SqliteSaver

import llm
from graph import build_graph


def main() -> None:
    p = argparse.ArgumentParser(description="Notree 과목 노트 업데이트 모양의 LangGraph hello world")
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

    out = Path("out"); out.mkdir(exist_ok=True)
    thread = a.thread or f"op-{int(time.time())}"
    config = {"configurable": {
        "thread_id": thread, "llm": a.llm, "sloppy": a.sloppy,
        "crash_at": None if a.resume else a.crash_at, "out_dir": str(out),
    }}

    with SqliteSaver.from_conn_string(str(out / "checkpoints.sqlite")) as saver:
        app = build_graph(saver)

        if a.graph:
            mmd = app.get_graph().draw_mermaid()
            (out / "graph.mmd").write_text(mmd, "utf-8")
            print(mmd); print(f"→ {out/'graph.mmd'} 저장 (mermaid.live 에 붙여 넣으면 그림으로 보임)")
            return

        if a.history:
            print(f"thread {thread} 체크포인트 이력 (최신 → 과거)")
            for s in app.get_state_history(config):
                m = s.metadata or {}
                print(f"  step {m.get('step'):>2}  source={m.get('source'):<6} 다음={list(s.next) or '끝'}")
            return

        if a.resume:
            snap = app.get_state(config)
            if not snap.next:
                print(f"thread {thread}: 이어서 할 작업이 없습니다(이미 끝났거나 없는 thread)."); return
            print(f"▶ 이어하기 thread={thread}  남은 노드={list(snap.next)}")
            inp = None                                   # None = 마지막 체크포인트부터 계속
        else:
            print(f"▶ 시작 thread={thread}  llm={a.llm}  lectures={a.lectures}"
                  + (f"  crash-at={a.crash_at}" if a.crash_at else ""))
            inp = {"course": "자료구조", "sources": [s.strip() for s in a.lectures.split(",") if s.strip()]}

        try:
            for mode, chunk in app.stream(inp, config, stream_mode=["updates", "checkpoints"], durability="sync"):
                if mode == "checkpoints":
                    step = chunk["metadata"].get("step")
                    print(f"      💾 checkpoint step={step}  다음={chunk['next'] or '끝'}")
        except RuntimeError as e:
            snap = app.get_state(config)
            print(f"\n✖ {e}\n  남은 노드={list(snap.next)}  저장된 extract={len(snap.values.get('extracts', []))}개")
            print(f"  이어서 실행: python run.py --thread {thread} --resume")
            return
        finally:
            print(f"  LLM 호출 {len(llm.CALLS)}회: {llm.CALLS}")

        final = app.get_state(config).values
        print(f"■ 결과: {final.get('draft')!r} [{final.get('status')}]  → {out/'published.json'}")


if __name__ == "__main__":
    main()
