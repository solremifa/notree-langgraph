"""질문 하나를 넣고 답이 저장될 때까지 기다려 본다 (api·화면 역할 흉내).

    uv run python -m notree.dev.qa_ask "스택 오버플로는 왜 생겨?"
    uv run python -m notree.dev.qa_ask "힙 정렬의 시간 복잡도는?" --user u-123 --course c-ds

MCP 서버(--http)·배정기·qa worker(--stream qa)가 떠 있어야 한다. 처음 쓰는 과목이면 데모 말뭉치(자료구조)를 넣는다.
"""
import argparse
import asyncio
import json
import time

from notree.common import settings as S
from notree.graphs.qa.retriever import sample_corpus
from notree.worker.mcp_client import McpClient

LECTURE = {"n1": "L1", "n2": "L1", "n3": "L2", "t1": "L1", "t2": "L1"}


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("question")
    p.add_argument("--user", default="demo-user")
    p.add_argument("--course", default="c-ds")
    p.add_argument("--wait", type=float, default=100, help="최대 기다리는 초")
    a = p.parse_args()
    async with McpClient(url=S.MCP_URL) as mcp:
        docs = [{"source": s, "id": i, "text": t, "lecture_id": LECTURE.get(i)}
                for s, items in sample_corpus().items() for i, t in items]
        await mcp.tool("qa_seed", user_id=a.user, course_id=a.course, course_name="자료구조", docs=docs)
        q = await mcp.tool("qa_ask", user_id=a.user, course_id=a.course, question=a.question)
        print(f"{q['question_id']}  작업 {q['job_id']}  Q. {a.question}")
        end = time.time() + a.wait
        while time.time() < end:
            got = await mcp.tool("qa_get", question_id=q["question_id"], user_id=a.user)
            job = await mcp.tool("job_get", job_id=q["job_id"])
            if got["state"] == "answered" or job["state"] == "failed":
                break
            await asyncio.sleep(0.5)
        if got["state"] == "answered":
            ans = got["answer"]
            print(f"A. [{ans['label']} · {ans['status']}] {ans['answer']}")
            for s in ans["sources"]:
                print(f"   [^{s['id']}] {s['source']}: {s['text']}")
        else:
            print(f"답 없음: 작업 {job['state']} {job.get('fail_reason') or ''}")
        print(json.dumps(got, ensure_ascii=False, indent=2) if got.get("answer") else "")


if __name__ == "__main__":
    asyncio.run(main())
