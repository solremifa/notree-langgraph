"""작업 하나를 넣어 본다 (api 역할 흉내).

    uv run python -m notree.dev.enqueue echo text=hi sleep=3
    uv run python -m notree.dev.enqueue echo fail=retry:1
    uv run python -m notree.dev.enqueue course_update sources=hello,big,world

값 규칙: 숫자·true/false는 그대로, 쉼표가 있으면 문자열 목록, 나머지는 문자열.
(PowerShell은 JSON 따옴표를 깨뜨리기 쉬워서 key=value 방식을 쓴다)
"""
import argparse
import asyncio
import json

from notree.common import settings as S
from notree.worker.mcp_client import McpClient


def parse_value(v: str):
    if "," in v:
        return [x.strip() for x in v.split(",") if x.strip()]
    try:
        return json.loads(v)
    except ValueError:
        return v


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("kind")
    p.add_argument("fields", nargs="*", help="key=value ...")
    p.add_argument("--stream", choices=sorted(S.STREAMS), default=None, help="생략하면 작업 종류별 기본 Stream")
    a = p.parse_args()
    payload = {k: parse_value(v) for k, v in (f.split("=", 1) for f in a.fields)}
    async with McpClient(url=S.MCP_URL) as mcp:
        stream = S.STREAMS[a.stream].stream if a.stream else None
        job = await mcp.call("job_create", kind=a.kind, payload=payload, stream=stream)
        print(f"{job['job_id']}  {a.kind} {payload}")


if __name__ == "__main__":
    asyncio.run(main())
