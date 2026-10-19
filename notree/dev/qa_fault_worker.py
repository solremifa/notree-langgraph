"""시험용 qa worker: 실제 Redis·MCP 서버에 붙되, 장애를 일부러 낸다 (실제 프로세스로 재검증할 때만).

    uv run python -m notree.dev.qa_fault_worker --lose-complete-response 1

--lose-complete-response N : qa_complete를 서버에 전달(저장 커밋)한 뒤 응답을 N번 잃어버린다
                             → 루프가 lease를 반납하고, 재배송 기준(qa 20초) 뒤 다시 처리
LLM은 가짜이고, 호출할 때마다 'LLM 호출' 로그를 남긴다 (재시도 때 다시 불렸는지 보려고).
"""
import argparse
import asyncio

import redis.asyncio as aioredis

from notree.common import settings as S
from notree.common.log import log
from notree.graphs.qa.llm import FakeQALLM
from notree.worker.loop import Worker
from notree.worker.main import install_stop_signals
from notree.worker.mcp_client import McpClient


class LoseResponse:
    def __init__(self, mcp: McpClient, tool: str, times: int) -> None:
        self.mcp, self.tool_name, self.left = mcp, tool, times

    async def tool(self, name: str, **args):
        res = await self.mcp.tool(name, **args)
        if name == self.tool_name and self.left > 0:
            self.left -= 1
            log("fault", f"{name} 저장은 됐지만 응답을 잃어버림 (남은 횟수 {self.left})")
            raise ConnectionResetError("응답 유실 (시험)")
        return res


class LoggingLLM(FakeQALLM):
    async def understand(self, question, course, **kw):          # kw: 대화 속 질문이면 history·memory_state
        log("llm", f"LLM 호출: understand (질문 {len(question)}자, 이력 {len(kw.get('history') or [])}개)")
        return await super().understand(question, course, **kw)

    async def answer(self, question, evidence, attempt, problem="", related=True, **kw):
        log("llm", f"LLM 호출: answer#{attempt}")
        return await super().answer(question, evidence, attempt, problem, related, **kw)


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--name", default="F")
    p.add_argument("--lose-complete-response", type=int, default=0)
    a = p.parse_args()
    stop = asyncio.Event()
    install_stop_signals(stop)
    r = aioredis.from_url(S.REDIS_URL, decode_responses=True)
    async with McpClient(url=S.MCP_URL) as mcp:
        wrapped = LoseResponse(mcp, "qa_complete", a.lose_complete_response)
        await Worker(a.name, S.QA, r, wrapped, extras={"qa_llm": LoggingLLM()}).run(stop)
    await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
