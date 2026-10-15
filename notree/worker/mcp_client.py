"""내부 MCP 서버 클라이언트.

worker가 바깥과 주고받는 통로는 Redis, OpenAI, 그리고 이 클라이언트뿐이다. DB 접속 정보는 모른다.
url이 있으면 streamable HTTP(compose 배치), 없으면 notree/mcp_server/server.py를 stdio 하위 프로세스로 띄운다(테스트용).
"""
import json
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from notree.contracts.envelope import ToolError, unwrap  # noqa: F401  (ToolError 재노출)

SERVER = Path(__file__).resolve().parents[1] / "mcp_server" / "server.py"


class McpToolError(RuntimeError):
    """MCP 프로토콜 수준 오류 (도구 이름 없음, 인자 스키마 불일치 등). 도구의 '거절'은 ToolError."""


class McpClient:
    """내부 MCP 서버에 붙어 도구를 호출한다.
    url이 있으면 streamable HTTP(실제 compose 배치와 같은 방식), 없으면 notree/mcp_server/server.py를 stdio 하위 프로세스로 띄운다."""

    def __init__(self, env: dict[str, str] | None = None, url: str | None = None) -> None:
        self.env = env
        self.url = url
        self.calls: list[str] = []
        self._stack = AsyncExitStack()
        self.session: ClientSession | None = None

    async def __aenter__(self) -> "McpClient":
        if self.url:
            read, write, _ = await self._stack.enter_async_context(streamablehttp_client(self.url))
        else:
            params = StdioServerParameters(command=sys.executable, args=[str(SERVER)], env=self.env)
            read, write = await self._stack.enter_async_context(stdio_client(params))
        self.session = await self._stack.enter_async_context(ClientSession(read, write))
        await self.session.initialize()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._stack.aclose()

    async def tool(self, name: str, **args: Any) -> dict:
        """공통 응답 형식을 풀어 data만 돌려준다. 거절(ok=false)이면 ToolError (code, retryable 포함).
        permit이 None이면 보내지 않는다 (작업 밖에서 부르는 PoC 스크립트용)."""
        if args.get("permit", "") is None:
            args.pop("permit")
        return unwrap(name, await self.call(name, **args))

    async def call(self, tool: str, **args: Any) -> dict:
        """날것 그대로의 응답 (envelope 포함)."""
        self.calls.append(tool)
        res = await self.session.call_tool(tool, args)
        if res.isError:
            raise McpToolError(f"MCP {tool} 실패: {res.content}")
        if res.structuredContent is not None:
            data = res.structuredContent
            return data.get("result", data) if set(data) == {"result"} else data
        return json.loads(res.content[0].text)
