"""내부 MCP 서버 클라이언트.

worker가 바깥과 주고받는 통로는 Redis, OpenAI, 그리고 이 클라이언트뿐이다. DB 접속 정보는 모른다.
url이 있으면 streamable HTTP(compose 배치), 없으면 저장소 루트의 mcp_server.py를 stdio 하위 프로세스로 띄운다(테스트용).
"""
import json
import sys
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

SERVER = Path(__file__).resolve().parents[2] / "mcp_server.py"


class McpToolError(RuntimeError):
    """MCP 도구가 오류를 돌려줌 (인자 오류, 서버 예외 등)."""


class McpClient:
    """내부 MCP 서버에 붙어 도구를 호출한다.
    url이 있으면 streamable HTTP(실제 compose 배치와 같은 방식), 없으면 mcp_server.py를 stdio 하위 프로세스로 띄운다."""

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

    async def call(self, tool: str, **args: Any) -> dict:
        self.calls.append(tool)
        res = await self.session.call_tool(tool, args)
        if res.isError:
            raise McpToolError(f"MCP {tool} 실패: {res.content}")
        if res.structuredContent is not None:
            data = res.structuredContent
            return data.get("result", data) if set(data) == {"result"} else data
        return json.loads(res.content[0].text)
