"""A안: MCP 도구로만 체크포인트를 읽고 쓰는 LangGraph 저장기.

worker(이 프로세스)는 DB 접속 정보를 모른다. 체크포인트는 LangGraph의 serde로 바이트로 만든 뒤
base64로 MCP 서버에 넘기고, 서버는 그것을 불투명 blob으로 저장한다.
"""
import base64
import json
import sys
from collections.abc import AsyncIterator, Sequence
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

SERVER = Path(__file__).with_name("mcp_server.py")


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
            raise RuntimeError(f"MCP {tool} 실패: {res.content}")
        if res.structuredContent is not None:
            data = res.structuredContent
            return data.get("result", data) if set(data) == {"result"} else data
        return json.loads(res.content[0].text)


def _enc(typed: tuple[str, bytes]) -> list[str]:
    return [typed[0], base64.b64encode(typed[1]).decode()]


def _dec(pair: list[str]) -> tuple[str, bytes]:
    return pair[0], base64.b64decode(pair[1])


class McpCheckpointSaver(BaseCheckpointSaver):
    """BaseCheckpointSaver의 비동기 메서드를 MCP 도구 호출로 구현. 그래프는 ainvoke/astream으로 돌린다."""

    def __init__(self, client: McpClient) -> None:
        super().__init__()
        self.client = client

    # ── 읽기 ──
    def _to_tuple(self, d: dict) -> CheckpointTuple:
        cfg = {"configurable": {"thread_id": d["thread_id"], "checkpoint_ns": d["ns"],
                                "checkpoint_id": d["checkpoint_id"]}}
        parent = ({"configurable": {"thread_id": d["thread_id"], "checkpoint_ns": d["ns"],
                                    "checkpoint_id": d["parent_id"]}} if d["parent_id"] else None)
        return CheckpointTuple(
            config=cfg,
            checkpoint=self.serde.loads_typed(_dec(d["checkpoint"])),
            metadata=self.serde.loads_typed(_dec(d["metadata"])),
            parent_config=parent,
            pending_writes=[(t, ch, self.serde.loads_typed(_dec(v))) for t, ch, v, _p, _i in d["writes"]],
        )

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        c = config["configurable"]
        d = await self.client.call("checkpoint_get", thread_id=c["thread_id"],
                                   ns=c.get("checkpoint_ns", ""), checkpoint_id=get_checkpoint_id(config))
        return self._to_tuple(d) if d.get("found") else None

    async def alist(self, config: RunnableConfig | None, *, filter: dict[str, Any] | None = None,
                    before: RunnableConfig | None = None, limit: int | None = None
                    ) -> AsyncIterator[CheckpointTuple]:
        c = (config or {}).get("configurable", {})
        d = await self.client.call("checkpoint_list", thread_id=c["thread_id"], ns=c.get("checkpoint_ns"),
                                   before=get_checkpoint_id(before) if before else None, limit=None)
        n = 0
        for item in d["items"]:
            t = self._to_tuple(item)
            if filter and any(t.metadata.get(k) != v for k, v in filter.items()):
                continue
            yield t
            n += 1
            if limit and n >= limit:
                break

    # ── 쓰기 ──
    async def aput(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata,
                   new_versions: ChannelVersions) -> RunnableConfig:
        c = config["configurable"]
        ns = c.get("checkpoint_ns", "")
        await self.client.call(
            "checkpoint_put", thread_id=c["thread_id"], ns=ns, checkpoint_id=checkpoint["id"],
            parent_id=c.get("checkpoint_id"),
            checkpoint=_enc(self.serde.dumps_typed(checkpoint)),
            metadata=_enc(self.serde.dumps_typed(get_checkpoint_metadata(config, metadata))),
        )
        return {"configurable": {"thread_id": c["thread_id"], "checkpoint_ns": ns,
                                 "checkpoint_id": checkpoint["id"]}}

    async def aput_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str,
                          task_path: str = "") -> None:
        c = config["configurable"]
        rows = [[WRITES_IDX_MAP.get(ch, i), ch, *_enc(self.serde.dumps_typed(v))] for i, (ch, v) in enumerate(writes)]
        await self.client.call("checkpoint_put_writes", thread_id=c["thread_id"], ns=c.get("checkpoint_ns", ""),
                               checkpoint_id=c["checkpoint_id"], task_id=task_id, task_path=task_path, writes=rows)

    async def adelete_thread(self, thread_id: str) -> None:
        await self.client.call("checkpoint_delete_thread", thread_id=thread_id)

    # ── 동기 API는 쓰지 않는다 (MCP 클라이언트가 비동기) ──
    def get_tuple(self, config):  # noqa: D102
        raise NotImplementedError("McpCheckpointSaver는 비동기 전용입니다. ainvoke/astream을 쓰세요.")

    def list(self, *a, **k):  # noqa: D102
        raise NotImplementedError("비동기 전용 (alist)")

    def put(self, *a, **k):  # noqa: D102
        raise NotImplementedError("비동기 전용 (aput)")

    def put_writes(self, *a, **k):  # noqa: D102
        raise NotImplementedError("비동기 전용 (aput_writes)")


class McpNoteStore:
    """publish 노드가 쓰는 발행 저장소 (MCP note_publish)."""

    def __init__(self, client: McpClient) -> None:
        self.client = client

    async def publish(self, key: str, course: str, note: str, status: str) -> bool:
        return (await self.client.call("note_publish", idem_key=key, course=course or "", note=note,
                                       status=status))["published"]
