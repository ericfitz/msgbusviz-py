import asyncio
import json
from typing import Any

import websockets

from ._schema import PROTOCOL_VERSION, validate_message


class AsyncClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self._ws: Any = None
        self._reader_task: asyncio.Task | None = None

    async def connect(self) -> None:
        self._ws = await websockets.connect(self.url)
        hello = json.loads(await self._ws.recv())
        if hello.get("type") != "hello" or hello.get("protocolVersion") != PROTOCOL_VERSION:
            raise RuntimeError(f"protocol mismatch: {hello}")

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def send_message(
        self,
        channel: str,
        *,
        from_: str | None = None,
        to: str | None = None,
        label: str | None = None,
        color: str | None = None,
    ) -> None:
        msg: dict[str, Any] = {"type": "sendMessage", "channel": channel}
        if from_ is not None:
            msg["from"] = from_
        if to is not None:
            msg["to"] = to
        if label is not None:
            msg["label"] = label
        if color is not None:
            msg["color"] = color
        ok, errs = validate_message(msg)
        if not ok:
            raise ValueError(f"invalid message: {errs}")
        if self._ws is None:
            raise RuntimeError("not connected")
        await self._ws.send(json.dumps(msg))
