import asyncio
import json
import threading
from collections import deque
from typing import Any, Callable, Optional

import websockets

from ._schema import PROTOCOL_VERSION, validate_message


class ClientError(Exception):
    pass


class Client:
    def __init__(
        self,
        url: str,
        *,
        reconnect: bool = True,
        max_queue: int = 1000,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> None:
        self.url = url
        self.reconnect = reconnect
        self.max_queue = max_queue
        self.on_error = on_error or (lambda e: None)

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._closed = threading.Event()
        self._connected = threading.Event()
        self._ws: Any = None
        self._queue: deque[dict] = deque()
        self._lock = threading.Lock()
        self._connect_error: Exception | None = None

    def connect(self, timeout: float = 5.0) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._ready.wait(timeout=timeout)
        if not self._connected.is_set():
            if self._connect_error:
                raise self._connect_error
            raise ClientError("connect timed out")

    def close(self) -> None:
        self._closed.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=2.0)

    def send_message(
        self,
        channel: str,
        *,
        from_: Optional[str] = None,
        to: Optional[str] = None,
        label: Optional[str] = None,
        color: Optional[str] = None,
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
        self._enqueue(msg)

    def update_channel(
        self,
        channel: str,
        *,
        color: Optional[str] = None,
        speed: Optional[float] = None,
        size: Optional[float] = None,
        message_model: Optional[str] = None,
    ) -> None:
        patch: dict[str, Any] = {}
        if color is not None:
            patch["color"] = color
        if speed is not None:
            patch["speed"] = speed
        if size is not None:
            patch["size"] = size
        if message_model is not None:
            patch["messageModel"] = message_model
        if not patch:
            raise ValueError("update_channel requires at least one field")
        self._enqueue({"type": "updateChannel", "channel": channel, "patch": patch})

    def _enqueue(self, msg: dict) -> None:
        ok, errors = validate_message(msg)
        if not ok:
            raise ClientError(f"invalid message: {errors}")
        with self._lock:
            if len(self._queue) >= self.max_queue:
                self._queue.popleft()
                self.on_error(ClientError("queue overflow"))
            self._queue.append(msg)
        if self._loop and self._connected.is_set():
            self._loop.call_soon_threadsafe(self._wake)

    def _wake(self) -> None:
        pass

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main())
        except Exception as err:
            self._connect_error = err
            self.on_error(err)
        finally:
            self._ready.set()
            self._loop.close()

    async def _main(self) -> None:
        backoff = 0.25
        while not self._closed.is_set():
            try:
                async with websockets.connect(self.url) as ws:
                    self._ws = ws
                    hello_raw = await ws.recv()
                    hello = json.loads(hello_raw)
                    if hello.get("type") != "hello" or hello.get("protocolVersion") != PROTOCOL_VERSION:
                        raise ClientError(f"protocol mismatch: {hello}")
                    self._connected.set()
                    self._ready.set()
                    backoff = 0.25
                    await asyncio.gather(self._sender(ws), self._reader(ws))
            except Exception as err:
                self._connected.clear()
                self._ws = None
                self.on_error(err)
                if not self.reconnect or self._closed.is_set():
                    self._ready.set()
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _sender(self, ws: Any) -> None:
        while not self._closed.is_set():
            msg = None
            with self._lock:
                if self._queue:
                    msg = self._queue.popleft()
            if msg is None:
                await asyncio.sleep(0.005)
                continue
            await ws.send(json.dumps(msg))

    async def _reader(self, ws: Any) -> None:
        async for raw in ws:
            try:
                obj = json.loads(raw)
            except Exception as err:
                self.on_error(err)
                continue
            if obj.get("type") == "error":
                self.on_error(ClientError(f"{obj.get('code')}: {obj.get('message')}"))
