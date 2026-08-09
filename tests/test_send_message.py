import json
import threading
import time
from contextlib import contextmanager

import pytest
from websockets.sync.server import serve

from msgbusviz import Client, ClientError


@contextmanager
def fake_server(received: list):
    def handler(websocket):
        websocket.send(json.dumps({"type": "hello", "protocolVersion": 1, "config": {}}))
        for msg in websocket:
            received.append(json.loads(msg))

    server = serve(handler, "127.0.0.1", 0)
    port = server.socket.getsockname()[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        yield f"ws://127.0.0.1:{port}"
    finally:
        server.shutdown()


def test_send_message_round_trip():
    received: list[dict] = []
    with fake_server(received) as url:
        c = Client(url=url, reconnect=False)
        c.connect(timeout=2.0)
        c.send_message("orders", from_="A", to="B", label="x")
        for _ in range(50):
            if received:
                break
            time.sleep(0.05)
        c.close()
    assert received, "no message received by server"
    msg = received[0]
    assert msg["type"] == "sendMessage"
    assert msg["from"] == "A"
    assert msg["to"] == "B"
    assert msg["label"] == "x"


def test_invalid_send_message_raises():
    received: list[dict] = []
    with fake_server(received) as url:
        c = Client(url=url, reconnect=False)
        c.connect(timeout=2.0)
        with pytest.raises(ClientError, match="invalid message"):
            c.send_message("orders", color="lime")
        c.close()
