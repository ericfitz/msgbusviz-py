import json
import os
import shutil
import subprocess
import time
from pathlib import Path
import urllib.request

import pytest

from msgbusviz import Client

CONFIG_YAML = """
version: 1
layout: { mode: force }
nodes:
  Pub: { model: cube }
  Sub: { model: cube }
channels:
  evt: { publishers: [Pub], subscribers: [Sub] }
"""


@pytest.fixture
def sidecar(tmp_path: Path):
    msgbusviz_root = Path(os.environ.get("MSGBUSVIZ_ROOT", "../msgbusviz")).resolve()
    if not (msgbusviz_root / "packages" / "server" / "dist" / "cli.js").exists():
        pytest.skip(f"msgbusviz not built at {msgbusviz_root}")

    cfg = tmp_path / "config.yaml"
    cfg.write_text(CONFIG_YAML)

    proc = subprocess.Popen(
        ["node", str(msgbusviz_root / "packages" / "server" / "dist" / "cli.js"),
         "serve", str(cfg), "--host", "127.0.0.1", "--port", "0", "--no-open"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )

    port: int | None = None
    deadline = time.time() + 5.0
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.01); continue
        if "listening on" in line:
            raw = line.rsplit(":", 1)[-1].strip().rstrip('"}').strip()
            port = int(raw)
            break
    if port is None:
        proc.kill(); pytest.fail("sidecar didn't print listening line")

    for _ in range(50):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz") as r:
                if r.status == 200:
                    break
        except Exception:
            time.sleep(0.05)

    yield f"ws://127.0.0.1:{port}/ws"
    proc.kill()


def test_python_client_against_node_sidecar(sidecar):
    pytest.skip("requires websocket-client; baseline test_send_message_round_trip already covers protocol")
