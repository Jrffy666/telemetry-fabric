#!/usr/bin/env python3
"""Start the mock EVM RPC server and exercise core JSON-RPC methods."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "fixtures" / "rpc" / "mock_evm_rpc.py"
SCENARIO = ROOT / "fixtures" / "rpc" / "scenario_reorg_gap_duplicate.json"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def rpc(endpoint: str, method: str, params: list[Any] | None = None) -> Any:
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    ).encode("utf-8")
    request = Request(endpoint, data=body, headers={"content-type": "application/json"})
    with urlopen(request, timeout=2) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if "error" in payload:
        raise AssertionError(payload["error"])
    return payload["result"]


def wait_for_server(endpoint: str) -> None:
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            rpc(endpoint, "telemetry_health")
            return
        except (ConnectionError, URLError, TimeoutError):
            time.sleep(0.1)
    raise TimeoutError(f"mock RPC did not start: {endpoint}")


def main() -> None:
    port = free_port()
    endpoint = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            sys.executable,
            str(SERVER),
            "--scenario",
            str(SCENARIO),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_server(endpoint)
        assert rpc(endpoint, "eth_chainId") == "0x1"
        assert rpc(endpoint, "eth_blockNumber") == "0x13"
        assert rpc(endpoint, "eth_getBlockByNumber", ["0x11", False]) is None
        logs = rpc(endpoint, "eth_getLogs", [{"fromBlock": "0x12", "toBlock": "0x12"}])
        assert len(logs) == 2
        reorg = rpc(endpoint, "telemetry_simulateReorg")
        assert reorg["applied"] is True
        replaced = rpc(endpoint, "eth_getBlockByNumber", ["0x12", False])
        assert replaced["hash"] == "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0012"
        print("mock RPC smoke validation passed")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


if __name__ == "__main__":
    main()
