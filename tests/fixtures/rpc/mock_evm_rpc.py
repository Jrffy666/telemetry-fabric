#!/usr/bin/env python3
"""Small JSON-RPC mock for crawler integration tests.

The server intentionally uses only the Python standard library so it can run in
minimal CI images. It is a test skeleton, not a production RPC implementation.
"""

from __future__ import annotations

import argparse
import copy
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


def hex_to_int(value: str | int) -> int:
    if isinstance(value, int):
        return value
    return int(value, 16) if value.startswith("0x") else int(value)


def int_to_hex(value: int) -> str:
    return hex(value)


class ScenarioState:
    def __init__(self, scenario: dict[str, Any]):
        self.scenario = copy.deepcopy(scenario)
        self.chain_id = self.scenario.get("chain_id", "0x1")
        self.network = self.scenario.get("network", "fixture")
        self.head = hex_to_int(self.scenario.get("head", "0x0"))
        self.finalized = hex_to_int(self.scenario.get("finalized", "0x0"))
        self.blocks = {
            hex_to_int(block["number"]): copy.deepcopy(block)
            for block in self.scenario.get("blocks", [])
        }
        self.gaps = {hex_to_int(height) for height in self.scenario.get("gaps", [])}
        self.reorgs = list(self.scenario.get("reorgs", []))
        self.applied_reorgs: list[str] = []

    def block_by_number(self, number: int) -> dict[str, Any] | None:
        if number in self.gaps:
            return None
        block = self.blocks.get(number)
        if block is None:
            return None
        return copy.deepcopy(block)

    def logs(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        from_block = hex_to_int(query.get("fromBlock", "0x0"))
        to_value = query.get("toBlock", int_to_hex(self.head))
        to_block = self.head if to_value == "latest" else hex_to_int(to_value)
        address = query.get("address")
        topics = query.get("topics")

        results: list[dict[str, Any]] = []
        for number in range(from_block, to_block + 1):
            block = self.block_by_number(number)
            if block is None:
                continue
            for log in block.get("logs", []):
                if address and log.get("address", "").lower() != str(address).lower():
                    continue
                if topics and not topics_match(log.get("topics", []), topics):
                    continue
                results.append(copy.deepcopy(log))
        return results

    def apply_next_reorg(self) -> dict[str, Any]:
        if not self.reorgs:
            return {"applied": False, "reason": "no reorg configured"}

        reorg = self.reorgs.pop(0)
        for block in reorg.get("blocks", []):
            self.blocks[hex_to_int(block["number"])] = copy.deepcopy(block)
        if "new_head" in reorg:
            self.head = hex_to_int(reorg["new_head"])
        self.applied_reorgs.append(reorg.get("name", "unnamed-reorg"))
        return {
            "applied": True,
            "name": reorg.get("name", "unnamed-reorg"),
            "replace_from": reorg.get("replace_from"),
            "head": int_to_hex(self.head),
        }

    def health(self) -> dict[str, Any]:
        return {
            "name": self.scenario.get("name", "unnamed"),
            "chain_id": self.chain_id,
            "network": self.network,
            "head": int_to_hex(self.head),
            "finalized": int_to_hex(self.finalized),
            "block_count": len(self.blocks),
            "gaps": [int_to_hex(height) for height in sorted(self.gaps)],
            "pending_reorgs": len(self.reorgs),
            "applied_reorgs": self.applied_reorgs,
        }


def topics_match(actual: list[str], expected: list[Any]) -> bool:
    for index, want in enumerate(expected):
        if want is None:
            continue
        if index >= len(actual):
            return False
        if isinstance(want, list):
            if actual[index] not in want:
                return False
            continue
        if actual[index] != want:
            return False
    return True


class RPCHandler(BaseHTTPRequestHandler):
    state: ScenarioState

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        try:
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length))
            response = self.handle_rpc(payload)
            self.write_json(response)
        except Exception as exc:  # pragma: no cover - defensive server boundary
            self.write_json({"jsonrpc": "2.0", "id": None, "error": rpc_error(-32000, str(exc))})

    def handle_rpc(self, payload: Any) -> Any:
        if isinstance(payload, list):
            return [self.handle_call(call) for call in payload]
        return self.handle_call(payload)

    def handle_call(self, call: dict[str, Any]) -> dict[str, Any]:
        method = call.get("method")
        params = call.get("params") or []
        request_id = call.get("id")

        try:
            result = self.dispatch(method, params)
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except ValueError as exc:
            return {"jsonrpc": "2.0", "id": request_id, "error": rpc_error(-32602, str(exc))}
        except NotImplementedError as exc:
            return {"jsonrpc": "2.0", "id": request_id, "error": rpc_error(-32601, str(exc))}

    def dispatch(self, method: str, params: list[Any]) -> Any:
        if method == "eth_chainId":
            return self.state.chain_id
        if method == "eth_blockNumber":
            return int_to_hex(self.state.head)
        if method == "eth_getBlockByNumber":
            if not params:
                raise ValueError("eth_getBlockByNumber requires a block number")
            number = self.state.head if params[0] == "latest" else hex_to_int(params[0])
            return self.state.block_by_number(number)
        if method == "eth_getLogs":
            query = params[0] if params else {}
            return self.state.logs(query)
        if method == "telemetry_health":
            return self.state.health()
        if method == "telemetry_setHead":
            if not params:
                raise ValueError("telemetry_setHead requires a height")
            self.state.head = hex_to_int(params[0])
            return self.state.health()
        if method == "telemetry_setGap":
            if not params:
                raise ValueError("telemetry_setGap requires a height")
            self.state.gaps.add(hex_to_int(params[0]))
            return self.state.health()
        if method == "telemetry_clearGaps":
            self.state.gaps.clear()
            return self.state.health()
        if method == "telemetry_simulateReorg":
            return self.state.apply_next_reorg()
        raise NotImplementedError(f"unsupported method: {method}")

    def write_json(self, payload: Any) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def rpc_error(code: int, message: str) -> dict[str, Any]:
    return {"code": code, "message": message}


def load_scenario(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a fixture-backed EVM JSON-RPC mock.")
    parser.add_argument("--scenario", required=True, type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=18545, type=int)
    args = parser.parse_args()

    RPCHandler.state = ScenarioState(load_scenario(args.scenario))
    server = ThreadingHTTPServer((args.host, args.port), RPCHandler)
    print(f"mock EVM RPC listening on http://{args.host}:{args.port}")
    print(json.dumps(RPCHandler.state.health(), sort_keys=True))
    server.serve_forever()


if __name__ == "__main__":
    main()
