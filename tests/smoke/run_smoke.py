#!/usr/bin/env python3
"""Lightweight smoke validation for the tests directory."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_DIRS = [
    ROOT / "integration",
    ROOT / "load",
    ROOT / "chaos",
    ROOT / "fixtures",
    ROOT / "fixtures" / "evm",
    ROOT / "fixtures" / "reorg",
    ROOT / "fixtures" / "rpc",
    ROOT / "smoke",
]


def load_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def validate_directories() -> None:
    for path in REQUIRED_DIRS:
        assert_true(path.is_dir(), f"missing directory: {path}")


def validate_json_fixtures() -> None:
    for path in sorted((ROOT / "fixtures").rglob("*.json")):
        load_json(path)


def validate_rpc_scenarios() -> None:
    for path in sorted((ROOT / "fixtures" / "rpc").glob("scenario_*.json")):
        scenario = load_json(path)
        assert_true(isinstance(scenario, dict), f"scenario is not an object: {path}")
        for key in ["name", "chain_id", "head", "blocks", "gaps", "reorgs"]:
            assert_true(key in scenario, f"scenario missing {key}: {path}")
        assert_true(isinstance(scenario["blocks"], list), f"blocks must be a list: {path}")


def validate_mock_server_import() -> None:
    module_path = ROOT / "fixtures" / "rpc" / "mock_evm_rpc.py"
    spec = importlib.util.spec_from_file_location("mock_evm_rpc", module_path)
    assert_true(spec is not None and spec.loader is not None, "cannot load mock_evm_rpc module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert_true(hasattr(module, "ScenarioState"), "mock_evm_rpc missing ScenarioState")


def validate_expected_dedupe_keys() -> None:
    fixture = load_json(ROOT / "fixtures" / "evm" / "expected_dedupe_keys.json")
    assert_true(isinstance(fixture, dict), "expected_dedupe_keys must be an object")
    for key in ["event_keys", "transaction_keys"]:
        values = fixture.get(key)
        assert_true(isinstance(values, list), f"{key} must be a list")
        assert_true(len(values) == len(set(values)), f"{key} contains duplicate expected keys")


def main() -> None:
    validate_directories()
    validate_json_fixtures()
    validate_rpc_scenarios()
    validate_mock_server_import()
    validate_expected_dedupe_keys()
    print("tests smoke validation passed")


if __name__ == "__main__":
    main()
