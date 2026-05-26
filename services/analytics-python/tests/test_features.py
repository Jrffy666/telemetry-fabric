from __future__ import annotations

from decimal import Decimal

from analytics.features._records import canonical_transfer_records, transfer_identity_key
from analytics.features.address_profile import profile_address
from analytics.features.risk_features import extract_risk_features
from analytics.features.token_flow import detect_large_transfers, summarize_token_flow


TRANSFERS = [
    {
        "tx_hash": "0x1",
        "from_address": "0xaaa",
        "to_address": "0xbbb",
        "token_address": "0xtoken",
        "amount_usd": "100.50",
    },
    {
        "tx_hash": "0x2",
        "from_address": "0xbbb",
        "to_address": "0xccc",
        "token_address": "0xtoken",
        "amount_usd": "250000",
    },
    {
        "tx_hash": "0x3",
        "from_address": "0xddd",
        "to_address": "0xbbb",
        "token_symbol": "ETH",
        "amount_usd": "25",
    },
]


def test_profile_address_counts_flow_and_blacklist():
    profile = profile_address(TRANSFERS, "0xbbb", blacklist={"0xccc"})

    assert profile.tx_count == 3
    assert profile.sent_count == 1
    assert profile.received_count == 2
    assert profile.unique_counterparties == 3
    assert profile.blacklist_counterparty_count == 1
    assert profile.net_usd == Decimal("-249874.50")


def test_summarize_token_flow():
    flow = summarize_token_flow(TRANSFERS, "0xbbb")

    assert flow.transfer_count == 3
    assert flow.inbound_usd == Decimal("125.50")
    assert flow.outbound_usd == Decimal("250000")
    assert flow.net_usd == Decimal("-249874.50")


def test_detect_large_transfers_sorted():
    matches = detect_large_transfers(TRANSFERS, min_amount_usd="1000")

    assert len(matches) == 1
    assert matches[0]["tx_hash"] == "0x2"
    assert matches[0]["amount_usd_decimal"] == Decimal("250000")


def test_canonical_transfer_records_deduplicates_evm_identity():
    duplicate = {
        "chain_id": 1,
        "block_hash": "0xabc",
        "tx_hash": "0xaaa",
        "log_index": "0x0",
        "from_address": "0xaaa",
        "to_address": "0xbbb",
        "amount_usd": "10",
    }

    rows = canonical_transfer_records([duplicate, dict(duplicate)])

    assert len(rows) == 1
    assert transfer_identity_key(duplicate) == "1|0xabc|0xaaa|0"


def test_feature_builders_ignore_duplicate_and_reorged_transfers():
    rows = [
        {
            "chain_id": 1,
            "block_hash": "0xaaa",
            "tx_hash": "0x1",
            "log_index": 0,
            "from_address": "0xaaa",
            "to_address": "0xbbb",
            "amount_usd": "10",
        },
        {
            "chain_id": 1,
            "block_hash": "0xaaa",
            "tx_hash": "0x1",
            "log_index": 0,
            "from_address": "0xaaa",
            "to_address": "0xbbb",
            "amount_usd": "10",
        },
        {
            "chain_id": 1,
            "block_hash": "0xold",
            "tx_hash": "0x2",
            "log_index": 0,
            "from_address": "0xccc",
            "to_address": "0xbbb",
            "amount_usd": "1000",
            "finality_status": "FINALITY_STATUS_REORGED",
        },
        {
            "chain_id": 1,
            "block_hash": "0xnew",
            "tx_hash": "0x3",
            "log_index": 0,
            "from_address": "0xddd",
            "to_address": "0xbbb",
            "amount_usd": "25",
        },
    ]

    profile = profile_address(rows, "0xbbb")
    flow = summarize_token_flow(rows, "0xbbb")
    large = detect_large_transfers(rows, min_amount_usd="100")

    assert profile.received_count == 2
    assert profile.total_received_usd == Decimal("35")
    assert flow.transfer_count == 2
    assert flow.inbound_usd == Decimal("35")
    assert large == []


def test_extract_risk_features():
    profile = profile_address(TRANSFERS, "0xbbb", blacklist={"0xccc"})
    flow = summarize_token_flow(TRANSFERS, "0xbbb")

    features = extract_risk_features(profile, flow, blacklist_hops=1, anomaly_score=0.5)

    assert features["blacklist_hops"] == 1.0
    assert features["anomaly_score"] == 0.5
    assert features["largest_transfer_usd"] == 250000.0
