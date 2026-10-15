"""Causal PostgreSQL checks for financial admission at Spider transmission."""

from decimal import Decimal
from hashlib import sha256
import os
import time
from typing import Any, cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from src.core.adapters.okx_spider_financial_profile import (
    create_okx_spider_financial_profile,
)
from src.core.adapters.okx_spider_profile import create_okx_spider_profile_prerequisite
from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.spider_management.orm import SpiderRuntimeTapeIndex, SpiderRuntimeTransmission
from src.core.spider_management.runtime_owner import SpiderRuntimeComposition
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_profile_prerequisite_pg import _CountingTransport, _profile, _snapshot
from test_spider_source_market_main_pg import _configure_main
from test_spider_transmission_owner_pg import _saved_normal_place_group
from test_spider_profile_prerequisite_pg import _record_demo_full_decision

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
fresh_pg_db = _migration_fresh_pg_db
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _financial_profile(request_payload: bytes, *, unavailable: str | None = None):
    prerequisite = _profile(request_payload)
    selection = cast(dict[str, object], decode_canonical(prerequisite.selection_bytes))
    evidence = cast(dict[str, object], decode_canonical(prerequisite.evidence_bytes))
    products = cast(list[dict[str, object]], selection["products"])
    instruments = cast(dict[str, object], evidence["instruments"])
    rows = cast(list[dict[str, object]], instruments["data"])
    for product, instrument in zip(products, rows, strict=True):
        base = cast(str, product["base_ccy"])
        instrument["instFamily"] = f"{base}-USDT"
        instrument["groupId"] = f"group-{base}"

    digest = sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": selection,
        "evidence": evidence,
    })).hexdigest()
    bound = create_okx_spider_profile_prerequisite(
        canonical_bytes(selection), canonical_bytes(evidence), digest
    )
    received = time.time_ns() // 1_000_000
    records: list[dict[str, object]] = [
        {
            "endpoint": "account/balance",
            "params": {},
            "received_at_ms": received,
            "response": {"code": "0", "data": [{
                "adjEq": "10000", "imr": "1", "mmr": "1", "ordFroz": "0",
                "uTime": str(received),
                "details": [{
                    "ccy": "USDT", "eq": "1000", "isoEq": "0",
                    "availEq": "900", "liab": "0", "uTime": str(received),
                }],
            }]},
        },
        {
            "endpoint": "account/positions", "params": {},
            "received_at_ms": received,
            "response": {"code": "0", "data": []},
        },
        {
            "endpoint": "trade/orders-pending", "params": {"limit": "100"},
            "received_at_ms": received,
            "response": {"code": "0", "data": []},
        },
    ]
    families: dict[str, list[str]] = {}
    for product in products:
        base = cast(str, product["base_ccy"])
        families.setdefault(f"{base}-USDT", []).append(f"group-{base}")
    for family, group_ids in families.items():
        records.append({
            "endpoint": "account/trade-fee",
            "params": {"instType": "SWAP", "instFamily": family},
            "received_at_ms": received,
            "response": {"code": "0", "data": [{
                "instType": "SWAP", "ts": str(received),
                "feeGroup": [
                    {"groupId": group_id, "maker": "-0.0002", "taker": "-0.0005"}
                    for group_id in group_ids
                ],
            }]},
        })
    return create_okx_spider_financial_profile(
        bound, Decimal("0"), tuple(records), unavailable
    )


def _marker_snapshot(engine: Engine, strategy_id: str) -> tuple[int, int, int]:
    with engine.connect() as connection:
        tape_count = connection.scalar(select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
            SpiderRuntimeTapeIndex.strategy_id == strategy_id,
            SpiderRuntimeTapeIndex.kind == "SEND_MARKER",
        ))
        transmission_count = connection.scalar(select(func.count()).select_from(
            SpiderRuntimeTransmission
        ).where(SpiderRuntimeTransmission.strategy_id == strategy_id))
        last_ordinal = connection.scalar(select(func.coalesce(func.max(
            SpiderRuntimeTapeIndex.ordinal
        ), 0)).where(SpiderRuntimeTapeIndex.strategy_id == strategy_id))
    return int(tape_count or 0), int(transmission_count or 0), int(last_ordinal or 0)


@pytest.mark.parametrize("financial_unavailable", ["CAPTURE_FAILED", None])
def test_normal_place_requires_financial_profile_before_marker_and_transport(
    operation_pg: Engine, mock_clock, mock_exchange_adapter, financial_unavailable: str | None
) -> None:
    owner, scope, _operation_id, delivery_id = _record_demo_full_decision(
        operation_pg, mock_clock, mock_exchange_adapter
    )
    _group_id, request_payload, _members = _saved_normal_place_group(
        operation_pg, scope, delivery_id
    )
    financial = _financial_profile(request_payload, unavailable=financial_unavailable)
    enabled = _transport_enabled_owner(
        owner, scope, profile_prerequisite=financial
    )
    transport = _CountingTransport()
    before = _snapshot(operation_pg, scope)
    before_markers = _marker_snapshot(operation_pg, scope.strategy_id)
    result = enabled._transmit_normal_place_group(
        scope, request_payload, OkxSpiderBatchTransport(cast(Any, transport))
    )
    if financial_unavailable is not None:
        assert result.disposition == "REJECTED"
        assert transport.place_calls == transport.cancel_calls == 0
        assert _snapshot(operation_pg, scope) == before
        assert _marker_snapshot(operation_pg, scope.strategy_id) == before_markers
    else:
        assert result.disposition == "RECORDED"
        assert transport.place_calls == 1 and transport.cancel_calls == 0
        assert _marker_snapshot(operation_pg, scope.strategy_id)[0] == before_markers[0] + 1


@pytest.mark.parametrize("financial_unavailable", ["CAPTURE_FAILED", None])
def test_initial_place_requires_financial_profile_before_marker_and_transport(
    operation_pg: Engine, financial_unavailable: str | None
) -> None:
    owner, scope, _operation_id, _manager, _prepared, _initial, *_ = _install_owner(
        operation_pg, environment="demo", execution_scope_id="demo-account-ref"
    )
    source_group, request_payload, _members = _saved_initial_place_request(
        operation_pg, scope
    )
    financial = _financial_profile(request_payload, unavailable=financial_unavailable)
    enabled = _transport_enabled_owner(
        owner, scope, profile_prerequisite=financial
    )
    transport = _CountingTransport()
    before = _snapshot(operation_pg, scope)
    before_markers = _marker_snapshot(operation_pg, scope.strategy_id)
    result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, transport)),
    )
    if financial_unavailable is not None:
        assert result.disposition == "REJECTED"
        assert transport.place_calls == transport.cancel_calls == 0
        assert _snapshot(operation_pg, scope) == before
        assert _marker_snapshot(operation_pg, scope.strategy_id) == before_markers
    else:
        assert result.disposition == "RECORDED"
        assert transport.place_calls == 1 and transport.cancel_calls == 0
        assert _marker_snapshot(operation_pg, scope.strategy_id)[0] == before_markers[0] + 1


def test_main_forwards_optional_profile_prerequisite_and_keeps_default_none(
    operation_pg: Engine, monkeypatch: Any
) -> None:
    _owner, scope, _operation_id, _manager, _prepared, _initial, confirmation, source = (
        _install_owner(operation_pg, environment="demo", execution_scope_id="demo-account-ref")
    )
    _source_group, request_payload, _members = _saved_initial_place_request(
        operation_pg, scope
    )
    checker = _financial_profile(request_payload, unavailable="CAPTURE_FAILED")
    events: list[str] = []
    captured: dict[str, Any] = {}
    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}, profile_prerequisite=checker
    )
    with monkeypatch.context() as context:
        _configure_main(context, operation_pg, composition, events, captured)
    forwarded_checker = captured["owner"]._profile_prerequisite
    assert forwarded_checker is checker
    assert forwarded_checker(request_payload, time.time_ns() // 1_000_000) is False

    default_events: list[str] = []
    default_captured: dict[str, Any] = {}
    default_composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: source}
    )
    with monkeypatch.context() as context:
        _configure_main(
            context, operation_pg, default_composition, default_events, default_captured
        )
    assert default_captured["owner"]._profile_prerequisite is None

    with pytest.raises(ValueError, match="profile prerequisite"):
        SpiderRuntimeComposition(
            (scope,), {confirmation.request_id: source},
            profile_prerequisite=cast(Any, object()),
        )
