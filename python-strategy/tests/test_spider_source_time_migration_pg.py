"""PostgreSQL proofs for the storage-only Spider SOURCE_TIME delivery kind."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import os
from typing import Any, cast

import pytest
from sqlalchemy import func, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeDelivery,
    SpiderRuntimeScope,
)
from test_migrations import _downgrade, _schema_fingerprint, _upgrade
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_member_resubmission_migration_pg import (
    _database_snapshot,
    _legacy_tape_snapshot,
)
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_source_market_migration_pg import _seed_financial_facts
from test_spider_transmission_owner_pg import (
    _OfflinePlaceClient,
    _run_with_initial_order_count,
)
from test_spider_source_market_pg import _market_payload

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]

_PREDECESSOR = "4c2f8a91d5e7"
_REVISION = "6d8a1e2f4c90"


def _source_time_values(
    scope: Any, sequence: int, disposition: str, generation: int, config_sha256: str
) -> dict[str, object]:
    payload = f"storage-only SOURCE_TIME observation {sequence}".encode()
    return {
        "environment": scope.environment,
        "execution_scope_id": scope.execution_scope_id,
        "strategy_id": scope.strategy_id,
        "sequence": sequence,
        "delivery_id": sha256(f"source-time-id:{sequence}".encode()).hexdigest(),
        "content_sha256": sha256(payload).hexdigest(),
        "kind": "SOURCE_TIME",
        "local_order_id": None,
        "trade_id": None,
        "action_id": None,
        "generation": generation,
        "configuration_sha256": config_sha256,
        "role": None,
        "source_payload": payload,
        "visible_at": datetime(2026, 10, 14, 12, 0, 0, 123000, tzinfo=timezone.utc),
        "elapsed_ms": None,
        "disposition": disposition,
        "suppression_reason": None,
        "decision_payload": None,
        "decision_sha256": None,
        "full_callback_key": None,
    }


def _check_definitions(engine: Engine) -> tuple[tuple[str, str | None, str], ...]:
    inspector = inspect(engine)
    definitions = [
        (table, row["name"], row["sqltext"])
        for table in inspector.get_table_names()
        for row in inspector.get_check_constraints(table)
    ]
    return tuple(sorted(definitions, key=lambda item: (item[0], item[1] or "", item[2])))


def _check_map(engine: Engine) -> dict[tuple[str, str | None], str]:
    return {(table, name): definition for table, name, definition in _check_definitions(engine)}


@pytest.mark.parametrize("disposition", ("PENDING", "CONSUMED", "SUPPRESSED"))
def test_source_time_storage_constraints_and_each_disposition_downgrade_guard(
    operation_pg: Engine,
    disposition: str,
) -> None:
    engine = operation_pg
    owner, scope, _operation_id, order_manager, *_ = _install_owner(
        engine, admitted_run=_run_with_initial_order_count(11)
    )
    # These SOURCE_TIME rows are storage-only seeds, not actual time ingress.
    assert "SOURCE_TIME" in str(
        SpiderRuntimeDelivery.__table__.c.kind.type
    ) or any(
        constraint.name == "ck_spider_runtime_delivery_kind"
        and "SOURCE_TIME" in str(constraint.sqltext)
        for constraint in SpiderRuntimeDelivery.__table__.constraints
    )
    assert any(
        constraint.name == "ck_spider_runtime_delivery_kind_references"
        and "kind = 'SOURCE_TIME'" in str(constraint.sqltext)
        and "action_id IS NULL" in str(constraint.sqltext)
        and "local_order_id IS NULL" in str(constraint.sqltext)
        for constraint in SpiderRuntimeDelivery.__table__.constraints
    )

    order, trade = _seed_financial_facts(
        owner, scope, sessionmaker(engine), order_manager
    )
    with engine.connect() as connection:
        runtime = connection.execute(
            select(
                SpiderRuntimeScope.generation,
                SpiderRuntimeScope.configuration_sha256,
            ).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).one()
        next_sequence = int(connection.execute(
            select(func.coalesce(func.max(SpiderRuntimeDelivery.sequence), 0)).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        ).scalar_one()) + 1
        generation = int(runtime.generation)
        config_sha256 = str(runtime.configuration_sha256)
    values = _source_time_values(
        scope, next_sequence, disposition, generation, config_sha256
    )
    if disposition == "SUPPRESSED":
        values["suppression_reason"] = "POLICY_FILTERED"
    with Session(engine) as session, session.begin():
        session.execute(SpiderRuntimeDelivery.__table__.insert(), values)

    # Use existing FK-valid identities so failures are from the kind-reference CHECK.
    with Session(engine) as session:
        action_id = session.execute(
            select(SpiderRuntimeAction.action_id).where(
                SpiderRuntimeAction.environment == scope.environment,
                SpiderRuntimeAction.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.local_order_id == order.id,
            ).limit(1)
        ).scalar_one_or_none()
    assert action_id is not None
    invalid_refs = []
    for field, value in (
        ("local_order_id", order.id),
        ("action_id", action_id),
        ("trade_id", trade.id),
        ("role", "GRID"),
    ):
        invalid = _source_time_values(
            scope, next_sequence + 1, "PENDING", generation, config_sha256
        )
        invalid[field] = value
        invalid_refs.append(invalid)
    unknown_kind = _source_time_values(
        scope, next_sequence + 1, "PENDING", generation, config_sha256
    )
    unknown_kind["kind"] = "UNKNOWN_KIND"
    for invalid in (*invalid_refs, unknown_kind):
        with Session(engine) as session, session.begin():
            with pytest.raises(IntegrityError) as error:
                session.execute(SpiderRuntimeDelivery.__table__.insert(), invalid)
            expected_constraint = (
                "ck_spider_runtime_delivery_kind"
                if invalid["kind"] == "UNKNOWN_KIND"
                else "ck_spider_runtime_delivery_kind_references"
            )
            diagnostic = getattr(error.value.orig, "diag", None)
            assert getattr(diagnostic, "constraint_name", None) == expected_constraint

    before = _database_snapshot(engine)
    before_checks = _check_definitions(engine)
    database_name = cast(str, engine.url.database)
    with pytest.raises(
        DBAPIError,
        match="Cannot downgrade while Spider SOURCE_TIME deliveries exist",
    ):
        _downgrade(database_name, _PREDECESSOR)
    assert _database_snapshot(engine) == before
    assert _check_definitions(engine) == before_checks


def test_predecessor_roundtrip_preserves_legacy_financial_source_and_transport_facts(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, _operation_id, order_manager, prepared, *_ = _install_owner(
        engine, admitted_run=_run_with_initial_order_count(11)
    )
    enabled = _transport_enabled_owner(owner, scope)
    source_market = _market_payload(scope, prepared, "9" * 64, price="123.5")
    received = enabled.receive_source_market(source_market)
    assert received.success is True and received.data is not None
    _seed_financial_facts(enabled, scope, sessionmaker(engine), order_manager)

    source_group, source_payload, members = _saved_initial_place_request(engine, scope)
    client = _OfflinePlaceClient({
        "code": "0",
        "data": [
            {
                "clOrdId": member["wire_clOrdId"],
                "ordId": f"source-time-migration-{index}",
                "sCode": "0",
            }
            for index, member in enumerate(members)
        ],
        "msg": "synthetic legacy transport response",
    })
    transmitted = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        source_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert transmitted.disposition == "RECORDED" and client.calls == 1

    before_schema = _schema_fingerprint(engine)
    before_checks = _check_map(engine)
    before_legacy = _legacy_tape_snapshot(engine)
    with engine.connect() as connection:
        delivery_kinds = set(connection.execute(
            select(SpiderRuntimeDelivery.kind).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        ).scalars())
    assert {"FILL", "ORDER_STATE", "SOURCE_MARKET", "TRANSPORT_RESULT"} <= delivery_kinds
    assert before_legacy.runtime_rows
    database_name = cast(str, engine.url.database)

    _downgrade(database_name, _PREDECESSOR)
    assert _legacy_tape_snapshot(engine) == before_legacy
    predecessor_checks = _check_map(engine)
    changed_checks = {
        key for key in before_checks.keys() | predecessor_checks.keys()
        if before_checks.get(key) != predecessor_checks.get(key)
    }
    assert changed_checks == {
        ("spider_runtime_delivery", "ck_spider_runtime_delivery_kind"),
        ("spider_runtime_delivery", "ck_spider_runtime_delivery_kind_references"),
    }
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _PREDECESSOR

    _upgrade(database_name, "head")
    assert _schema_fingerprint(engine) == before_schema
    assert _check_map(engine) == before_checks
    assert _legacy_tape_snapshot(engine) == before_legacy
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _REVISION
