"""PostgreSQL downgrade guard and legacy-tape migration checks."""

from __future__ import annotations

import os
from typing import Any, NamedTuple, cast

import pytest
import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport
from src.core.spider_management.member_resubmission import MemberResubmissionRequest
from src.core.spider_management.orm import (
    SpiderMemberResubmission,
    SpiderRuntimeAction,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
)
from test_migrations import _downgrade, _schema_fingerprint, _upgrade
from test_spider_control_command_pg import _command, _dispatch
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_transmission_owner_pg import (
    _OfflinePlaceClient,
    _run_with_initial_order_count,
)

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]

_REVISION = "4c2f8a91d5e7"
_PREDECESSOR = "f47c2a91d683"
_ROW_TABLES = (
    "alembic_version",
    "spider_member_resubmission_request",
    "spider_member_resubmission",
    "spider_runtime_scope",
    "spider_runtime_action",
    "spider_runtime_delivery",
    "spider_runtime_transmission",
    "spider_runtime_tape_index",
    "order",
    "trade",
)


class _LegacyTapeSnapshot(NamedTuple):
    delivery_rows: tuple[tuple[object, ...], ...]
    tape_rows: tuple[tuple[object, ...], ...]
    transmission_rows: tuple[tuple[object, ...], ...]
    runtime_rows: tuple[tuple[object, ...], ...]


def _ordered_rows(engine: Engine, table_name: str) -> tuple[tuple[object, ...], ...]:
    table = sa.Table(table_name, sa.MetaData(), autoload_with=engine)
    ordering = tuple(table.primary_key.columns)
    query = select(table).order_by(*ordering) if ordering else select(table)
    with engine.connect() as connection:
        return tuple(tuple(row) for row in connection.execute(query).all())


def _database_snapshot(engine: Engine) -> tuple[object, ...]:
    return (
        _schema_fingerprint(engine),
        tuple((table, _ordered_rows(engine, table)) for table in _ROW_TABLES),
    )


def _legacy_tape_snapshot(engine: Engine) -> _LegacyTapeSnapshot:
    tape = sa.Table("spider_runtime_tape_index", sa.MetaData(), autoload_with=engine)
    legacy_tape_columns = tuple(
        tape.c[name]
        for name in (
            "environment", "execution_scope_id", "strategy_id", "ordinal", "kind",
            "delivery_sequence", "transmission_group_id",
        )
    )
    legacy_tape_query = select(*legacy_tape_columns).order_by(*tape.primary_key.columns)
    with engine.connect() as connection:
        tape_rows = tuple(tuple(row) for row in connection.execute(legacy_tape_query).all())
    delivery_rows = _ordered_rows(engine, "spider_runtime_delivery")
    transmission_rows = _ordered_rows(engine, "spider_runtime_transmission")
    with engine.connect() as connection:
        runtime_rows = tuple(tuple(row) for row in connection.execute(text(
            "SELECT environment, execution_scope_id, strategy_id, state_version, "
            "last_received_sequence, last_consumed_sequence, last_tape_sequence, "
            "source_checkpoint, source_checkpoint_sha256 "
            "FROM spider_runtime_scope ORDER BY environment, execution_scope_id, strategy_id"
        )).all())
    return _LegacyTapeSnapshot(delivery_rows, tape_rows, transmission_rows, runtime_rows)


def _commit_resubmission(engine: Engine) -> str:
    owner, scope, _operation_id, _manager, _prepared, _initial, *_ = _install_owner(
        engine, admitted_run=_run_with_initial_order_count(11)
    )
    enabled = _transport_enabled_owner(owner, scope)
    source_group, source_payload, members = _saved_initial_place_request(engine, scope)
    target_action_id = cast(str, members[0]["action_id"])
    response = {
        "code": "0",
        "data": [
            {
                "clOrdId": member["wire_clOrdId"],
                "ordId": "" if member["action_id"] == target_action_id else f"migration-{index}",
                "sCode": "51000" if member["action_id"] == target_action_id else "0",
            }
            for index, member in enumerate(members)
        ],
        "msg": "synthetic initial rejection for one proved member",
    }
    client = _OfflinePlaceClient(response)
    transmitted = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        source_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert transmitted.disposition == "RECORDED"
    assert client.calls == 1

    with Session(engine) as session:
        runtime = session.execute(select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.environment == scope.environment,
            SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
            SpiderRuntimeScope.strategy_id == scope.strategy_id,
        )).scalar_one()
        target_action = session.get(SpiderRuntimeAction, target_action_id)
        assert target_action is not None
        assert target_action.plan_id == runtime.initial_plan_id
        request = MemberResubmissionRequest.from_object({
            "schema_version": "spider_member_resubmission_request_v1",
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
            "actor": "operator",
            "idempotency_key": "migration-guard-resubmit",
            "expected_version": runtime.state_version,
            "generation": runtime.generation,
            "configuration_sha256": runtime.configuration_sha256,
            "initial_plan_id": runtime.initial_plan_id,
            "target_action_id": target_action_id,
        })
    assert enabled.record_member_resubmission_request(request.canonical_request_bytes) is not None

    command = _command(scope, key=request.idempotency_key, version=request.expected_version)
    command["command"] = "RESUBMIT"
    cast(dict[str, object], command["params"])["actor"] = "operator"
    result = _dispatch(enabled, command)
    assert result.success is True and result.data is not None
    assert result.data["disposition"] == "ACCEPTED"
    with Session(engine) as session:
        relation = session.get(SpiderMemberResubmission, request.request_id)
        assert relation is not None
        tape = session.execute(select(SpiderRuntimeTapeIndex).where(
            SpiderRuntimeTapeIndex.environment == scope.environment,
            SpiderRuntimeTapeIndex.execution_scope_id == scope.execution_scope_id,
            SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id,
            SpiderRuntimeTapeIndex.kind == "RESUBMIT",
        )).scalars().all()
        assert len(tape) == 1 and tape[0].member_resubmission_request_id == request.request_id
    return request.request_id


def test_committed_resubmission_blocks_downgrade_without_any_rollback_loss(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    request_id = _commit_resubmission(engine)
    before = _database_snapshot(engine)
    relation_rows = _ordered_rows(engine, "spider_member_resubmission")
    assert any(row[0] == request_id for row in relation_rows)
    assert _ordered_rows(engine, "spider_member_resubmission_request")
    assert _ordered_rows(engine, "spider_runtime_scope")
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _REVISION

    database_name = cast(str, engine.url.database)
    with pytest.raises(
        DBAPIError,
        match="Cannot downgrade while Spider resubmission tape entries exist",
    ):
        _downgrade(database_name, _PREDECESSOR)

    assert _database_snapshot(engine) == before


def test_clean_downgrade_upgrade_preserves_legacy_delivery_and_send_marker(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, _operation_id, _manager, _prepared, _initial, *_ = _install_owner(
        engine, admitted_run=_run_with_initial_order_count(11)
    )
    enabled = _transport_enabled_owner(owner, scope)
    source_group, source_payload, members = _saved_initial_place_request(engine, scope)
    client = _OfflinePlaceClient({
        "code": "0",
        "data": [
            {
                "clOrdId": member["wire_clOrdId"],
                "ordId": f"legacy-{index}",
                "sCode": "0",
            }
            for index, member in enumerate(members)
        ],
        "msg": "synthetic accepted legacy PLACE response",
    })
    transmitted = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        source_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert transmitted.disposition == "RECORDED"
    assert client.calls == 1
    before_schema = _schema_fingerprint(engine)
    before_legacy = _legacy_tape_snapshot(engine)
    tape_kinds = [row[4] for row in before_legacy.tape_rows]
    assert "DELIVERY" in tape_kinds
    assert "SEND_MARKER" in tape_kinds
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _REVISION

    database_name = cast(str, engine.url.database)
    _downgrade(database_name, _PREDECESSOR)
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _PREDECESSOR
    assert _legacy_tape_snapshot(engine) == before_legacy
    assert "spider_member_resubmission" not in sa.inspect(engine).get_table_names()
    assert "member_resubmission_request_id" not in {
        column["name"] for column in sa.inspect(engine).get_columns("spider_runtime_tape_index")
    }

    _upgrade(database_name, "head")
    assert _schema_fingerprint(engine) == before_schema
    assert _legacy_tape_snapshot(engine) == before_legacy
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == _REVISION
