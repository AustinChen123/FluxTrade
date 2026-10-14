"""Migration preservation and PostgreSQL receipt-constraint proofs."""

from __future__ import annotations

from decimal import Decimal
from hashlib import sha256
import os
from typing import Any, cast

import pytest
from sqlalchemy import MetaData, Table, inspect, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.clock import BacktestClock
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent, IExchangeAdapter
from src.core.orm_models import Order, Trade
from src.core.spider_management.orm import (
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
)
from test_migrations import (
    _downgrade,
    _target_url,
    _upgrade,
    fresh_pg_db as _migration_fresh_pg_db,
)
from test_spider_control_command_pg import _dispatch
from test_spider_source_market_pg import _market_payload, _owner

pytest_plugins = ["test_migrations"]
fresh_pg_db = _migration_fresh_pg_db
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


class _AdapterProbe:
    def supports_runtime_reconciliation(self) -> bool:
        return False

    def close(self) -> None:
        pass


def _new_engine(db_name: str) -> Engine:
    from sqlalchemy import create_engine

    _upgrade(db_name, "head")
    engine = create_engine(_target_url(db_name))
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO strategy(id,name,configuration_json) VALUES ('spider-main','Spider','{}')")
        )
        connection.execute(
            text(
                "INSERT INTO strategy_state(strategy_id,status,config_json,version) "
                "VALUES ('spider-main','ACTIVE','{}',7)"
            )
        )
    return engine


def _seed_financial_facts(owner: Any, scope: Any, factory: Any, order_manager: Any):
    execution = ExecutionEngine(
        db_session=None,
        clock=BacktestClock(1_800_000_000),
        adapter=cast(IExchangeAdapter, _AdapterProbe()),
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    with factory() as session:
        order_id = session.scalar(
            select(Order.id).where(Order.strategy_id == scope.strategy_id)
            .order_by(Order.timestamp, Order.id).limit(1)
        )
    assert order_id is not None
    order = order_manager.repo.get_order(order_id)
    assert order is not None
    price = cast(Decimal, order.price)
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id="migration-probe-order",
    ))["action"] == "applied"
    quantity = order.quantity / Decimal(2)
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="partially_filled",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        cumulative_filled_quantity=quantity,
        cumulative_average_price=price,
        last_fill_quantity=quantity,
        last_fill_price=price,
        fee=Decimal("0.01"),
        fee_asset="USDT",
        last_fill_exchange_trade_id="migration-probe-trade",
    ))["action"] == "applied"
    with factory() as session:
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "migration-probe-trade")
        ).scalar_one()
        deliveries = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
    assert trade.fee == Decimal("0.01")
    assert {row.kind for row in deliveries} >= {"FILL", "ORDER_STATE"}
    return order, trade


def _scoped_snapshot(engine: Engine, scope: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    inspector = inspect(engine)
    table_names = sorted(inspector.get_table_names())
    schemas: dict[str, Any] = {}
    for name in table_names:
        columns = inspector.get_columns(name)
        schemas[name] = {
            "columns": tuple(
                (row["name"], str(row["type"]), bool(row["nullable"]))
                for row in columns
            ),
            "checks": tuple(sorted(
                (row["name"], row["sqltext"])
                for row in inspector.get_check_constraints(name)
            )),
            "primary_key": inspector.get_pk_constraint(name),
            "unique": tuple(sorted(
                (row["name"], tuple(row["column_names"] or ()))
                for row in inspector.get_unique_constraints(name)
            )),
            "foreign_keys": tuple(sorted(
                (
                    row["name"], tuple(row["constrained_columns"] or ()),
                    row["referred_table"], tuple(row["referred_columns"] or ()),
                )
                for row in inspector.get_foreign_keys(name)
            )),
        }

    meta = MetaData()
    reflected = {
        name: Table(name, meta, autoload_with=engine)
        for name in table_names
        if name.startswith("spider_") or name in {"order", "trade", "strategy_state", "strategy_state_transition", "alembic_version"}
    }
    order_table = reflected.get("order")
    order_ids: list[str] = []
    if order_table is not None and "strategy_id" in order_table.c:
        with engine.connect() as connection:
            order_ids = list(connection.execute(
                select(order_table.c.id).where(order_table.c.strategy_id == scope.strategy_id)
            ).scalars())
    rows: dict[str, Any] = {}
    with engine.connect() as connection:
        for name, table in reflected.items():
            predicates = []
            for column_name, value in (
                ("environment", scope.environment),
                ("execution_scope_id", scope.execution_scope_id),
                ("strategy_id", scope.strategy_id),
            ):
                if column_name in table.c:
                    predicates.append(table.c[column_name] == value)
            if name == "trade" and "order_id" in table.c:
                predicates.append(table.c.order_id.in_(order_ids))
            if name == "alembic_version":
                predicates = []
            if not predicates and name != "alembic_version":
                rows[name] = ()
                continue
            statement = select(table)
            for predicate in predicates:
                statement = statement.where(predicate)
            values = [dict(row._mapping) for row in connection.execute(statement)]
            rows[name] = tuple(sorted(values, key=repr))
    return schemas, rows


def _assert_legacy_rows_preserved(
    before: tuple[dict[str, Any], dict[str, Any]],
    after: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    old_schemas, old_rows = before
    new_schemas, new_rows = after
    for table, rows in old_rows.items():
        if table == "alembic_version":
            continue
        assert table in new_schemas
        old_columns = tuple(column[0] for column in old_schemas[table]["columns"])
        projected = tuple(sorted(
            ({key: row[key] for key in old_columns} for row in new_rows[table]), key=repr
        ))
        assert projected == rows


def _assert_expected_schema_transition(
    before: tuple[dict[str, Any], dict[str, Any]],
    after: tuple[dict[str, Any], dict[str, Any]],
) -> None:
    old_schemas, new_schemas = before[0], after[0]
    assert set(new_schemas) == set(old_schemas)
    for table, old in old_schemas.items():
        new = new_schemas[table]
        if table not in {"spider_runtime_plan", "spider_runtime_delivery"}:
            assert new == old
            continue
        old_columns = old["columns"]
        new_columns = new["columns"]
        if table == "spider_runtime_plan":
            old_names = tuple(column[0] for column in old_columns)
            new_names = tuple(column[0] for column in new_columns)
            assert new_names == old_names + (
                "original_build_checkpoint", "original_build_checkpoint_sha256"
            )
            assert new_columns[:len(old_columns)] == old_columns
            old_checks = dict(old["checks"])
            new_checks = dict(new["checks"])
            assert set(new_checks) == set(old_checks) | {
                "ck_spider_runtime_plan_build_checkpoint",
                "ck_spider_runtime_plan_build_checkpoint_hash",
            }
            assert "octet_length(original_build_checkpoint)" in new_checks[
                "ck_spider_runtime_plan_build_checkpoint"
            ]
            assert "original_build_checkpoint_sha256" in new_checks[
                "ck_spider_runtime_plan_build_checkpoint_hash"
            ]
            assert new_columns[-2][2] is False and new_columns[-1][2] is False
            assert new["primary_key"] == old["primary_key"]
            assert new["unique"] == old["unique"]
            assert new["foreign_keys"] == old["foreign_keys"]
        else:
            assert tuple(column[0] for column in new_columns) == tuple(
                column[0] for column in old_columns
            )
            nullable_source_references = {"local_order_id", "action_id", "role"}
            changed_nullability: set[str] = set()
            for old_column, new_column in zip(old_columns, new_columns, strict=True):
                if old_column[0] in nullable_source_references:
                    assert old_column[1] == new_column[1]
                    assert old_column[2] is False and new_column[2] is True
                    changed_nullability.add(old_column[0])
                else:
                    assert new_column == old_column
            assert changed_nullability == nullable_source_references
            old_checks = dict(old["checks"])
            new_checks = dict(new["checks"])
            assert set(new_checks) == (
                set(old_checks) - {"ck_spider_runtime_delivery_role"}
            ) | {
                "ck_spider_runtime_delivery_kind",
                "ck_spider_runtime_delivery_kind_references",
            }
            assert "ROLE IS NOT NULL" in new_checks[
                "ck_spider_runtime_delivery_kind_references"
            ].upper()
            assert "GRID" in new_checks[
                "ck_spider_runtime_delivery_kind_references"
            ]
            assert new["primary_key"] == old["primary_key"]
            assert new["unique"] == old["unique"]
            assert new["foreign_keys"] == old["foreign_keys"]


def _assert_check_violation(factory: Any, values: dict[str, Any], sequence: int) -> None:
    values = {**values, "sequence": sequence}
    table = SpiderRuntimeDelivery.__table__
    with factory() as session:
        assert isinstance(session, Session)
        with session.begin():
            with pytest.raises(IntegrityError) as error:
                with session.begin_nested():
                    session.execute(table.insert().values(**values))
            database_error = cast(Any, error.value.orig)
            assert database_error.diag.constraint_name == (
                "ck_spider_runtime_delivery_kind_references"
            )


def _assert_savepoint_insert_rolls_back(factory: Any, values: dict[str, Any], sequence: int) -> None:
    values = {**values, "sequence": sequence}
    table = SpiderRuntimeDelivery.__table__
    identity = tuple(values[key] for key in (
        "environment", "execution_scope_id", "strategy_id", "sequence"
    ))
    with factory() as session:
        with session.begin():
            nested = session.begin_nested()
            session.execute(table.insert().values(**values))
            assert session.get(SpiderRuntimeDelivery, identity) is not None
            nested.rollback()
    with factory() as session:
        assert session.get(SpiderRuntimeDelivery, identity) is None


def test_postgres_delivery_kind_reference_constraint_matrix(fresh_pg_db: str) -> None:
    engine = _new_engine(fresh_pg_db)
    try:
        owner, scope, _operation_id, factory, prepared, _initial, order_manager, _confirmation, _source = _owner(engine)
        _seed_financial_facts(owner, scope, factory, order_manager)
        raw_market = _market_payload(scope, prepared, "9" * 64, price="123.5")
        result = owner.receive_source_market(raw_market)
        assert result.success is True and result.data is not None
        with factory.begin() as session:
            templates = {
                row.kind: {column.name: getattr(row, column.name) for column in SpiderRuntimeDelivery.__table__.columns}
                for row in session.execute(
                    select(SpiderRuntimeDelivery).where(
                        SpiderRuntimeDelivery.strategy_id == scope.strategy_id
                    ).order_by(SpiderRuntimeDelivery.sequence)
                ).scalars().all()
                if row.kind in {"FILL", "ORDER_STATE", "SOURCE_MARKET"}
            }
        assert set(templates) == {"FILL", "ORDER_STATE", "SOURCE_MARKET"}
        sequence = 100
        for kind in ("FILL", "ORDER_STATE"):
            for role in (None, "NOT_GRID"):
                values = {
                    **templates[kind],
                    "delivery_id": sha256(f"invalid:{kind}:{role}".encode()).hexdigest(),
                    "role": role,
                }
                _assert_check_violation(factory, values, sequence)
                sequence += 1
            values = {
                **templates[kind],
                "delivery_id": sha256(f"valid:{kind}".encode()).hexdigest(),
                "role": "GRID",
            }
            _assert_savepoint_insert_rolls_back(factory, values, sequence)
            sequence += 1
        source = templates["SOURCE_MARKET"]
        assert source["local_order_id"] is None
        assert source["action_id"] is None and source["trade_id"] is None
        assert source["role"] is None
        source["delivery_id"] = sha256(b"valid:source-market-null-financial-refs").hexdigest()
        _assert_savepoint_insert_rolls_back(factory, source, sequence)
    finally:
        engine.dispose()


def test_populated_c3_predecessor_upgrade_preserves_corrupt_checkpoint_and_facts(
    fresh_pg_db: str,
) -> None:
    engine = _new_engine(fresh_pg_db)
    try:
        owner, scope, operation_id, factory, prepared, initial, order_manager, _confirmation, _source = _owner(engine)
        order, trade = _seed_financial_facts(owner, scope, factory, order_manager)
        stopped = _dispatch(owner, {
            "command": "STOP",
            "strategy_id": scope.strategy_id,
            "params": {
                "strategy_id": scope.strategy_id,
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "actor": "api_key",
                "idempotency_key": "migration-predecessor-stop",
                "expected_version": initial["state_version"],
            },
        })
        assert stopped.success is True
        before_manifest_readback = owner.readback(scope, operation_id)
        assert before_manifest_readback is not None
        with factory.begin() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                )
            ).scalar_one()
            checkpoint = cast(dict[str, Any], decode_canonical(bytes(runtime.source_checkpoint)))
            original_checkpoint_hash = prepared.build_checkpoint_sha256
            checkpoint["order_id"] = checkpoint["order_id"] + 1
            corrupted = canonical_bytes(checkpoint)
            runtime.source_checkpoint = corrupted
            runtime.source_checkpoint_sha256 = sha256(corrupted).hexdigest()
            runtime_values = (bytes(runtime.source_checkpoint), runtime.source_checkpoint_sha256)
            plan = session.execute(
                select(SpiderRuntimePlan).where(
                    SpiderRuntimePlan.plan_id == runtime.initial_plan_id
                )
            ).scalar_one()
            manifest_bytes = bytes(plan.manifest_payload)
            manifest_hash = plan.manifest_sha256
            manifest = cast(dict[str, Any], decode_canonical(manifest_bytes))
            assert manifest["build_checkpoint_sha256"] == original_checkpoint_hash
        _downgrade(fresh_pg_db, "c3f8d1a904b7")
        with engine.connect() as connection:
            assert connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one() == "c3f8d1a904b7"
        predecessor = _scoped_snapshot(engine, scope)
        assert "original_build_checkpoint" not in {
            column[0] for column in predecessor[0]["spider_runtime_plan"]["columns"]
        }
        with engine.begin() as connection:
            stored = connection.exec_driver_sql(
                "SELECT source_checkpoint, source_checkpoint_sha256 FROM spider_runtime_scope WHERE strategy_id = %s",
                (scope.strategy_id,),
            ).one()
        assert (bytes(stored[0]), stored[1]) == runtime_values
        _upgrade(fresh_pg_db, "d8a61c3f9b42")
        upgraded = _scoped_snapshot(engine, scope)
        _assert_expected_schema_transition(predecessor, upgraded)
        _assert_legacy_rows_preserved(predecessor, upgraded)
        _upgrade(fresh_pg_db, "head")
        with factory() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                )
            ).scalar_one()
            plan = session.execute(
                select(SpiderRuntimePlan).where(
                    SpiderRuntimePlan.plan_id == runtime.initial_plan_id
                )
            ).scalar_one()
            assert (bytes(runtime.source_checkpoint), runtime.source_checkpoint_sha256) == runtime_values
            assert bytes(plan.original_build_checkpoint) == runtime_values[0]
            assert plan.original_build_checkpoint_sha256 == runtime_values[1]
            assert plan.original_build_checkpoint_sha256 != original_checkpoint_hash
            assert bytes(plan.manifest_payload) == manifest_bytes
            assert plan.manifest_sha256 == manifest_hash
            assert session.get(Trade, trade.id) is not None
            assert session.get(Order, order.id) is not None
            assert runtime.control_mode == "STOP"
        assert owner.readback(scope, operation_id) is None
        assert before_manifest_readback["runtime_control"] == "STOP"
    finally:
        engine.dispose()


def test_source_market_receipt_blocks_downgrade_without_any_partial_change(
    fresh_pg_db: str,
) -> None:
    engine = _new_engine(fresh_pg_db)
    try:
        owner, scope, operation_id, _factory, prepared, _initial, _order_manager, _confirmation, _source = _owner(engine)
        raw = _market_payload(scope, prepared, "e" * 64, price="124.5")
        received = owner.receive_source_market(raw)
        assert received.success is True and received.data is not None
        assert received.data["delivery_disposition"] == "CONSUMED"
        before_readback = owner.readback(scope, operation_id)
        assert before_readback is not None
        before = _scoped_snapshot(engine, scope)
        with engine.connect() as connection:
            version_before = connection.exec_driver_sql(
                "SELECT version_num FROM alembic_version"
            ).scalar_one()
        with pytest.raises(Exception) as error:
            _downgrade(fresh_pg_db, "c3f8d1a904b7")
        assert "Cannot downgrade while Spider source-market receipts exist" in str(error.value)
        after = _scoped_snapshot(engine, scope)
        assert after == before
        with engine.connect() as connection:
            version_after = connection.exec_driver_sql(
                "SELECT version_num FROM alembic_version"
            ).scalar_one()
        assert version_after == version_before
        assert owner.readback(scope, operation_id) == before_readback
        with engine.connect() as connection:
            row = connection.execute(
                select(SpiderRuntimeDelivery.source_payload).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                    SpiderRuntimeDelivery.kind == "SOURCE_MARKET",
                )
            ).scalar_one()
        assert bytes(row) == raw
    finally:
        engine.dispose()
