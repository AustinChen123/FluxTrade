"""Populated predecessor-to-transport migration preservation on PostgreSQL."""

from __future__ import annotations

import os
from hashlib import sha256
from decimal import Decimal
from threading import RLock
from typing import Any, cast

import pytest
import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

pytestmark = pytest.mark.integration
pytest_plugins = ["test_migrations"]

if os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1":
    pytest.skip(
        "set FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS=1 to run destructive "
        "PostgreSQL migration tests",
        allow_module_level=True,
    )

from test_migrations import (  # noqa: E402
    _downgrade,
    _target_url,
    _upgrade,
    fresh_pg_db as _migration_fresh_pg_db,
)

fresh_pg_db = _migration_fresh_pg_db

from test_spider_financial_delivery_pg import _install_owner  # noqa: E402
from src.core.backtest.spider_run_artifacts import decode_canonical  # noqa: E402
from src.core.clock import BacktestClock  # noqa: E402
from src.core.execution import ExecutionEngine  # noqa: E402
from src.core.interfaces.exchange import ExchangeOrderEvent  # noqa: E402
from src.core.orm_models import Order  # noqa: E402
from src.core.spider_management.orm import SpiderRuntimePlan, SpiderRuntimeScope  # noqa: E402
from src.core.spider_management.runtime_owner import SpiderRuntimeOwner  # noqa: E402
from src.core.strategy_state_manager import StrategyStateManager  # noqa: E402
from test_spider_control_command_pg import _dispatch  # noqa: E402


class _RedisProbe:
    def publish(self, _channel: str, _message: str) -> int:
        return 1


def _seed_install_receipt_stop(
    engine: Engine, strategy_id: str, adapter: Any
) -> tuple[SpiderRuntimeOwner, Any, str, dict[str, Any], dict[str, Any]]:
    (
        owner,
        scope,
        _operation_id,
        order_manager,
        _prepared,
        install_readback,
        _confirmation,
        source,
    ) = _install_owner(engine, strategy_id=strategy_id)
    factory = sessionmaker(engine)
    lifecycle_lock = RLock()
    owner = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            cast(dict[str, Any], install_readback["effective"])[
                "confirmation_request_id"
            ]: source
        },
        strategy_state_manager=StrategyStateManager(factory, _RedisProbe()),
        lifecycle_lock=lambda _strategy_id: lifecycle_lock,
        order_event_lock=RLock(),
    )
    with factory() as session:
        order = session.scalar(
            select(Order)
            .where(Order.strategy_id == strategy_id)
            .order_by(Order.timestamp, Order.id)
        )
    assert order is not None
    assert isinstance(order.quantity, Decimal)
    execution = ExecutionEngine(
        db_session=None,
        clock=BacktestClock(1_800_000_000),
        adapter=adapter,
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    price = cast(Decimal, order.price)
    assert (
        execution.process_exchange_order_event(
            ExchangeOrderEvent(
                status="open",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                exchange_order_id=f"migration-provider-{strategy_id}",
            )
        )["action"]
        == "applied"
    )
    assert (
        execution.process_exchange_order_event(
            ExchangeOrderEvent(
                status="partially_filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                cumulative_filled_quantity=order.quantity / Decimal(2),
                cumulative_average_price=price,
                last_fill_quantity=order.quantity / Decimal(2),
                last_fill_price=price,
                fee=Decimal("0.01"),
                fee_asset="USDT",
                last_fill_exchange_trade_id=f"migration-{strategy_id}",
            )
        )["action"]
        == "applied"
    )
    result = _dispatch(
        owner,
        {
            "command": "STOP",
            "strategy_id": scope.strategy_id,
            "params": {
                "strategy_id": scope.strategy_id,
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "actor": "api_key",
                "idempotency_key": f"migration-stop-{strategy_id}",
                "expected_version": install_readback["state_version"],
            },
        },
    )
    assert result.success is True and result.completed is True
    assert result.data is not None
    stopped = cast(dict[str, Any], result.data["readback"])
    assert stopped["runtime_control"] == "STOP"
    assert owner.readback(scope, _operation_id) == stopped
    return owner, scope, _operation_id, install_readback, stopped


def _legacy_snapshot(engine: Engine) -> dict[str, tuple[tuple[Any, ...], ...]]:
    """Read predecessor rows, excluding only additive transport columns."""
    tables = (
        "spider_runtime_scope",
        "spider_runtime_operation",
        "spider_runtime_plan",
        "spider_runtime_binding",
        "spider_runtime_action",
        "spider_runtime_delivery",
        "spider_control_receipt",
        "order",
        "trade",
        "strategy_state",
    )
    result: dict[str, tuple[tuple[Any, ...], ...]] = {}
    inspector = sa.inspect(engine)
    with engine.connect() as conn:
        for table in tables:
            columns = [
                column["name"]
                for column in inspector.get_columns(table)
                if not (
                    (table == "spider_runtime_scope" and column["name"] == "last_tape_sequence")
                    or (
                        table == "spider_runtime_plan"
                        and column["name"] in {
                            "original_build_checkpoint",
                            "original_build_checkpoint_sha256",
                        }
                    )
                )
            ]
            ordering = ", ".join(f'"{name}"' for name in columns[:3])
            selected = ", ".join(f'"{name}"' for name in columns)
            rows = conn.execute(
                sa.text(f'SELECT {selected} FROM "{table}" ORDER BY {ordering}')
            ).all()
            result[table] = tuple(tuple(row) for row in rows)
    return result


def _build_checkpoint_roots(engine: Engine) -> dict[str, tuple[bytes, str]]:
    factory = sessionmaker(engine)
    with factory() as session:
        rows = session.execute(
            select(
                SpiderRuntimeScope.strategy_id,
                SpiderRuntimeScope.source_checkpoint,
                SpiderRuntimeScope.source_checkpoint_sha256,
                SpiderRuntimePlan.manifest_payload,
            ).join(
                SpiderRuntimePlan,
                SpiderRuntimePlan.plan_id == SpiderRuntimeScope.initial_plan_id,
            )
        ).all()
    roots: dict[str, tuple[bytes, str]] = {}
    for strategy_id, raw_checkpoint, checkpoint_sha256, raw_manifest in rows:
        checkpoint = bytes(raw_checkpoint)
        manifest = cast(dict[str, Any], decode_canonical(bytes(raw_manifest)))
        initial_manifest = cast(dict[str, Any], manifest["initial_manifest"])
        assert sha256(checkpoint).hexdigest() == checkpoint_sha256
        assert manifest["build_checkpoint_sha256"] == checkpoint_sha256
        assert initial_manifest["build_checkpoint_sha256"] == checkpoint_sha256
        roots[strategy_id] = (checkpoint, checkpoint_sha256)
    return roots


def test_populated_predecessor_transport_migration_backfill_and_downgrade(
    fresh_pg_db: str, mock_exchange_adapter: Any
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.begin() as conn:
            for strategy_id in ("spider-main", "spider-secondary"):
                conn.execute(
                    sa.text(
                        "INSERT INTO strategy(id, name, configuration_json) "
                        "VALUES (:id, 'Spider fixture', '{}')"
                    ),
                    {"id": strategy_id},
                )
                conn.execute(
                    sa.text(
                        "INSERT INTO strategy_state(strategy_id, status, config_json, version) "
                        "VALUES (:id, 'ACTIVE', '{}', 7)"
                    ),
                    {"id": strategy_id},
                )
        # These go through the existing owner, financial receiver, and STOP
        # path: the predecessor contains actual INSTALL, receipt, and control
        # rows rather than schema-only or hand-authored financial fixtures.
        seeded = [
            _seed_install_receipt_stop(engine, strategy_id, mock_exchange_adapter)
            for strategy_id in ("spider-main", "spider-secondary")
        ]
        checkpoint_roots = _build_checkpoint_roots(engine)
        assert set(checkpoint_roots) == {"spider-main", "spider-secondary"}
        _downgrade(fresh_pg_db, "a7d0c5e2f941")
        predecessor = _legacy_snapshot(engine)
        assert predecessor["spider_runtime_delivery"]
        assert predecessor["spider_control_receipt"]
        assert predecessor["trade"]

        _upgrade(fresh_pg_db, "head")
        factory = sessionmaker(engine)
        with factory() as session:
            plans = session.execute(
                select(
                    SpiderRuntimePlan.strategy_id,
                    SpiderRuntimePlan.original_build_checkpoint,
                    SpiderRuntimePlan.original_build_checkpoint_sha256,
                )
            ).all()
        assert {
            strategy_id: (bytes(checkpoint), checkpoint_sha256)
            for strategy_id, checkpoint, checkpoint_sha256 in plans
        } == checkpoint_roots
        for owner, scope, operation_id, _install, stopped in seeded:
            assert owner.readback(scope, operation_id) == stopped
        with engine.connect() as conn:
            scopes = conn.execute(
                sa.text(
                    "SELECT environment, execution_scope_id, strategy_id, "
                    "last_received_sequence, last_tape_sequence "
                    "FROM spider_runtime_scope ORDER BY strategy_id"
                )
            ).all()
            for environment, execution_scope_id, strategy_id, received, tape in scopes:
                indexed = conn.execute(
                    sa.text(
                        "SELECT ordinal, kind, delivery_sequence, transmission_group_id "
                        "FROM spider_runtime_tape_index WHERE environment=:environment "
                        "AND execution_scope_id=:execution_scope_id AND strategy_id=:strategy_id "
                        "ORDER BY ordinal"
                    ),
                    {
                        "environment": environment,
                        "execution_scope_id": execution_scope_id,
                        "strategy_id": strategy_id,
                    },
                ).all()
                assert tape == received
                assert received >= 2
                assert [row.ordinal for row in indexed] == list(range(1, received + 1))
                assert all(
                    row.kind == "DELIVERY"
                    and row.delivery_sequence == row.ordinal
                    and row.transmission_group_id is None
                    for row in indexed
                )
        assert _legacy_snapshot(engine) == predecessor

        _downgrade(fresh_pg_db, "a7d0c5e2f941")
        assert _legacy_snapshot(engine) == predecessor
        _upgrade(fresh_pg_db, "head")
        for owner, scope, operation_id, _install, stopped in seeded:
            assert owner.readback(scope, operation_id) == stopped
        assert _legacy_snapshot(engine) == predecessor
        with engine.connect() as conn:
            assert (
                conn.execute(
                    sa.text(
                        "SELECT count(*) FROM spider_runtime_tape_index WHERE kind='SEND_MARKER'"
                    )
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()
