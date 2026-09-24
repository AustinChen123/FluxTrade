"""Consume owner transaction contract, not runtime activation acceptance."""

from unittest.mock import MagicMock
import os

import pytest
from sqlalchemy.orm import Session, sessionmaker

from src.core.strategy_activation_service import (
    ProfileActivationConsumption,
    consume_profile_activation_request,
)
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestStore,
    ProfileActivationRequestStatus,
)
from src.core.strategy_state_manager import StrategyStateManager
from test_profile_activation_request import request
from test_strategy_activation_request_store_pg import prepare, terminal_sql_log
from test_strategy_state_request_atomic_pg import evidence

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


@pytest.mark.parametrize("phase", ["commit", "rollback", "ack_lost"])
def test_consume_caller_transaction_and_fresh_reentry(profile_repository_pg, phase):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    store = ProfileActivationRequestStore(sessionmaker(engine))
    admitted = store.admit(value, current=value.intent)
    manager = StrategyStateManager(MagicMock(), MagicMock())
    error = RuntimeError("fixed caller fault")
    result = None
    with terminal_sql_log(engine) as sql:
        with Session(engine) as first:
            try:
                with first.begin():
                    result = consume_profile_activation_request(
                        first, manager, value, terminal_at=admitted.requested_at
                    )
                    assert (
                        result.disposition is ProfileActivationConsumption.CONSUMED_NOW
                    )
                    if phase == "rollback":
                        raise error
                if phase == "ack_lost":
                    raise error
            except RuntimeError as caught:
                assert phase != "commit" and caught is error
            else:
                assert phase == "commit"
        assert result is not None
        expected = (
            (("READY", 0), [], "PENDING")
            if phase == "rollback"
            else (
                ("ACTIVE", 1),
                [
                    (
                        "READY",
                        "ACTIVE",
                        value.actor,
                        "ACTIVATION_COMMITTED",
                        admitted.requested_at,
                    )
                ],
                "CONSUMED",
            )
        )
        with engine.connect() as connection:
            assert evidence(connection) == expected
        confirmed = store.confirm(value)
        assert confirmed == (admitted if phase == "rollback" else result.record)
        if phase != "rollback":
            assert confirmed is not None
            assert confirmed.status is ProfileActivationRequestStatus.CONSUMED
            assert confirmed.terminal_at == admitted.requested_at
            assert confirmed.terminal_reason == "ACTIVATION_COMMITTED"
        updates = sum(statement.startswith("UPDATE") for statement in sql)
        assert updates == 1  # Request update; state SQL is not in this scoped log.
        if phase == "ack_lost":
            # A new manager/session cannot rely on the first call's in-memory state.
            fresh_manager = StrategyStateManager(MagicMock(), MagicMock())
            with Session(engine) as second, second.begin():
                assert second is not first
                replay = consume_profile_activation_request(
                    second, fresh_manager, value, terminal_at=admitted.requested_at
                )
                assert (
                    replay.disposition is ProfileActivationConsumption.ALREADY_CONSUMED
                )
                assert replay.record == result.record
            with engine.connect() as connection:
                assert evidence(connection) == expected
            assert store.confirm(value) == confirmed
            assert sum(statement.startswith("UPDATE") for statement in sql) == updates
