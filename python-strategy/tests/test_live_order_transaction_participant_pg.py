"""Opt-in PostgreSQL execution of the live repository participant contract."""

import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.core.orm_models import Strategy
from test_repositories import TestLiveOrderRepositoryTransactionParticipant as _Contract

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


@pytest.fixture
def sqlite_order_session_factory(fresh_pg_db):
    """Override the shared contract fixture with a fresh migrated PostgreSQL DB."""
    from test_migrations import _target_url, _upgrade

    _upgrade(fresh_pg_db)
    engine = create_engine(_target_url(fresh_pg_db), pool_pre_ping=True)
    with sessionmaker(bind=engine)() as session:
        session.add(Strategy(id="test_strategy", name="Test Strategy"))
        session.commit()
    try:
        yield sessionmaker(bind=engine)
    finally:
        engine.dispose()


class TestLiveOrderTransactionParticipantPostgres(_Contract):
    pass
