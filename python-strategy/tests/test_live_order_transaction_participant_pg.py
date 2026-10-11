"""Opt-in PostgreSQL execution of the live repository participant contract."""

import os

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.core.orm_models import Order, Product, Strategy
from src.core.repositories import LiveOrderRepository
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
    def test_prepared_order_stages_only_in_caller_owned_transaction(
        self, sqlite_order_session_factory, order_factory
    ):
        factory = sqlite_order_session_factory
        engine = factory.kw["bind"]
        participant_calls = []
        repo = LiveOrderRepository(
            db_session_factory=factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
            transaction_participant=lambda *_: participant_calls.append(True),
        )
        order = order_factory(
            order_id="caller-staged-order",
            account_profile=None,
            account_id=None,
        )
        order_id = order.id

        with factory() as session:
            with session.begin():
                repo.stage_order_in_transaction(session, order)
                assert session.get(Order, order_id) is order
                assert order.account_profile == "binance"
                assert order.account_id == "ACCOUNT-A"
                with factory() as independent:
                    assert independent.get(Order, order_id) is None
                assert participant_calls == []

        with factory() as independent:
            persisted = independent.get(Order, order_id)
            assert persisted is not None
            assert persisted.account_profile == "binance"
            assert persisted.account_id == "ACCOUNT-A"
        assert participant_calls == []
        assert engine.dialect.name == "postgresql"

    def test_caller_rollback_removes_staged_order_and_product(
        self, sqlite_order_session_factory, order_factory
    ):
        factory = sqlite_order_session_factory
        repo = LiveOrderRepository(db_session_factory=factory)
        order = order_factory(
            order_id="caller-rollback-order",
            product_id="BINANCE:BNBUSDT-PERP",
        )
        order_id = order.id
        product_id = order.product_id

        with pytest.raises(RuntimeError, match="^caller rollback$"):
            with factory() as session:
                with session.begin():
                    repo.stage_order_in_transaction(session, order)
                    assert session.get(Order, order_id) is order
                    with factory() as independent:
                        assert independent.get(Order, order_id) is None
                        assert independent.get(Product, product_id) is None
                    raise RuntimeError("caller rollback")

        with factory() as independent:
            assert independent.get(Order, order_id) is None
            assert independent.get(Product, product_id) is None

    @pytest.mark.parametrize("session_state", ["unbound", "ended"])
    def test_staging_rejects_unbound_or_ended_caller_session(
        self, sqlite_order_session_factory, order_factory, session_state
    ):
        factory = sqlite_order_session_factory
        repo = LiveOrderRepository(db_session_factory=factory)
        order = order_factory(order_id=f"caller-{session_state}-order")

        with factory() as session:
            if session_state == "ended":
                with session.begin():
                    pass
            with pytest.raises(
                RuntimeError,
                match="^active_postgresql_order_transaction_required$",
            ):
                repo.stage_order_in_transaction(session, order)
            assert not session.in_transaction()

        with factory() as independent:
            assert independent.get(Order, order.id) is None

    def test_staging_rejects_inactive_caller_session(
        self, sqlite_order_session_factory, order_factory
    ):
        factory = sqlite_order_session_factory
        repo = LiveOrderRepository(db_session_factory=factory)
        order = order_factory(order_id="caller-inactive-order")

        with factory() as session:
            session.begin()
            session.add(Strategy(id="test_strategy", name="Duplicate Strategy"))
            with pytest.raises(IntegrityError):
                session.flush()
            assert session.in_transaction()
            assert not session.is_active
            with pytest.raises(
                RuntimeError,
                match="^active_postgresql_order_transaction_required$",
            ):
                repo.stage_order_in_transaction(session, order)
            session.rollback()

        with factory() as independent:
            assert independent.get(Order, order.id) is None

    def test_staging_rejects_account_mismatch_without_leak(
        self, sqlite_order_session_factory, order_factory
    ):
        factory = sqlite_order_session_factory
        repo = LiveOrderRepository(
            db_session_factory=factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
        )
        order = order_factory(
            order_id="caller-account-mismatch-order",
            account_profile="binance",
            account_id="ACCOUNT-B",
        )

        with factory() as session:
            with session.begin():
                with pytest.raises(
                    RuntimeError, match="^order_account_identity_mismatch$"
                ):
                    repo.stage_order_in_transaction(session, order)
                assert session.get(Order, order.id) is None

        with factory() as independent:
            assert independent.get(Order, order.id) is None

    @pytest.mark.parametrize("collision_key", ["client", "exchange"])
    def test_staging_rejects_legacy_identifier_collision_without_leak(
        self, sqlite_order_session_factory, order_factory, collision_key
    ):
        factory = sqlite_order_session_factory
        legacy = order_factory(
            order_id=f"legacy-{collision_key}",
            client_order_id="legacy-client" if collision_key == "client" else None,
            exchange_order_id="legacy-exchange" if collision_key == "exchange" else None,
        )
        LiveOrderRepository(db_session_factory=factory).add_order(legacy)
        repo = LiveOrderRepository(
            db_session_factory=factory,
            account_profile="binance",
            account_id="ACCOUNT-A",
        )
        order = order_factory(
            order_id=f"identified-{collision_key}",
            client_order_id="legacy-client" if collision_key == "client" else None,
            exchange_order_id="legacy-exchange" if collision_key == "exchange" else None,
        )

        with factory() as session:
            with session.begin():
                with pytest.raises(
                    RuntimeError,
                    match="^order_account_identity_legacy_collision$",
                ):
                    repo.stage_order_in_transaction(session, order)
                assert session.get(Order, order.id) is None

        with factory() as independent:
            assert independent.get(Order, order.id) is None
            assert independent.get(Order, legacy.id) is not None

    def test_staging_rejects_conflicting_product_master_without_order_leak(
        self, sqlite_order_session_factory, order_factory
    ):
        factory = sqlite_order_session_factory
        product_id = "BINANCE:ETHUSDT-PERP"
        order = order_factory(
            order_id="caller-product-conflict-order", product_id=product_id
        )
        repo = LiveOrderRepository(db_session_factory=factory)
        with factory() as session:
            with session.begin():
                product = session.get(Product, product_id)
                if product is None:
                    session.add(
                        Product(
                            id=product_id,
                            exchange_id="BINANCE",
                            base_asset="WRONG",
                            quote_asset="USDT",
                        )
                    )
                else:
                    session.execute(
                        update(Product)
                        .where(Product.id == product_id)
                        .values(base_asset="WRONG")
                    )
                session.flush()
                assert session.execute(
                    Product.__table__.select()
                    .with_only_columns(Product.base_asset, Product.quote_asset)
                    .where(Product.id == product_id)
                ).one() == ("WRONG", "USDT")

        with factory() as session:
            with session.begin():
                with pytest.raises(
                    ValueError,
                    match="^product master data conflicts with canonical identity",
                ):
                    repo.stage_order_in_transaction(session, order)
                assert session.get(Order, order.id) is None

        with factory() as independent:
            assert independent.get(Order, order.id) is None

    def test_staging_leaves_caller_transaction_and_lifecycle_owned_by_caller(
        self, sqlite_order_session_factory, order_factory, monkeypatch
    ):
        factory = sqlite_order_session_factory
        participant_calls = []
        repo = LiveOrderRepository(
            db_session_factory=factory,
            transaction_participant=lambda *_: participant_calls.append(True),
        )
        order = order_factory(order_id="caller-lifecycle-order")
        with factory() as session:
            with session.begin():
                calls = {name: 0 for name in ("commit", "rollback", "close", "refresh")}
                for name in calls:
                    original = getattr(session, name)

                    def tracked(*args, _name=name, _original=original, **kwargs):
                        calls[_name] += 1
                        return _original(*args, **kwargs)

                    monkeypatch.setattr(session, name, tracked)

                repo.stage_order_in_transaction(session, order)

                assert session.in_transaction()
                assert session.is_active
                assert calls == {name: 0 for name in calls}
                assert participant_calls == []
