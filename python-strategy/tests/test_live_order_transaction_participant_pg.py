"""Opt-in PostgreSQL execution of the live repository participant contract."""

import os
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from src.core.execution import ExecutionEngine
from src.core.orm_models import Order, Product, Strategy, Trade
from src.core.repositories import LiveOrderRepository
from test_repositories import TestLiveOrderRepositoryTransactionParticipant as _Contract
from test_migrations import fresh_pg_db as _migration_fresh_pg_db

fresh_pg_db = _migration_fresh_pg_db

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
    @pytest.mark.parametrize("fail_participant", [False, True])
    def test_fill_participant_can_target_flush_original_trade_after_prewrite_proof(
        self,
        sqlite_order_session_factory,
        order_factory,
        fail_participant,
    ):
        factory = sqlite_order_session_factory
        order = order_factory(
            order_id=f"target-flush-original-trade-{fail_participant}",
            status="open",
        )
        LiveOrderRepository(db_session_factory=factory).add_order(order)
        order.status = "closed"
        order.filled_quantity = Decimal("1")
        order.filled_price = Decimal("42000")
        trade = Trade(
            id=f"target-flush-trade-{fail_participant}",
            order_id=order.id,
            exchange_trade_id=f"target-flush-trade-{fail_participant}",
            product_id=order.product_id,
            side=order.side,
            price=Decimal("42000"),
            quantity=Decimal("1"),
            fee=Decimal("0"),
            fee_asset="USDT",
            timestamp=1_800_000_001_000,
        )
        trade_id = trade.id
        callback_sessions: list[Session] = []

        def participant(
            session: Session,
            intended_order: Order,
            original_trade: Trade | None,
        ) -> None:
            callback_sessions.append(session)
            assert session.in_transaction() and session.is_active
            assert intended_order is order
            assert original_trade is trade
            assert original_trade in session.new
            assert session.is_modified(intended_order)
            assert session.execute(
                select(Order.status).where(Order.id == order.id)
            ).scalar_one() == "open"
            session.flush([original_trade])
            assert session.execute(
                select(Trade.id).where(Trade.id == trade_id)
            ).scalar_one() == trade_id
            assert session.execute(
                select(Order.status).where(Order.id == order.id)
            ).scalar_one() == "open"
            with factory() as independent:
                assert independent.get(Trade, trade_id) is None
                persisted_order = independent.get(Order, order.id)
                assert persisted_order is not None
                assert persisted_order.status == "open"
            if fail_participant:
                raise RuntimeError("participant failure after Trade flush")

        repository = LiveOrderRepository(
            db_session_factory=factory,
            transaction_participant=participant,
        )
        if fail_participant:
            with pytest.raises(
                RuntimeError,
                match="^participant failure after Trade flush$",
            ):
                repository.persist_fill(order, trade)
        else:
            repository.persist_fill(order, trade)

        assert len(callback_sessions) == 1
        with factory() as independent:
            persisted_order = independent.get(Order, order.id)
            assert persisted_order is not None
            assert persisted_order.status == ("open" if fail_participant else "closed")
            persisted_trade = independent.get(Trade, trade_id)
            assert (persisted_trade is None) is fail_participant

    @pytest.mark.parametrize("fail_participant", [False, True])
    def test_execution_engine_financial_update_shares_participant_transaction(
        self,
        sqlite_order_session_factory,
        order_factory,
        mock_clock,
        mock_exchange_adapter,
        fail_participant,
    ):
        factory = sqlite_order_session_factory
        engine = factory.kw["bind"]
        with engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE participant_marker (order_id TEXT NOT NULL)")
            )
        order = order_factory(
            order_id=f"execution-participant-{fail_participant}",
            status="open",
        )
        LiveOrderRepository(db_session_factory=factory).add_order(order)
        sessions_seen: list[Session] = []
        owned_sessions: list[Session] = []

        def tracked_sessions() -> Session:
            session = factory()
            owned_sessions.append(session)
            return session

        def participant(session: Session, intended_order: Order, _trade) -> None:
            sessions_seen.append(session)
            assert session.in_transaction()
            assert session.is_active
            assert session.get_bind().dialect.name == "postgresql"
            assert intended_order is order
            with factory() as independent:
                assert independent.query(Order.status).filter_by(id=order.id).scalar() == "open"
                assert independent.execute(
                    text("SELECT count(*) FROM participant_marker WHERE order_id=:id"),
                    {"id": order.id},
                ).scalar_one() == 0
            session.execute(
                text("INSERT INTO participant_marker (order_id) VALUES (:id)"),
                {"id": order.id},
            )
            assert session.execute(
                text("SELECT count(*) FROM participant_marker WHERE order_id=:id"),
                {"id": order.id},
            ).scalar_one() == 1
            with factory() as independent:
                assert independent.execute(
                    text("SELECT count(*) FROM participant_marker WHERE order_id=:id"),
                    {"id": order.id},
                ).scalar_one() == 0
            if fail_participant:
                raise RuntimeError("participant failure")

        execution = ExecutionEngine(
            db_session=None,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            db_session_factory=tracked_sessions,
            repository_transaction_participant=participant,
        )
        repository = execution.order_manager.repo
        assert isinstance(repository, LiveOrderRepository)
        order.status = "closed"

        if fail_participant:
            with pytest.raises(RuntimeError, match="^participant failure$"):
                repository.update_order(order)
        else:
            repository.update_order(order)

        assert len(sessions_seen) == 1
        assert owned_sessions[-1] is sessions_seen[0]
        with factory() as independent:
            persisted = independent.get(Order, order.id)
            assert persisted is not None
            assert persisted.status == ("open" if fail_participant else "closed")
            assert independent.execute(
                text("SELECT count(*) FROM participant_marker WHERE order_id=:id"),
                {"id": order.id},
            ).scalar_one() == (0 if fail_participant else 1)

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
