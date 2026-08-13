from sqlalchemy import BigInteger, DateTime, Numeric, String, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from src.core.orm_models import Order, StrategyState


def test_order_typed_mapping_preserves_database_contract() -> None:
    expected_columns = {
        "id": (String, False),
        "exchange_order_id": (String, True),
        "strategy_id": (String, False),
        "product_id": (String, False),
        "exchange_id": (String, False),
        "account_profile": (String, True),
        "account_id": (String, True),
        "type": (String, False),
        "side": (String, False),
        "price": (Numeric, True),
        "trigger_price": (Numeric, True),
        "quantity": (Numeric, False),
        "status": (String, False),
        "timestamp": (BigInteger, False),
        "filled_quantity": (Numeric, True),
        "filled_price": (Numeric, True),
        "client_order_id": (String, True),
        "intent_payload": (JSONB, True),
        "submitted_at": (DateTime, True),
        "acked_at": (DateTime, True),
        "last_reconciled_at": (DateTime, True),
    }

    assert list(Order.__table__.columns.keys()) == list(expected_columns)
    for name, (expected_type, nullable) in expected_columns.items():
        column = Order.__table__.columns[name]
        assert type(column.type) is expected_type
        assert column.nullable is nullable

    assert {
        name: str(column.type) for name, column in Order.__table__.columns.items()
    } == {
        "id": "VARCHAR",
        "exchange_order_id": "VARCHAR",
        "strategy_id": "VARCHAR",
        "product_id": "VARCHAR",
        "exchange_id": "VARCHAR",
        "account_profile": "VARCHAR(128)",
        "account_id": "VARCHAR(128)",
        "type": "VARCHAR",
        "side": "VARCHAR",
        "price": "NUMERIC",
        "trigger_price": "NUMERIC",
        "quantity": "NUMERIC",
        "status": "VARCHAR",
        "timestamp": "BIGINT",
        "filled_quantity": "NUMERIC",
        "filled_price": "NUMERIC",
        "client_order_id": "VARCHAR(128)",
        "intent_payload": "JSONB",
        "submitted_at": "DATETIME",
        "acked_at": "DATETIME",
        "last_reconciled_at": "DATETIME",
    }

    assert Order.__table__.columns.filled_quantity.default.arg == 0
    assert {
        foreign_key.target_fullname
        for column in Order.__table__.columns
        for foreign_key in column.foreign_keys
    } == {"strategy.id", "product.id", "exchange.id"}
    assert {constraint.name for constraint in Order.__table__.constraints} >= {
        "uq_order_exchange_id",
        "chk_order_account_identity_complete",
    }
    unique = next(
        constraint
        for constraint in Order.__table__.constraints
        if constraint.name == "uq_order_exchange_id"
    )
    assert [column.name for column in unique.columns] == [
        "exchange_order_id",
        "exchange_id",
    ]


def test_strategy_state_typed_mapping_preserves_sqlite_round_trip() -> None:
    db_engine = create_engine("sqlite:///:memory:")
    try:
        StrategyState.__table__.create(db_engine)

        with Session(db_engine) as session:
            session.add(
                StrategyState(
                    strategy_id="strategy-1",
                    status="READY",
                    config_json='{"product_id":"RITHMIC:MNQ-202509"}',
                    performance_json=None,
                    last_heartbeat=123,
                    uptime_start=100,
                )
            )
            session.commit()
            session.expire_all()

            state = session.get(StrategyState, "strategy-1")

            assert state is not None
            assert state.strategy_id == "strategy-1"
            assert state.status == "READY"
            assert state.config_json == '{"product_id":"RITHMIC:MNQ-202509"}'
            assert state.performance_json is None
            assert state.last_heartbeat == 123
            assert state.uptime_start == 100
            assert state.version == 0
    finally:
        db_engine.dispose()
