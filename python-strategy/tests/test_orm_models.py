from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.core.orm_models import StrategyState


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
