from dataclasses import replace
from decimal import Decimal
from unittest.mock import MagicMock
from typing import Any, cast

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from src.core.orm_models import Candlestick as Row
from src.core.strategy_hydration_service import (
    FixedCandleWindowError,
    StrategyHydrationService,
)
from test_modeled_bootstrap_seed import setup, build, S


@pytest.fixture
def database():
    engine = create_engine("sqlite://")
    Row.__table__.create(engine)
    statements = []
    event.listen(
        engine, "before_cursor_execute", lambda *args: statements.append(args[2:4])
    )
    with Session(engine) as db:
        yield db, statements
    engine.dispose()


def service():
    return StrategyHydrationService(
        signal_processor=MagicMock(), account_service=MagicMock()
    )


def insert(db, candles):
    db.add_all([Row(**c.model_dump()) for c in candles])
    db.commit()


@pytest.mark.parametrize("status", list(S))
def test_exact_sql_window_detached_then_real_builder(database, status):
    db, statements = database
    data = setup(status)
    candles, cutover = data[3:5]
    extras = [
        candles[0].model_copy(update=update)
        for update in (
            {"timestamp": candles[0].timestamp - 60000},
            {"timestamp": cutover},
            {"timestamp": cutover + 60000},
            {"product_id": "BINANCE:ETHUSDT-SPOT"},
            {"timeframe": "5m"},
        )
    ]
    insert(db, (*reversed(candles), *extras))
    statements.clear()
    reader = service()
    result = reader.read_fixed_candles(
        db, data[2], cutover_ms=cutover, max_seed_candles=2
    )
    assert result == candles and type(result) is tuple
    sql, params = statements[0]
    assert len(statements) == 1
    assert "timestamp >=" in sql and "timestamp <" in sql and "timestamp ASC" in sql
    assert params == (data[2].product_id, "1m", candles[0].timestamp, cutover, 3, 0)
    data[5].context_for.assert_not_called()
    db.close()
    assert build(data, candles=result).candles[1].context == data[6][1]
    assert data[5].context_for.call_count == 2
    assert (
        not cast(MagicMock, reader._signal_processor).mock_calls
        and not cast(MagicMock, reader._account_service).mock_calls
    )


@pytest.mark.parametrize("damage", ["missing", "gap", "off_grid", "excess"])
def test_incomplete_window_never_borrows_earlier_rows(database, damage):
    db, _ = database
    data = setup()
    first, last = data[3]
    rows = [first.model_copy(update={"timestamp": first.timestamp - 60000})]
    if damage == "missing":
        rows += [first]
    elif damage == "gap":
        rows += [last]
    elif damage == "off_grid":
        rows += [first, last.model_copy(update={"timestamp": last.timestamp + 1})]
    else:
        rows += [
            first,
            last,
            first.model_copy(update={"timestamp": first.timestamp + 1}),
        ]
    insert(db, rows)
    with pytest.raises(FixedCandleWindowError, match="^FIXED_CANDLE_WINDOW_INVALID$"):
        service().read_fixed_candles(
            db, data[2], cutover_ms=data[4], max_seed_candles=2
        )


class Integer(int):
    pass


@pytest.mark.parametrize(
    "field,value",
    [
        ("lookback_window", True),
        ("lookback_window", -1),
        ("lookback_window", 3),
        ("cutover_ms", True),
        ("cutover_ms", -1),
        ("cutover_ms", 0),
        ("cutover_ms", 86400001),
        ("cutover_ms", 1 << 63),
        ("cutover_ms", Integer(86400000)),
        ("max_seed_candles", True),
        ("max_seed_candles", 0),
        ("max_seed_candles", -1),
        ("product_id", "SECRET"),
        ("timeframe", ""),
        ("timeframe", "0m"),
        ("timeframe", "1w"),
        ("requirements", None),
    ],
)
def test_preflight_zero_sql(database, field, value):
    db, statements = database
    data = setup()
    args: dict[str, Any] = dict(
        requirements=data[2], cutover_ms=data[4], max_seed_candles=2
    )
    if field in ("lookback_window", "product_id", "timeframe"):
        args["requirements"] = replace(data[2], **{field: value})
    else:
        args[field] = value
    with pytest.raises(FixedCandleWindowError):
        service().read_fixed_candles(db, **args)
    assert statements == []


def test_zero_lookback_zero_sql(database):
    db, statements = database
    requirements = replace(setup()[2], lookback_window=0, timeframe="01m")
    assert (
        service().read_fixed_candles(db, requirements, cutover_ms=0, max_seed_candles=1)
        == ()
    )
    assert statements == []


class Halt(BaseException):
    pass


@pytest.mark.parametrize("error", [RuntimeError("SECRET"), Halt("SECRET")])
def test_db_error_identity(error):
    db = MagicMock()
    db.query.side_effect = error
    data = setup()
    with pytest.raises(type(error)) as caught:
        service().read_fixed_candles(
            db, data[2], cutover_ms=data[4], max_seed_candles=2
        )
    assert caught.value is error
    db.commit.assert_not_called()
    db.rollback.assert_not_called()


def test_pending_session_state_is_untouched_and_persisted_values_are_read(database):
    db, statements = database
    data = setup()
    candles = data[3]
    outside = candles[0].model_copy(update={"timestamp": data[4]})
    insert(db, (*candles, outside))
    dirty = db.get(Row, (candles[0].product_id, "1m", candles[0].timestamp))
    deleted = db.get(Row, (outside.product_id, "1m", outside.timestamp))
    assert dirty is not None and deleted is not None
    dirty.close = Decimal("999")
    new = Row(**outside.model_copy(update={"timestamp": data[4] + 60000}).model_dump())
    db.add(new)
    db.delete(deleted)
    pending = tuple(tuple(group) for group in (db.dirty, db.new, db.deleted))
    states = [dict(row.__dict__) for row in (dirty, new, deleted)]
    statements.clear()
    result = service().read_fixed_candles(
        db, data[2], cutover_ms=data[4], max_seed_candles=2
    )
    assert result == candles and result[0].close != dirty.close
    assert len(statements) == 1 and statements[0][0].lstrip().startswith("SELECT")
    assert tuple(tuple(group) for group in (db.dirty, db.new, db.deleted)) == pending
    assert [dict(row.__dict__) for row in (dirty, new, deleted)] == states
