from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from src.core import live_candle_application
from src.core.live_candle_application import LiveCandleApplicationService


@contextmanager
def _session_factory():
    session = MagicMock()
    session.get.return_value = None
    yield session


def test_non_live_application_runs_callback_without_persistence() -> None:
    service = LiveCandleApplicationService(
        environment_identity=lambda: "backtest",
        db_session_factory=_session_factory,
    )
    candle = MagicMock()
    apply_new = MagicMock()
    rebuild_applied = MagicMock()

    service.apply(
        candle,
        apply_new=apply_new,
        rebuild_applied=rebuild_applied,
    )

    apply_new.assert_called_once_with(candle)
    rebuild_applied.assert_not_called()


@pytest.mark.parametrize("already_applied", [False, True])
def test_apply_selects_exactly_one_application_disposition(
    sample_candlestick,
    already_applied: bool,
) -> None:
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=_session_factory,
    )
    candle = sample_candlestick
    events: list[str] = []
    service.was_applied = MagicMock(return_value=already_applied)
    service.assert_newer = MagicMock(side_effect=lambda _candle: events.append("newer"))
    service._assert_compatible = MagicMock(
        side_effect=lambda _candle: events.append("compatible")
    )
    service._persist = MagicMock(side_effect=lambda _candle: events.append("persist"))

    service.apply(
        candle,
        apply_new=lambda _candle: events.append("apply"),
        rebuild_applied=lambda _candle: events.append("rebuild"),
    )

    assert events == (
        ["rebuild"] if already_applied else ["newer", "compatible", "apply", "persist"]
    )


def test_callback_failure_prevents_persistence_and_preserves_identity(
    sample_candlestick,
) -> None:
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=_session_factory,
    )
    failure = RuntimeError("application failed")
    service.was_applied = MagicMock(return_value=False)
    service.assert_newer = MagicMock()
    service._assert_compatible = MagicMock()
    service._persist = MagicMock()

    with pytest.raises(RuntimeError) as raised:
        service.apply(
            sample_candlestick,
            apply_new=MagicMock(side_effect=failure),
            rebuild_applied=MagicMock(),
        )

    assert raised.value is failure
    service._persist.assert_not_called()


@pytest.mark.parametrize("already_applied", [False, True])
def test_replay_rewinds_only_an_unapplied_candle(
    sample_candlestick,
    already_applied: bool,
) -> None:
    service = LiveCandleApplicationService(
        environment_identity=lambda: "backtest",
        db_session_factory=_session_factory,
    )
    candle = sample_candlestick
    events: list[str] = []
    service.was_applied = MagicMock(return_value=already_applied)
    service.apply = MagicMock(
        side_effect=lambda *_args, **_kwargs: events.append("apply")
    )

    service.replay(
        candle,
        rewind_pending=lambda _candle: events.append("rewind"),
        apply_new=MagicMock(),
        rebuild_applied=lambda _candle: events.append("rebuild"),
    )

    assert events == (["rebuild"] if already_applied else ["rewind", "apply"])


def test_environment_identity_is_resolved_at_call_time(sample_candlestick) -> None:
    environment = "backtest"
    service = LiveCandleApplicationService(
        environment_identity=lambda: environment,
        db_session_factory=_session_factory,
    )

    assert service.was_applied(sample_candlestick) is False
    environment = "live"
    assert service.was_applied(sample_candlestick) is False


@pytest.mark.parametrize("failure_point", ["add", "commit"])
def test_persistence_failure_rolls_back_and_preserves_exception_identity(
    sample_candlestick,
    monkeypatch,
    failure_point: str,
) -> None:
    session = MagicMock()
    session.get.side_effect = [None, None]
    failure = RuntimeError(f"{failure_point} failed")
    if failure_point == "add":
        session.add.side_effect = failure
    else:
        session.commit.side_effect = failure

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(
        live_candle_application,
        "ensure_product_registered",
        MagicMock(),
    )
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=session_factory,
    )

    with pytest.raises(RuntimeError) as raised:
        service._persist(sample_candlestick)

    assert raised.value is failure
    session.rollback.assert_called_once_with()
    if failure_point == "add":
        session.commit.assert_not_called()


def test_concurrent_receipt_fails_closed_and_rolls_back(
    sample_candlestick,
    monkeypatch,
) -> None:
    session = MagicMock()
    session.get.side_effect = [None, object()]

    @contextmanager
    def session_factory():
        yield session

    monkeypatch.setattr(
        live_candle_application,
        "ensure_product_registered",
        MagicMock(),
    )
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=session_factory,
    )

    with pytest.raises(
        RuntimeError,
        match="live application receipt appeared concurrently",
    ):
        service._persist(sample_candlestick)

    session.rollback.assert_called_once_with()
    session.commit.assert_not_called()


def test_owner_has_no_concrete_venue_dependency() -> None:
    from pathlib import Path

    source = (
        Path(__file__).parents[1] / "src" / "core" / "live_candle_application.py"
    ).read_text()

    for venue in ("rithmic", "binance", "backpack", "bybit", "okx", "ccxt"):
        assert venue not in source.lower()
