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
    service._persist = MagicMock(
        side_effect=lambda _candle, _pending: events.append("persist")
    )

    service.apply(
        candle,
        apply_new=lambda _candle: events.append("apply"),
        rebuild_applied=lambda _candle: events.append("rebuild"),
    )

    assert events == (
        ["rebuild"] if already_applied else ["newer", "compatible", "apply", "persist"]
    )


@pytest.mark.parametrize("contract", [False, True])
def test_public_apply_preserves_exact_pending_identity(sample_candlestick, contract):
    from test_profile_decision_application import batch

    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=_session_factory,
    )
    pending = batch() if contract else None
    service.was_applied = MagicMock(return_value=False)
    service.assert_newer = MagicMock()
    service._assert_compatible = MagicMock()
    service._persist = MagicMock()
    callback = MagicMock(return_value=pending)
    rebuild = MagicMock()
    service.apply(sample_candlestick, apply_new=callback, rebuild_applied=rebuild)
    callback.assert_called_once_with(sample_candlestick)
    rebuild.assert_not_called()
    service._persist.assert_called_once_with(sample_candlestick, pending)
    assert service._persist.call_args.args[1] is pending
    service.was_applied.return_value = True
    service.apply(sample_candlestick, apply_new=callback, rebuild_applied=rebuild)
    callback.assert_called_once()
    rebuild.assert_called_once_with(sample_candlestick)
    service._persist.assert_called_once()


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


@pytest.mark.parametrize("contract", [False, True])
@pytest.mark.parametrize("failure_point", [None, "helper", "commit"])
def test_contract_single_session_persistence(
    sample_candlestick, monkeypatch, contract, failure_point
):
    from contextlib import nullcontext
    from dataclasses import replace
    from test_profile_decision_application import batch

    session = MagicMock()
    session.get.side_effect = [None, None]
    factory = MagicMock(return_value=nullcontext(session))
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live", db_session_factory=factory
    )
    candle = sample_candlestick
    pending = (
        replace(
            batch(),
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            bar_start_ms=candle.timestamp,
        )
        if contract
        else None
    )
    helper = MagicMock()
    service.was_applied = MagicMock(return_value=False)
    service.assert_newer = MagicMock()
    service._assert_compatible = MagicMock()
    monkeypatch.setattr(live_candle_application, "append_decision_batch", helper)
    monkeypatch.setattr(
        live_candle_application, "ensure_product_registered", MagicMock()
    )
    error = RuntimeError("injected")
    if failure_point == "helper":
        if not contract:
            return
        helper.side_effect = error
    elif failure_point == "commit":
        session.commit.side_effect = error
    if failure_point:
        with pytest.raises(RuntimeError) as caught:
            service.apply(
                candle, apply_new=lambda _: pending, rebuild_applied=MagicMock()
            )
        assert caught.value is error
        session.rollback.assert_called_once()
    else:
        service.apply(candle, apply_new=lambda _: pending, rebuild_applied=MagicMock())
        session.commit.assert_called_once()
    factory.assert_called_once()
    receipt = session.add.call_args_list[-1].args[0]
    assert receipt.decision_contract_version == (1 if contract else None)
    if contract:
        helper.assert_called_once_with(session, pending)
        session.flush.assert_called_once()
    else:
        helper.assert_not_called()


def test_pending_identity_rejected_before_session(sample_candlestick):
    from typing import Any, cast
    from test_profile_decision_application import batch
    from dataclasses import replace

    factory = MagicMock()
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live", db_session_factory=factory
    )
    candle = sample_candlestick
    valid = replace(
        batch(),
        product_id=candle.product_id,
        timeframe=candle.timeframe,
        bar_start_ms=candle.timestamp,
    )
    for invalid in (
        True,
        replace(valid, environment="other"),
        replace(valid, bar_start_ms=candle.timestamp + 1),
        replace(valid, timeframe="other"),
    ):
        with pytest.raises(live_candle_application.DecisionBatchIntegrityError):
            service._persist(candle, cast(Any, invalid))
    factory.assert_not_called()


@pytest.mark.parametrize("marker", [None, 1, 2, True])
@pytest.mark.parametrize("present", [False, True])
def test_receipt_marker_requires_complete_evidence(
    sample_candlestick, monkeypatch, marker, present
):
    from types import SimpleNamespace
    from contextlib import nullcontext

    candle = sample_candlestick
    values = {
        name: getattr(candle, name)
        for name in ("open", "high", "low", "close", "volume")
    }
    session = MagicMock()
    session.get.side_effect = [
        SimpleNamespace(**values, decision_contract_version=marker),
        SimpleNamespace(**values),
    ]
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=lambda: nullcontext(session),
    )
    reader = MagicMock(
        return_value=SimpleNamespace(batch=MagicMock()) if present else None
    )
    monkeypatch.setattr(live_candle_application, "read_decision_batch", reader)
    if marker is None or type(marker) is int and marker == 1 and present:
        assert service.was_applied(candle)
    else:
        with pytest.raises(live_candle_application.DecisionBatchIntegrityError):
            service.was_applied(candle)
    if type(marker) is int and marker == 1:
        assert reader.call_args.args[0] is session
    else:
        reader.assert_not_called()


@pytest.mark.parametrize("contract", [False, True])
def test_applied_decision_batch_returns_exact_durable_evidence(
    sample_candlestick, monkeypatch, contract
):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from test_profile_decision_application import batch

    candle = sample_candlestick
    values = {
        name: getattr(candle, name)
        for name in ("open", "high", "low", "close", "volume")
    }
    pending = batch() if contract else None
    session = MagicMock()
    session.get.side_effect = [
        SimpleNamespace(
            **values,
            decision_contract_version=1 if contract else None,
        ),
        SimpleNamespace(**values),
    ]
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=lambda: nullcontext(session),
    )
    reader = MagicMock(return_value=SimpleNamespace(batch=pending))
    monkeypatch.setattr(live_candle_application, "read_decision_batch", reader)

    assert service.applied_decision_batch(candle) is pending
    if contract:
        reader.assert_called_once()
    else:
        reader.assert_not_called()


def test_applied_decision_batch_rejects_unapplied_candle(sample_candlestick):
    service = LiveCandleApplicationService(
        environment_identity=lambda: "live",
        db_session_factory=_session_factory,
    )

    with pytest.raises(RuntimeError, match="unapplied candle"):
        service.applied_decision_batch(sample_candlestick)


def test_owner_has_no_concrete_venue_dependency() -> None:
    from pathlib import Path

    source = (
        Path(__file__).parents[1] / "src" / "core" / "live_candle_application.py"
    ).read_text()

    for venue in ("rithmic", "binance", "backpack", "bybit", "okx", "ccxt"):
        assert venue not in source.lower()
