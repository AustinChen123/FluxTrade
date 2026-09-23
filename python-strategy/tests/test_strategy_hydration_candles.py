from unittest.mock import MagicMock
from typing import Any, cast

import pytest

from src.core.strategy_hydration_service import StrategyHydrationService
from test_profile_modeled_warmup import setup
from test_strategy_hydration_service import _service, _Strategy


@pytest.mark.parametrize("failure_phase", [None, "callback", "position"])
def test_real_callbacks_then_sync_only_on_success(failure_phase):
    strategy, candle, _, _, processor, execution, loader, seen, failure = setup()
    account = MagicMock()
    account.get_position.return_value = None
    service = StrategyHydrationService(
        signal_processor=processor, account_service=account
    )
    error = RuntimeError("injected failure")
    if failure_phase == "callback":
        failure.append(error)
    elif failure_phase == "position":
        account.get_position.side_effect = error
    published = []
    if failure_phase:
        with pytest.raises(RuntimeError) as caught:
            service.hydrate_candles(strategy, (candle,), decision_scope_loader=loader)
            published.append(strategy)
        if failure_phase == "callback":
            assert caught.value is error
            account.get_position.assert_not_called()
        else:
            assert caught.value.__cause__ is error
        assert not published
    else:
        assert (
            service.hydrate_candles(strategy, (candle,), decision_scope_loader=loader)
            == 1
        )
        account.get_position.assert_called_once_with(
            strategy.strategy_id, strategy.product_id
        )
    assert len(seen) == 1
    execution.execute_signal.assert_not_called()


def test_single_warmup_identity_and_order():
    service, processor, account = _service()
    strategy, candle, _, _, _, _, loader, _, _ = setup()
    second = candle.model_copy(update={"timestamp": candle.timestamp + 60000})
    order = []
    processor.warm_up.side_effect = lambda *args, **kwargs: order.append("warmup")
    account.get_position.side_effect = lambda *args: order.append("sync")
    assert (
        service.hydrate_candles(
            strategy, (candle, second), decision_scope_loader=loader
        )
        == 2
    )
    processor.warm_up.assert_called_once_with(
        strategy, [candle, second], decision_scope_loader=loader
    )
    assert processor.warm_up.call_args.args[1][0] is candle
    assert order == ["warmup", "sync"]


def test_empty_and_invalid_sequence():
    service, processor, account = _service()
    loader = MagicMock()
    strategy = _Strategy()
    assert service.hydrate_candles(strategy, (), decision_scope_loader=loader) == 0
    processor.warm_up.assert_called_once_with(
        strategy, [], decision_scope_loader=loader
    )
    account.get_position.assert_called_once()
    processor.reset_mock()
    account.reset_mock()
    for invalid in ([], (None,)):
        with pytest.raises(ValueError):
            service.hydrate_candles(
                strategy, cast(Any, invalid), decision_scope_loader=loader
            )
    processor.warm_up.assert_not_called()
    account.get_position.assert_not_called()


@pytest.mark.parametrize("loader", [None, True, object()])
def test_invalid_loader_rejected_even_for_empty(loader):
    service, processor, account = _service()
    with pytest.raises(ValueError):
        service.hydrate_candles(
            _Strategy(), (), decision_scope_loader=cast(Any, loader)
        )
    processor.warm_up.assert_not_called()
    account.get_position.assert_not_called()
