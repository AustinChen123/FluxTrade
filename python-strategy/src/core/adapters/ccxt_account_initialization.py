"""CCXT live-account initialization owner."""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

import ccxt

from src.core.interfaces.exchange import ExchangeError
from src.core.product_registry import to_ccxt_symbol


class AccountPositionMode(str, Enum):
    ONE_WAY = "one_way"


@dataclass(frozen=True)
class AccountInitializationConfig:
    """Live account settings that must be applied before trading starts."""

    product_ids: tuple[str, ...]
    leverage: int | None = None
    margin_mode: str | None = None
    position_mode: AccountPositionMode = AccountPositionMode.ONE_WAY

    @classmethod
    def from_config(
        cls,
        raw_config: dict | None,
        *,
        default_product_ids: list[str],
    ) -> "AccountInitializationConfig | None":
        if not raw_config:
            return None

        product_ids = tuple(
            raw_config.get("product_ids")
            or raw_config.get("instrument_product_ids")
            or default_product_ids
        )
        if not product_ids:
            raise ExchangeError(
                "account_initialization_requires_products: "
                "configure account_initialization.product_ids or instrument_product_ids"
            )

        position_mode = raw_config.get(
            "position_mode",
            AccountPositionMode.ONE_WAY.value,
        )
        if position_mode != AccountPositionMode.ONE_WAY.value:
            raise ExchangeError(
                f"unsupported_account_position_mode: position_mode={position_mode}"
            )

        leverage = raw_config.get("leverage")
        if leverage is not None:
            try:
                leverage = int(leverage)
            except (TypeError, ValueError) as error:
                raise ExchangeError(
                    f"invalid_account_leverage: leverage={leverage}"
                ) from error
            if leverage < 1:
                raise ExchangeError(f"invalid_account_leverage: leverage={leverage}")

        margin_mode = raw_config.get("margin_mode")
        if margin_mode is not None:
            margin_mode = str(margin_mode).lower()
            if margin_mode not in {"cross", "isolated"}:
                raise ExchangeError(
                    f"invalid_account_margin_mode: margin_mode={margin_mode}"
                )

        return cls(
            product_ids=product_ids,
            leverage=leverage,
            margin_mode=margin_mode,
            position_mode=AccountPositionMode.ONE_WAY,
        )


def initialize_ccxt_account(
    *,
    exchange_id: str,
    client: ccxt.Exchange,
    logger: logging.Logger,
    config: AccountInitializationConfig,
    operation_guard: Callable[[], None] | None = None,
) -> None:
    """Apply and verify configured live account settings."""
    guard = operation_guard or (lambda: None)
    guard()
    try:
        client.load_markets()
    except ccxt.BaseError as error:
        raise ExchangeError(
            f"account_initialization_load_markets_failed: {error}"
        ) from error
    guard()

    for product_id in config.product_ids:
        symbol = to_ccxt_symbol(product_id)
        _ensure_one_way_position_mode(exchange_id, client, symbol, guard)
        margin_mode_accepted = False
        if config.margin_mode is not None:
            margin_mode_accepted = _set_margin_mode(
                exchange_id,
                client,
                config.margin_mode,
                symbol,
                config,
                guard,
            )
        if config.leverage is not None:
            _set_leverage(exchange_id, client, config.leverage, symbol, guard)
            _verify_leverage(
                exchange_id,
                client,
                config.leverage,
                symbol,
                guard,
            )
        if config.margin_mode is not None:
            _verify_margin_mode(
                exchange_id,
                client,
                logger,
                config.margin_mode,
                symbol,
                allow_unsupported=margin_mode_accepted,
                operation_guard=guard,
            )


def _ensure_one_way_position_mode(
    exchange_id: str,
    client: ccxt.Exchange,
    symbol: str,
    operation_guard: Callable[[], None],
) -> None:
    set_position_mode = getattr(client, "set_position_mode", None)
    if not callable(set_position_mode):
        raise ExchangeError(
            f"account_position_mode_unsupported: exchange={exchange_id}"
        )
    set_accepted = False
    operation_guard()
    try:
        set_position_mode(False, symbol)
        set_accepted = True
    except ccxt.BaseError as error:
        if not _is_account_setting_no_change_error(error):
            raise ExchangeError(
                f"account_position_mode_set_failed: symbol={symbol} error={error}"
            ) from error
        set_accepted = True
    operation_guard()

    fetch_position_mode = getattr(client, "fetch_position_mode", None)
    if not callable(fetch_position_mode):
        if set_accepted:
            return
        raise ExchangeError(
            f"account_position_mode_verification_unsupported: exchange={exchange_id}"
        )
    operation_guard()
    try:
        result = fetch_position_mode(symbol)
    except ccxt.BaseError as error:
        if set_accepted and _is_position_mode_verification_unsupported(error):
            operation_guard()
            return
        raise ExchangeError(
            f"account_position_mode_verify_failed: symbol={symbol} error={error}"
        ) from error
    operation_guard()

    hedged = result.get("hedged") if isinstance(result, dict) else None
    if hedged is not False:
        raise ExchangeError(
            f"account_position_mode_not_one_way: symbol={symbol} hedged={hedged}"
        )


def _set_margin_mode(
    exchange_id: str,
    client: ccxt.Exchange,
    margin_mode: str,
    symbol: str,
    config: AccountInitializationConfig,
    operation_guard: Callable[[], None],
) -> bool:
    set_margin_mode = getattr(client, "set_margin_mode", None)
    if not callable(set_margin_mode):
        raise ExchangeError(f"account_margin_mode_unsupported: exchange={exchange_id}")
    params = {}
    if config.leverage is not None:
        params["leverage"] = str(config.leverage)
    operation_guard()
    try:
        set_margin_mode(margin_mode, symbol, params)
    except ccxt.BaseError as error:
        if not _is_account_setting_no_change_error(error):
            raise ExchangeError(
                f"account_margin_mode_set_failed: symbol={symbol} error={error}"
            ) from error
    operation_guard()
    return True


def _set_leverage(
    exchange_id: str,
    client: ccxt.Exchange,
    leverage: int,
    symbol: str,
    operation_guard: Callable[[], None],
) -> None:
    set_leverage = getattr(client, "set_leverage", None)
    if not callable(set_leverage):
        raise ExchangeError(f"account_leverage_unsupported: exchange={exchange_id}")
    operation_guard()
    try:
        set_leverage(leverage, symbol)
    except ccxt.BaseError as error:
        if not _is_account_setting_no_change_error(error):
            raise ExchangeError(
                f"account_leverage_set_failed: symbol={symbol} error={error}"
            ) from error
    operation_guard()


def _verify_leverage(
    exchange_id: str,
    client: ccxt.Exchange,
    expected_leverage: int,
    symbol: str,
    operation_guard: Callable[[], None],
) -> None:
    leverage = _fetch_leverage_value(exchange_id, client, symbol, operation_guard)
    if leverage != expected_leverage:
        raise ExchangeError(
            "account_leverage_not_configured: "
            f"symbol={symbol} expected={expected_leverage} actual={leverage}"
        )


def _verify_margin_mode(
    exchange_id: str,
    client: ccxt.Exchange,
    logger: logging.Logger,
    expected_margin_mode: str,
    symbol: str,
    *,
    allow_unsupported: bool,
    operation_guard: Callable[[], None],
) -> None:
    try:
        margin_mode = _fetch_margin_mode_value(
            exchange_id,
            client,
            symbol,
            operation_guard,
        )
    except ExchangeError as error:
        if allow_unsupported and str(error).startswith(
            "account_margin_mode_verification_unsupported"
        ):
            logger.warning(
                "Margin mode verification unsupported for %s on %s after accepted set_margin_mode",
                symbol,
                exchange_id,
            )
            return
        raise
    if margin_mode != expected_margin_mode:
        raise ExchangeError(
            "account_margin_mode_not_configured: "
            f"symbol={symbol} expected={expected_margin_mode} actual={margin_mode}"
        )


def _fetch_leverage_value(
    exchange_id: str,
    client: ccxt.Exchange,
    symbol: str,
    operation_guard: Callable[[], None],
) -> int | None:
    fetch_leverage = getattr(client, "fetch_leverage", None)
    if callable(fetch_leverage):
        operation_guard()
        try:
            leverage = _leverage_value_from_result(fetch_leverage(symbol))
            if leverage is not None:
                operation_guard()
                return leverage
        except ccxt.BaseError:
            pass
        operation_guard()

    fetch_leverages = getattr(client, "fetch_leverages", None)
    if callable(fetch_leverages):
        operation_guard()
        try:
            leverages = fetch_leverages([symbol])
            result = leverages.get(symbol) if isinstance(leverages, dict) else None
            leverage = _leverage_value_from_result(result)
            if leverage is not None:
                operation_guard()
                return leverage
        except ccxt.BaseError:
            pass
        operation_guard()

    raise ExchangeError(
        f"account_leverage_verification_unsupported: exchange={exchange_id}"
    )


def _leverage_value_from_result(result) -> int | None:
    if not isinstance(result, dict):
        return None
    long_leverage = result.get("longLeverage")
    short_leverage = result.get("shortLeverage")
    if long_leverage is not None and short_leverage is not None:
        if int(long_leverage) == int(short_leverage):
            return int(long_leverage)
        return None
    leverage = result.get("leverage")
    return int(leverage) if leverage is not None else None


def _fetch_margin_mode_value(
    exchange_id: str,
    client: ccxt.Exchange,
    symbol: str,
    operation_guard: Callable[[], None],
) -> str | None:
    fetch_margin_mode = getattr(client, "fetch_margin_mode", None)
    if callable(fetch_margin_mode):
        operation_guard()
        try:
            margin_mode = _margin_mode_from_result(fetch_margin_mode(symbol))
            if margin_mode is not None:
                operation_guard()
                return margin_mode
        except ccxt.BaseError:
            pass
        operation_guard()

    fetch_leverage = getattr(client, "fetch_leverage", None)
    if callable(fetch_leverage):
        operation_guard()
        try:
            margin_mode = _margin_mode_from_result(fetch_leverage(symbol))
            if margin_mode is not None:
                operation_guard()
                return margin_mode
        except ccxt.BaseError:
            pass
        operation_guard()

    raise ExchangeError(
        f"account_margin_mode_verification_unsupported: exchange={exchange_id}"
    )


def _margin_mode_from_result(result) -> str | None:
    if not isinstance(result, dict):
        return None
    margin_mode = result.get("marginMode") or result.get("marginType")
    return str(margin_mode).lower() if margin_mode is not None else None


def _is_account_setting_no_change_error(error: ccxt.BaseError) -> bool:
    message = str(error).lower()
    return (
        "no need to change" in message
        or "not modified" in message
        or "-4059" in message
        or "110025" in message
        or "110026" in message
        or "110043" in message
        or "140025" in message
        or "140026" in message
        or "140043" in message
        or "34036" in message
    )


def _is_position_mode_verification_unsupported(error: ccxt.BaseError) -> bool:
    message = str(error).lower()
    return "fetchpositionmode" in message and "not supported" in message
