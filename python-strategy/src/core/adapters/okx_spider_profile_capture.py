"""Bounded read-only OKX Demo capture for a Spider financial profile."""

from __future__ import annotations

import time
from collections.abc import Callable
from decimal import Decimal
from hashlib import sha256
from logging import Logger, WARNING
from typing import Protocol, cast

from src.core.adapters.okx_spider_financial_profile import (
    OkxSpiderFinancialProfile,
    create_okx_spider_financial_profile,
)
from src.core.adapters.okx_spider_profile import (
    OkxSpiderProfilePrerequisite,
    _validate_selection,
    create_okx_spider_profile_prerequisite,
)
from src.core.adapters.okx_spider_transport import create_okx_spider_batch_client
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

_REST = "https://www.okx.com"
_DEMO_HEADER = "x-simulated-trading"
_TIMEOUT_MS = 5_000
_TOTAL_BUDGET_SECONDS = 60.0
_PAGE_LIMIT = 100
_MAX_PAGES = 10
_READ_ENDPOINTS = (
    "account/config", "public/instruments", "account/trade-fee",
    "account/positions", "trade/orders-pending", "account/balance",
)


class OkxSpiderProfileCaptureUnavailable(ValueError):
    """D5A identity could not be established from the initial read-only calls."""


class _OkxProfileClient(Protocol):
    urls: dict[str, object]
    headers: dict[str, object]
    timeout: int

    def privateGetAccountConfig(self, params: dict[str, object]) -> object: ...

    def publicGetPublicInstruments(self, params: dict[str, object]) -> object: ...

    def privateGetAccountTradeFee(self, params: dict[str, object]) -> object: ...

    def privateGetAccountPositions(self, params: dict[str, object]) -> object: ...

    def privateGetTradeOrdersPending(self, params: dict[str, object]) -> object: ...

    def privateGetAccountBalance(self, params: dict[str, object]) -> object: ...


def create_okx_spider_demo_profile_client(config: dict[str, object]) -> object:
    """Use the existing no-resend raw client with explicit OKX Demo routing."""
    safe_config = dict(config)
    headers_value = safe_config.get("headers")
    headers = dict(headers_value) if type(headers_value) is dict else {}
    for key, value in headers.items():
        if type(key) is str and key.lower() == _DEMO_HEADER and value != "1":
            raise ValueError("invalid OKX Spider Demo profile client")
    headers = {
        key: value for key, value in headers.items()
        if type(key) is not str or key.lower() != _DEMO_HEADER
    }
    headers[_DEMO_HEADER] = "1"
    safe_config["headers"] = headers
    safe_config["timeout"] = _TIMEOUT_MS
    safe_config["verbose"] = False
    options_value = safe_config.get("options")
    options = dict(options_value) if type(options_value) is dict else {}
    options["maxRetriesOnFailure"] = 0
    for endpoint in _READ_ENDPOINTS:
        endpoint_value = options.get(endpoint)
        endpoint_options = dict(endpoint_value) if type(endpoint_value) is dict else {}
        endpoint_options["maxRetriesOnFailure"] = 0
        options[endpoint] = endpoint_options
    safe_config["options"] = options
    urls_value = safe_config.get("urls")
    urls = dict(urls_value) if type(urls_value) is dict else {}
    api_value = urls.get("api")
    api = dict(api_value) if type(api_value) is dict else {}
    supplied_rest = api.get("rest")
    if supplied_rest not in (None, _REST):
        raise ValueError("invalid OKX Spider Demo profile client")
    api["rest"] = _REST
    urls["api"] = api
    safe_config["urls"] = urls
    client = create_okx_spider_batch_client(safe_config)
    client.logger = Logger("okx-spider-financial-profile")
    client.logger.setLevel(WARNING)
    client.logger.propagate = False
    return client


def capture_okx_spider_financial_profile(
    client: object,
    selection_bytes: bytes,
    fixed_usdt: Decimal,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    wall_time_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
) -> OkxSpiderFinancialProfile:
    """Capture D5A identity first, then bounded financial GETs, without retries."""
    api = _validate_client(client)
    selection = _decode_canonical(selection_bytes)
    try:
        _validate_selection(selection)
    except Exception:
        raise OkxSpiderProfileCaptureUnavailable(
            "OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE"
        ) from None
    if (
        type(selection) is not dict or not isinstance(fixed_usdt, Decimal)
        or not fixed_usdt.is_finite() or fixed_usdt < 0
    ):
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
    selection_row = cast(dict[str, object], selection)
    products = selection_row.get("products")
    if type(products) is not list:
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
    product_rows = cast(list[dict[str, object]], products)
    records: list[dict[str, object]] = []
    start = monotonic()

    account_capture = _read(api, "privateGetAccountConfig", {}, "account/config", records, wall_time_ms)
    instruments_capture = _read(
        api, "publicGetPublicInstruments", {"instType": "SWAP"},
        "public/instruments", records, wall_time_ms,
    )
    if account_capture is None:
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
    account, account_at = account_capture
    instruments = None if instruments_capture is None else instruments_capture[0]
    instruments_at = None if instruments_capture is None else instruments_capture[1]
    _build_prerequisite(
        selection_bytes, account, account_at, instruments, instruments_at,
    )

    initial_instruments_valid = (
        instruments is not None and _response_rows(instruments) is not None
    )
    unavailable: str | None = None if initial_instruments_valid else "CAPTURE_FAILED"
    seen_order_ids: set[str] = set()
    try:
        families = _selected_families(product_rows, instruments) if instruments is not None else ()
    except OkxSpiderProfileCaptureUnavailable:
        families = ()
        unavailable = "CAPTURE_FAILED"
    for family in families if unavailable is None else ():
        if not _within_budget(start, monotonic, reserve_seconds=10.0):
            unavailable = "BUDGET_EXHAUSTED"
            break
        params = {"instType": "SWAP", "instFamily": family}
        captured = _read(api, "privateGetAccountTradeFee", params, "account/trade-fee", records, wall_time_ms)
        if captured is None or _response_rows(captured[0]) is None:
            unavailable = "CAPTURE_FAILED"
            break

    if unavailable is None:
        if not _within_budget(start, monotonic, reserve_seconds=10.0):
            unavailable = "BUDGET_EXHAUSTED"
        else:
            captured = _read(api, "privateGetAccountPositions", {}, "account/positions", records, wall_time_ms)
            if captured is None or _response_rows(captured[0]) is None:
                unavailable = "CAPTURE_FAILED"

    if unavailable is None:
        cursor = ""
        for page_index in range(_MAX_PAGES):
            if not _within_budget(start, monotonic, reserve_seconds=10.0):
                unavailable = "BUDGET_EXHAUSTED"
                break
            params: dict[str, object] = {"limit": str(_PAGE_LIMIT)}
            if cursor:
                params["after"] = cursor
            captured = _read(
                api, "privateGetTradeOrdersPending", params,
                "trade/orders-pending", records, wall_time_ms,
            )
            if captured is None:
                unavailable = "CAPTURE_FAILED"
                break
            response = captured[0]
            rows = _response_rows(response)
            if rows is None:
                unavailable = "CAPTURE_FAILED"
                break
            for row in rows:
                order_id = row.get("ordId")
                if type(order_id) is not str or not order_id or order_id in seen_order_ids:
                    unavailable = "CAPTURE_FAILED"
                    break
                seen_order_ids.add(order_id)
            if unavailable is not None:
                break
            if len(rows) < _PAGE_LIMIT:
                break
            if page_index == _MAX_PAGES - 1:
                unavailable = "BUDGET_EXHAUSTED"
                break
            next_cursor = rows[-1].get("ordId")
            if type(next_cursor) is not str or not next_cursor or next_cursor == cursor:
                unavailable = "CAPTURE_FAILED"
                break
            cursor = next_cursor

    if unavailable is None:
        if not _within_budget(start, monotonic, reserve_seconds=10.0):
            unavailable = "BUDGET_EXHAUSTED"
        else:
            captured = _read(api, "privateGetAccountBalance", {}, "account/balance", records, wall_time_ms)
            if captured is None or _response_rows(captured[0]) is None:
                unavailable = "CAPTURE_FAILED"

    refreshed_account = _read_if_budget(
        api, "privateGetAccountConfig", {}, "account/config", records,
        wall_time_ms, start, monotonic,
    )
    refreshed_instruments = None
    if initial_instruments_valid:
        refreshed_instruments = _read_if_budget(
            api, "publicGetPublicInstruments", {"instType": "SWAP"},
            "public/instruments", records, wall_time_ms, start, monotonic,
        )
    refresh_reason = (
        "BUDGET_EXHAUSTED"
        if not _within_budget(start, monotonic, reserve_seconds=5.0)
        else "CAPTURE_FAILED"
    )
    if refreshed_account is not None and _response_rows(refreshed_account[0]) is not None:
        account, account_at = refreshed_account
    else:
        unavailable = unavailable or refresh_reason
    if initial_instruments_valid:
        if refreshed_instruments is not None and _response_rows(refreshed_instruments[0]) is not None:
            instruments, instruments_at = refreshed_instruments
        else:
            unavailable = unavailable or refresh_reason
    prerequisite = _build_prerequisite(
        selection_bytes, account, account_at, instruments, instruments_at
    )
    return create_okx_spider_financial_profile(
        prerequisite, fixed_usdt, tuple(records), unavailable
    )


def _decode_canonical(raw: bytes) -> object:
    try:
        value = decode_canonical(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise OkxSpiderProfileCaptureUnavailable(
            "OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE"
        ) from None
    if type(raw) is not bytes or canonical_bytes(value) != raw:
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
    return value


def _validate_client(client: object) -> dict[str, Callable[[dict[str, object]], object]]:
    try:
        row = cast(_OkxProfileClient, client)
        api_row = cast(dict[str, object], row.urls["api"])
        if (
            api_row.get("rest") != _REST
            or row.headers.get(_DEMO_HEADER) != "1"
            or type(row.timeout) is not int or not 1 <= row.timeout <= _TIMEOUT_MS
        ):
            raise ValueError
        methods = (
            "privateGetAccountConfig", "publicGetPublicInstruments",
            "privateGetAccountTradeFee", "privateGetAccountPositions",
            "privateGetTradeOrdersPending", "privateGetAccountBalance",
        )
        calls = {name: getattr(client, name) for name in methods}
        if any(not callable(method) for method in calls.values()):
            raise ValueError
        return cast(dict[str, Callable[[dict[str, object]], object]], calls)
    except Exception:
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE") from None


def _read(
    api: dict[str, Callable[[dict[str, object]], object]],
    method: str,
    params: dict[str, object],
    endpoint: str,
    records: list[dict[str, object]],
    wall_time_ms: Callable[[], int],
) -> tuple[object, int] | None:
    try:
        response = api[method](params)
        if type(response) is not dict:
            return None
        raw = canonical_bytes(response)
        if not raw:
            return None
        received_at = wall_time_ms()
        if type(received_at) is not int:
            return None
        records.append({
            "endpoint": endpoint,
            "params": params,
            "received_at_ms": received_at,
            "response": decode_canonical(raw),
        })
        return response, received_at
    except Exception:
        return None


def _read_if_budget(
    api: dict[str, Callable[[dict[str, object]], object]],
    method: str,
    params: dict[str, object],
    endpoint: str,
    records: list[dict[str, object]],
    wall_time_ms: Callable[[], int],
    start: float,
    monotonic: Callable[[], float],
) -> tuple[object, int] | None:
    if not _within_budget(start, monotonic, reserve_seconds=5.0):
        return None
    return _read(api, method, params, endpoint, records, wall_time_ms)


def _within_budget(
    start: float, monotonic: Callable[[], float], *, reserve_seconds: float = 0.0
) -> bool:
    try:
        return 0 <= monotonic() - start < _TOTAL_BUDGET_SECONDS - reserve_seconds
    except Exception:
        return False


def _response_rows(response: object) -> list[dict[str, object]] | None:
    if type(response) is not dict:
        return None
    row = cast(dict[str, object], response)
    data = row.get("data")
    if row.get("code") != "0" or type(data) is not list or any(type(item) is not dict for item in cast(list[object], data)):
        return None
    return cast(list[dict[str, object]], data)


def _selected_families(products: list[dict[str, object]], response: object) -> tuple[str, ...]:
    rows = _response_rows(response)
    if rows is None:
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
    selected_ids = {product.get("instId") for product in products}
    matching = [row for row in rows if row.get("instId") in selected_ids]
    by_id: dict[str, dict[str, object]] = {}
    for row in matching:
        inst_id = row.get("instId")
        if type(inst_id) is not str or inst_id in by_id:
            raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
        by_id[inst_id] = row
    if set(by_id) != selected_ids:
        raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
    families: set[str] = set()
    for row in by_id.values():
        family = row.get("instFamily")
        if row.get("instType") != "SWAP" or type(family) is not str or not family:
            raise OkxSpiderProfileCaptureUnavailable("OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE")
        families.add(family)
    return tuple(sorted(families))


def _build_prerequisite(
    selection_bytes: bytes,
    account: object,
    account_at: object,
    instruments: object,
    instruments_at: object,
) -> OkxSpiderProfilePrerequisite:
    selection = _decode_canonical(selection_bytes)
    selected = cast(dict[str, object], selection)
    scope = cast(dict[str, object], selected["scope"])
    evidence = {
        "environment": "demo",
        "rest_host": "www.okx.com",
        "simulated_trading": True,
        "account_received_at_ms": account_at,
        "instruments_received_at_ms": instruments_at,
        "account": account,
        "instruments": instruments,
    }
    selection_copy = dict(selected)
    selection_copy["scope"] = dict(scope)
    selection_copy["executing_account_ref"] = selected["executing_account_ref"]
    selection_raw = canonical_bytes(selection_copy)
    evidence_raw = canonical_bytes(evidence)
    digest = sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": selection_copy,
        "evidence": evidence,
    })).hexdigest()
    try:
        return create_okx_spider_profile_prerequisite(selection_raw, evidence_raw, digest)
    except Exception:
        raise OkxSpiderProfileCaptureUnavailable(
            "OKX_SPIDER_PROFILE_CAPTURE_UNAVAILABLE"
        ) from None
