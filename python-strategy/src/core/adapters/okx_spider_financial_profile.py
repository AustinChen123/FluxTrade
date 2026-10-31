"""Immutable, replayable OKX financial profile evidence for Spider admission."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import cast

from src.core.adapters.okx_spider_profile import (
    OkxSpiderProfilePrerequisite,
    _canonical_input,
    _envelope,
    _fresh,
    _object,
    create_okx_spider_profile_prerequisite,
)
from src.core.adapters.okx_spider_transport import _decode_batch_request
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

_DOMAIN = "OKX_SPIDER_FINANCIAL_PROFILE_V1"
_MAX_FINANCIAL_AGE_MS = 10_000
_MAX_FEE_AGE_MS = 60_000
_UNAVAILABLE = {"CAPTURE_FAILED", "BUDGET_EXHAUSTED"}
_RECORD_KEYS = {"endpoint", "params", "received_at_ms", "response"}


class OkxSpiderFinancialProfileUnavailable(ValueError):
    """Financial evidence is absent or invalid; no provider details are exposed."""


@dataclass(frozen=True, slots=True)
class OkxSpiderFinancialProjection:
    cross_usdt_equity: Decimal
    usdt_available_equity: Decimal
    fixed_usdt: Decimal
    source_total: Decimal
    usd_adjusted_equity: Decimal
    usd_initial_margin: Decimal
    usd_maintenance_margin: Decimal
    usd_order_frozen: Decimal
    earn_exclusion: Decimal
    currency_equity_liabilities: tuple[tuple[str, Decimal, Decimal], ...]
    provider_positions: tuple[tuple[str, Decimal], ...]
    provider_pending_orders: tuple[tuple[str, str, Decimal, Decimal], ...]
    product_contract_values: tuple[tuple[str, Decimal], ...]
    product_fee_rates: tuple[tuple[str, str, str, Decimal, Decimal], ...]


def _canonical(raw: object) -> object:
    if type(raw) is not bytes:
        raise ValueError("invalid OKX Spider financial profile")
    try:
        value = decode_canonical(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise ValueError("invalid OKX Spider financial profile") from None
    if canonical_bytes(value) != raw:
        raise ValueError("invalid OKX Spider financial profile")
    return value


def _decimal(value: object, *, nonnegative: bool = False) -> Decimal:
    if type(value) is not str:
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise OkxSpiderFinancialProfileUnavailable(
            "OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE"
        ) from None
    if not parsed.is_finite() or nonnegative and parsed < 0:
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    return parsed


def _text(value: object) -> str:
    if type(value) is not str or not value or value.strip() != value:
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    return value


def _record(endpoint: object, params: object, received_at_ms: object, response: object) -> dict[str, object]:
    if (
        type(endpoint) is not str or not endpoint or type(params) is not dict
        or type(received_at_ms) is not int
    ):
        raise ValueError("invalid OKX Spider financial profile")
    return {
        "endpoint": endpoint,
        "params": params,
        "received_at_ms": received_at_ms,
        "response": response,
    }


def _domain_value(value: object) -> dict[str, object]:
    row = _object(value, {
        "domain", "selection", "evidence", "profile_sha256", "fixed_usdt",
        "records", "financial_unavailable",
    })
    if row is None or row.get("domain") != _DOMAIN:
        raise ValueError("invalid OKX Spider financial profile")
    selection = row.get("selection")
    evidence = row.get("evidence")
    if type(selection) is not dict or type(evidence) is not dict:
        raise ValueError("invalid OKX Spider financial profile")
    selection_bytes = canonical_bytes(selection)
    evidence_bytes = canonical_bytes(evidence)
    profile = create_okx_spider_profile_prerequisite(
        selection_bytes, evidence_bytes, cast(str, row.get("profile_sha256"))
    )
    if type(row.get("fixed_usdt")) is not str:
        raise ValueError("invalid OKX Spider financial profile")
    try:
        fixed = Decimal(cast(str, row["fixed_usdt"]))
    except InvalidOperation:
        raise ValueError("invalid OKX Spider financial profile") from None
    if not fixed.is_finite() or fixed < 0:
        raise ValueError("invalid OKX Spider financial profile")
    records = row.get("records")
    if type(records) is not list:
        raise ValueError("invalid OKX Spider financial profile")
    for item in cast(list[object], records):
        record = _object(item, _RECORD_KEYS)
        if record is None or type(record.get("received_at_ms")) is not int:
            raise ValueError("invalid OKX Spider financial profile")
        if type(record.get("endpoint")) is not str or not record["endpoint"] or type(record.get("params")) is not dict:
            raise ValueError("invalid OKX Spider financial profile")
    unavailable = row.get("financial_unavailable")
    if unavailable is not None and (
        type(unavailable) is not str or unavailable not in _UNAVAILABLE
    ):
        raise ValueError("invalid OKX Spider financial profile")
    # Keep the profile construction in this boundary so direct construction
    # validates the same D5A binding as the factory.
    del profile
    return row


@dataclass(frozen=True, slots=True)
class OkxSpiderFinancialProfile:
    """Canonical immutable D5A identity plus raw financial capture evidence."""

    profile_bytes: bytes
    content_sha256: str

    def __post_init__(self) -> None:
        raw = bytes(self.profile_bytes) if type(self.profile_bytes) is bytes else self.profile_bytes
        value = _domain_value(_canonical(raw))
        digest = sha256(canonical_bytes(value)).hexdigest()
        if type(self.content_sha256) is not str or self.content_sha256 != digest:
            raise ValueError("invalid OKX Spider financial profile")
        object.__setattr__(self, "profile_bytes", raw)

    @property
    def _identity_prerequisite(self) -> OkxSpiderProfilePrerequisite:
        value = cast(dict[str, object], _canonical(self.profile_bytes))
        return create_okx_spider_profile_prerequisite(
            canonical_bytes(value["selection"]),
            canonical_bytes(value["evidence"]),
            cast(str, value["profile_sha256"]),
        )

    @property
    def financial_unavailable(self) -> str | None:
        return cast(str | None, cast(dict[str, object], _canonical(self.profile_bytes))["financial_unavailable"])

    def projection(self, now_ms: int) -> OkxSpiderFinancialProjection:
        value = cast(dict[str, object], _canonical(self.profile_bytes))
        if value["financial_unavailable"] is not None:
            raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
        if type(now_ms) is not int:
            raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
        records = cast(list[dict[str, object]], value["records"])
        by_endpoint: dict[str, list[dict[str, object]]] = {}
        for record in records:
            by_endpoint.setdefault(cast(str, record["endpoint"]), []).append(record)
        try:
            balance_records = by_endpoint.get("account/balance", [])
            position_records = by_endpoint.get("account/positions", [])
            if (
                len(balance_records) != 1 or balance_records[0]["params"] != {}
                or len(position_records) != 1 or position_records[0]["params"] != {}
            ):
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
            balance = _one_envelope(by_endpoint, "account/balance")
            positions = _one_envelope(by_endpoint, "account/positions")
            balance_time = cast(int, balance_records[0]["received_at_ms"])
            positions_time = cast(int, position_records[0]["received_at_ms"])
            balance_rows = _rows(balance)
            if len(balance_rows) != 1:
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
            account_row = balance_rows[0]
            _validate_provider_time(account_row)
            details = _object_rows(account_row.get("details"))
            currencies: list[tuple[str, Decimal, Decimal]] = []
            by_currency: dict[str, dict[str, object]] = {}
            for raw_row in details:
                _validate_provider_time(raw_row)
                currency = _text(raw_row.get("ccy"))
                if currency in by_currency:
                    raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                equity = _decimal(raw_row.get("eq"))
                liability = _decimal(raw_row.get("liab"), nonnegative=True)
                by_currency[currency] = raw_row
                currencies.append((currency, equity, liability))
            usdt = by_currency.get("USDT")
            if usdt is None:
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
            equity = _decimal(usdt.get("eq"))
            isolated = _decimal(usdt.get("isoEq"), nonnegative=True)
            available = _decimal(usdt.get("availEq"))
            cross_equity = equity - isolated
            account_equity = _decimal(account_row.get("adjEq"))
            initial_margin = _decimal(account_row.get("imr"), nonnegative=True)
            maintenance_margin = _decimal(account_row.get("mmr"), nonnegative=True)
            order_frozen = _decimal(account_row.get("ordFroz"), nonnegative=True)

            position_rows: list[tuple[str, Decimal]] = []
            for row in _rows(positions):
                _validate_provider_time(row)
                position_rows.append((_text(row.get("instId")), _decimal(row.get("pos"))))

            order_rows: list[tuple[str, str, Decimal, Decimal]] = []
            pending_records = by_endpoint.get("trade/orders-pending", [])
            expected_cursor = ""
            ended = False
            seen_order_ids: set[str] = set()
            if not pending_records:
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
            for index, record in enumerate(pending_records):
                params = cast(dict[str, object], record["params"])
                if params != {"limit": "100", **({"after": expected_cursor} if expected_cursor else {})}:
                    raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                envelope = _checked_envelope(record["response"])
                page = _rows(envelope)
                for row in page:
                    _validate_provider_time(row)
                    order_id = _text(row.get("ordId"))
                    instrument = _text(row.get("instId"))
                    if order_id in seen_order_ids:
                        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                    seen_order_ids.add(order_id)
                    order_rows.append((
                        order_id, instrument, _decimal(row.get("sz")),
                        _decimal(row.get("accFillSz")),
                    ))
                if len(page) < 100:
                    ended = True
                    if index != len(pending_records) - 1:
                        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                    break
                if index == 9:
                    raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                expected_cursor = _text(page[-1].get("ordId"))
            if not ended:
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")

            selection = cast(dict[str, object], value["selection"])
            evidence = cast(dict[str, object], value["evidence"])
            instrument_envelope = _checked_envelope(evidence["instruments"])
            instrument_rows = _rows(instrument_envelope)
            selected_products = cast(list[dict[str, object]], selection["products"])
            instrument_by_id: dict[str, dict[str, object]] = {}
            for instrument in instrument_rows:
                instrument_id = instrument.get("instId")
                if type(instrument_id) is str and instrument_id in {
                    product["instId"] for product in selected_products
                }:
                    if instrument_id in instrument_by_id:
                        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                    instrument_by_id[instrument_id] = instrument
            fee_rates: list[tuple[str, str, str, Decimal, Decimal]] = []
            contract_values: list[tuple[str, Decimal]] = []
            for product in selected_products:
                inst_id = cast(str, product["instId"])
                instrument = instrument_by_id.get(inst_id)
                if instrument is None or instrument.get("instType") != "SWAP":
                    raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                family = _text(instrument.get("instFamily"))
                group_id = _text(instrument.get("groupId"))
                contract_values.append((
                    cast(str, product["product_id"]),
                    _decimal(product["ctVal"]) * _decimal(product["ctMult"]),
                ))
                matches: list[dict[str, object]] = []
                for fee_record in by_endpoint.get("account/trade-fee", []):
                    params = cast(dict[str, object], fee_record["params"])
                    if params == {"instType": "SWAP", "instFamily": family}:
                        fee_envelope = _checked_envelope(fee_record["response"])
                        for fee_row in _rows(fee_envelope):
                            _validate_provider_time(fee_row)
                            if fee_row.get("instType") != "SWAP":
                                continue
                            fee_groups = fee_row.get("feeGroup")
                            if type(fee_groups) is list and all(
                                type(item) is dict for item in cast(list[object], fee_groups)
                            ):
                                matches.extend(
                                    cast(dict[str, object], item)
                                    for item in cast(list[object], fee_groups)
                                    if cast(dict[str, object], item).get("groupId") == group_id
                                )
                        if not _fresh_age(fee_record["received_at_ms"], now_ms, _MAX_FEE_AGE_MS):
                            raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                if len(matches) != 1:
                    raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
                fee = matches[0]
                fee_rates.append((
                    cast(str, product["product_id"]), group_id, family,
                    _decimal(fee.get("maker")), _decimal(fee.get("taker")),
                ))

            if not _fresh(balance_time, now_ms) or not _fresh(positions_time, now_ms):
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
            if any(not _fresh(cast(int, row["received_at_ms"]), now_ms) for row in pending_records):
                raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
            fixed = _decimal(value["fixed_usdt"], nonnegative=True)
            return OkxSpiderFinancialProjection(
                cross_usdt_equity=cross_equity,
                usdt_available_equity=available,
                fixed_usdt=fixed,
                source_total=cross_equity - fixed,
                usd_adjusted_equity=account_equity,
                usd_initial_margin=initial_margin,
                usd_maintenance_margin=maintenance_margin,
                usd_order_frozen=order_frozen,
                earn_exclusion=Decimal(0),
                currency_equity_liabilities=tuple(currencies),
                provider_positions=tuple(position_rows),
                provider_pending_orders=tuple(order_rows),
                product_contract_values=tuple(contract_values),
                product_fee_rates=tuple(fee_rates),
            )
        except OkxSpiderFinancialProfileUnavailable:
            raise
        except Exception:
            raise OkxSpiderFinancialProfileUnavailable(
                "OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE"
            ) from None

    def __call__(self, request_payload: bytes, now_ms: int) -> bool:
        prerequisite = self._identity_prerequisite
        if not prerequisite(request_payload, now_ms):
            return False
        request = _decode_batch_request(request_payload)
        if request is None:
            return False
        operation = request.get("operation")
        if operation == "CANCEL":
            return True
        if operation != "PLACE":
            return False
        members = cast(list[dict[str, object]], request["members"])
        if members and all(
            cast(dict[str, object], member["request_payload"]).get("reduceOnly") is True
            for member in members
        ):
            return True
        try:
            projection = self.projection(now_ms)
        except Exception:
            return False
        return (
            projection.cross_usdt_equity > 0
            and projection.source_total > 0
            and projection.usdt_available_equity > 0
            and projection.usd_adjusted_equity > projection.usd_initial_margin
            and projection.usd_adjusted_equity > projection.usd_maintenance_margin
        )

def _checked_envelope(value: object) -> dict[str, object]:
    envelope = _envelope(value)
    if envelope is None:
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    return envelope


def _fresh_age(received_at: object, now_ms: int, max_age_ms: int) -> bool:
    return type(received_at) is int and 0 <= now_ms - received_at <= max_age_ms


def _validate_provider_time(row: dict[str, object]) -> None:
    for key in ("ts", "uTime"):
        value = row.get(key)
        if value is not None and (type(value) is not str or not value.isdigit()):
            raise OkxSpiderFinancialProfileUnavailable(
                "OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE"
            )


def _rows(envelope: dict[str, object]) -> list[dict[str, object]]:
    raw = envelope.get("data")
    if type(raw) is not list or any(type(row) is not dict for row in cast(list[object], raw)):
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    return cast(list[dict[str, object]], raw)


def _one_envelope(
    by_endpoint: dict[str, list[dict[str, object]]], endpoint: str
) -> dict[str, object]:
    records = by_endpoint.get(endpoint, [])
    if len(records) != 1:
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    return _checked_envelope(records[0]["response"])


def _object_rows(value: object) -> list[dict[str, object]]:
    if type(value) is not list or any(type(row) is not dict for row in cast(list[object], value)):
        raise OkxSpiderFinancialProfileUnavailable("OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE")
    return cast(list[dict[str, object]], value)


def create_okx_spider_financial_profile(
    prerequisite: OkxSpiderProfilePrerequisite,
    fixed_usdt: Decimal,
    records: tuple[dict[str, object], ...],
    financial_unavailable: str | None,
) -> OkxSpiderFinancialProfile:
    if type(prerequisite) is not OkxSpiderProfilePrerequisite:
        raise ValueError("invalid OKX Spider financial profile")
    if not isinstance(fixed_usdt, Decimal) or not fixed_usdt.is_finite() or fixed_usdt < 0:
        raise ValueError("invalid OKX Spider financial profile")
    if type(records) is not tuple or financial_unavailable not in (None, *_UNAVAILABLE):
        raise ValueError("invalid OKX Spider financial profile")
    selection = _canonical_input(prerequisite.selection_bytes)
    evidence = _canonical_input(prerequisite.evidence_bytes)
    domain = {
        "domain": _DOMAIN,
        "selection": selection,
        "evidence": evidence,
        "profile_sha256": prerequisite.content_sha256,
        "fixed_usdt": format(fixed_usdt, "f"),
        "records": [_record(**record) for record in records],
        "financial_unavailable": financial_unavailable,
    }
    raw = canonical_bytes(domain)
    return OkxSpiderFinancialProfile(raw, sha256(raw).hexdigest())
