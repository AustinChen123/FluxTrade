"""Pure OKX Demo profile prerequisite for Spider batch transmission."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import cast

from src.core.adapters.okx_spider_transport import _decode_batch_request
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

_DOMAIN = "OKX_SPIDER_PROFILE_PREREQUISITE_V1"
_MAX_AGE_MS = 10_000
_SELECTION_KEYS = {"scope", "executing_account_ref", "expected_uid", "products"}
_PRODUCT_KEYS = {
    "product_id", "instId", "base_ccy", "ctVal", "ctMult", "lotSz", "minSz", "tickSz"
}
_EVIDENCE_KEYS = {
    "environment", "rest_host", "simulated_trading", "account_received_at_ms",
    "instruments_received_at_ms", "account", "instruments",
}
_SCOPE_KEYS = {"environment", "execution_scope_id", "strategy_id"}
_SPEC_FIELDS = ("ctVal", "ctMult", "lotSz", "minSz", "tickSz")


def _text(value: object) -> str | None:
    return value if type(value) is str and value and value.strip() == value else None


def _decimal(value: object) -> Decimal | None:
    if type(value) is not str:
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _object(value: object, keys: set[str]) -> dict[str, object] | None:
    return cast(dict[str, object], value) if type(value) is dict and set(cast(dict[str, object], value)) == keys else None


def _envelope(value: object) -> dict[str, object] | None:
    if type(value) is not dict:
        return None
    row = cast(dict[str, object], value)
    return row if row.get("code") == "0" and type(row.get("data")) is list else None


def _canonical_input(raw: object) -> object:
    if type(raw) is not bytes:
        raise ValueError("invalid OKX Spider profile prerequisite")
    try:
        value = decode_canonical(raw)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise ValueError("invalid OKX Spider profile prerequisite") from None
    if canonical_bytes(value) != raw:
        raise ValueError("invalid OKX Spider profile prerequisite")
    return value


def _validate_selection(value: object) -> None:
    selection = _object(value, _SELECTION_KEYS)
    if selection is None:
        raise ValueError("invalid OKX Spider profile prerequisite")
    scope = _object(selection.get("scope"), _SCOPE_KEYS)
    if scope is None or scope.get("environment") != "demo" or any(
        _text(scope.get(key)) is None for key in _SCOPE_KEYS
    ):
        raise ValueError("invalid OKX Spider profile prerequisite")
    if _text(selection.get("executing_account_ref")) is None or _text(selection.get("expected_uid")) is None:
        raise ValueError("invalid OKX Spider profile prerequisite")
    products = selection.get("products")
    if type(products) is not list or not cast(list[object], products):
        raise ValueError("invalid OKX Spider profile prerequisite")
    product_ids: set[str] = set()
    inst_ids: set[str] = set()
    for raw_product in cast(list[object], products):
        product = _object(raw_product, _PRODUCT_KEYS)
        if product is None:
            raise ValueError("invalid OKX Spider profile prerequisite")
        product_id = _text(product.get("product_id"))
        inst_id = _text(product.get("instId"))
        base_ccy = _text(product.get("base_ccy"))
        if (
            product_id is None or inst_id is None or base_ccy is None
            or inst_id != f"{base_ccy}-USDT-SWAP"
            or product_id in product_ids or inst_id in inst_ids
            or any(_decimal(product.get(field)) is None for field in _SPEC_FIELDS)
        ):
            raise ValueError("invalid OKX Spider profile prerequisite")
        product_ids.add(product_id)
        inst_ids.add(inst_id)


def _validate_common_evidence(value: object, expected_uid: str) -> None:
    evidence = _object(value, _EVIDENCE_KEYS)
    if evidence is None or (
        evidence.get("environment") != "demo"
        or evidence.get("rest_host") != "www.okx.com"
        or evidence.get("simulated_trading") is not True
        or type(evidence.get("account_received_at_ms")) is not int
    ):
        raise ValueError("invalid OKX Spider profile prerequisite")
    account = _envelope(evidence.get("account"))
    if account is None:
        raise ValueError("invalid OKX Spider profile prerequisite")
    rows = cast(list[object], account["data"])
    if len(rows) != 1 or type(rows[0]) is not dict or cast(dict[str, object], rows[0]).get("uid") != expected_uid:
        raise ValueError("invalid OKX Spider profile prerequisite")


@dataclass(frozen=True, slots=True)
class OkxSpiderProfilePrerequisite:
    """Immutable selection/evidence binding callable at the pre-marker seam."""

    selection_bytes: bytes
    evidence_bytes: bytes
    content_sha256: str

    def __post_init__(self) -> None:
        selection = _canonical_input(self.selection_bytes)
        evidence = _canonical_input(self.evidence_bytes)
        _validate_selection(selection)
        expected_uid = cast(str, cast(dict[str, object], selection)["expected_uid"])
        _validate_common_evidence(evidence, expected_uid)
        digest = sha256(canonical_bytes({
            "domain": _DOMAIN,
            "selection": selection,
            "evidence": evidence,
        })).hexdigest()
        if type(self.content_sha256) is not str or self.content_sha256 != digest:
            raise ValueError("invalid OKX Spider profile prerequisite")

    def __call__(self, request_payload: bytes, now_ms: int) -> bool:
        if type(now_ms) is not int:
            return False
        try:
            selection = _canonical_input(self.selection_bytes)
            evidence = _canonical_input(self.evidence_bytes)
            selected = cast(dict[str, object], selection)
            scope = cast(dict[str, object], selected["scope"])
            selected_evidence = cast(dict[str, object], evidence)
            uid = cast(str, selected["expected_uid"])
            _validate_common_evidence(selected_evidence, uid)
            request = _decode_batch_request(request_payload)
            if request is None:
                return False
            if (
                request.get("scope") != scope
                or request.get("executing_account_ref") != selected["executing_account_ref"]
            ):
                return False
            received_at = selected_evidence["account_received_at_ms"]
            if not _fresh(received_at, now_ms):
                return False
            operation = request.get("operation")
            if operation == "CANCEL":
                return _cancel_members(request, cast(list[object], selected["products"]))
            if operation != "PLACE":
                return False
            if not _place_account(selected_evidence, uid):
                return False
            instrument_received_at = selected_evidence["instruments_received_at_ms"]
            instruments = _envelope(selected_evidence.get("instruments"))
            if not _fresh(instrument_received_at, now_ms) or instruments is None:
                return False
            return _place_instruments(
                instruments, request, cast(list[object], selected["products"])
            )
        except Exception:
            return False


def _fresh(received_at: object, now_ms: int) -> bool:
    return (
        type(received_at) is int
        and 0 <= now_ms - received_at <= _MAX_AGE_MS
    )


def _place_account(evidence: dict[str, object], expected_uid: str) -> bool:
    account = _envelope(evidence.get("account"))
    if account is None:
        return False
    rows = cast(list[object], account["data"])
    if len(rows) != 1 or type(rows[0]) is not dict:
        return False
    row = cast(dict[str, object], rows[0])
    return (
        row.get("uid") == expected_uid
        and row.get("acctLv") == "3"
        and row.get("posMode") == "net_mode"
        and row.get("autoLoan") is False
    )


def _cancel_members(request: dict[str, object], products: list[object]) -> bool:
    by_product = {
        cast(str, cast(dict[str, object], product)["product_id"]):
        cast(str, cast(dict[str, object], product)["instId"])
        for product in products
    }
    for raw_member in cast(list[object], request["members"]):
        member = cast(dict[str, object], raw_member)
        inst_id = by_product.get(cast(str, member["product_id"]))
        wire = cast(dict[str, object], member["request_payload"])
        if inst_id is None or wire.get("instId") != inst_id:
            return False
    return True


def _place_instruments(
    envelope: dict[str, object], request: dict[str, object], products: list[object]
) -> bool:
    selected = {
        cast(str, cast(dict[str, object], product)["instId"]): cast(dict[str, object], product)
        for product in products
    }
    rows_by_id: dict[str, dict[str, object]] = {}
    for raw in cast(list[object], envelope["data"]):
        if type(raw) is not dict:
            continue
        row = cast(dict[str, object], raw)
        inst_id = row.get("instId")
        if inst_id in selected:
            if cast(str, inst_id) in rows_by_id:
                return False
            rows_by_id[cast(str, inst_id)] = row
    if set(rows_by_id) != set(selected):
        return False
    for inst_id, product in selected.items():
        row = rows_by_id[inst_id]
        if (
            row.get("instType") != "SWAP"
            or row.get("ctType") != "linear"
            or row.get("settleCcy") != "USDT"
            or row.get("ctValCcy") != product.get("base_ccy")
            or row.get("state") != "live"
            or any(
                _decimal(row.get(field)) is None
                or _decimal(row.get(field)) != _decimal(product.get(field))
                for field in _SPEC_FIELDS
            )
        ):
            return False
    by_product = {
        cast(str, cast(dict[str, object], product)["product_id"]):
        cast(str, cast(dict[str, object], product)["instId"])
        for product in products
    }
    for raw_member in cast(list[object], request["members"]):
        member = cast(dict[str, object], raw_member)
        product_id = cast(str, member["product_id"])
        inst_id = by_product.get(product_id)
        wire = cast(dict[str, object], member["request_payload"])
        if inst_id is None or wire.get("instId") != inst_id:
            return False
        if wire.get("tdMode") != "cross" or (
            "posSide" in wire and wire.get("posSide") != "net"
        ):
            return False
        order_type = wire.get("ordType")
        if order_type not in {"limit", "market"}:
            return False
        size = _decimal(wire.get("sz"))
        product = cast(dict[str, object], next(p for p in products if cast(dict[str, object], p)["product_id"] == product_id))
        lot = cast(Decimal, _decimal(product["lotSz"]))
        minimum = cast(Decimal, _decimal(product["minSz"]))
        if size is None or size < minimum or size % lot != 0:
            return False
        if order_type == "limit":
            price = _decimal(wire.get("px"))
            tick = cast(Decimal, _decimal(product["tickSz"]))
            if price is None or price % tick != 0:
                return False
    return True


def create_okx_spider_profile_prerequisite(
    selection_bytes: bytes, evidence_bytes: bytes, expected_sha256: str
) -> OkxSpiderProfilePrerequisite:
    """Bind canonical trusted selection and provider evidence immutably."""
    selection = _canonical_input(selection_bytes)
    evidence = _canonical_input(evidence_bytes)
    digest = sha256(canonical_bytes({
        "domain": _DOMAIN,
        "selection": selection,
        "evidence": evidence,
    })).hexdigest()
    if type(expected_sha256) is not str or expected_sha256 != digest:
        raise ValueError("invalid OKX Spider profile prerequisite")
    return OkxSpiderProfilePrerequisite(selection_bytes, evidence_bytes, digest)
