"""Strict v1 Rust-produced daily evidence; no aggregation, I/O or DB publication.

Empty hours do not interrupt aggregate-ID continuity. Kline constituent trade
count is diagnostic, never compared to the number of aggregate-trade records.
Parsing validates wire structure/internal consistency only, not source
completeness. The Rust assembler must read back and verify official responses
and manifests before producing this evidence.
"""
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from src.core.decimal_math import canonical_decimal_text

from .jobs import JobSpec
from .publication import MAX_JSON_BYTES, CanonicalJsonObject, VerifiedProfilePublication
from .types import BIGINT_MAX, ProfileBin, VolumeProfileContent, _decimal

_HOUR = 3_600_000
_U64_MAX = (1 << 64) - 1
MAX_FRAMED_HANDOFF_BYTES = MAX_JSON_BYTES + 1  # Canonical JSON plus the CLI's single LF.
_PRODUCT = "BINANCE:BTCUSDT-SPOT"
_TOP = "schema_version job_id config_sha256 content content_sha256 hours reconciliation source_available_at_ms availability_basis raw_retention_state"
_CONTENT = "schema_version product_id window_start_ms window_end_ms period timezone grid_id bin_origin bin_step algorithm_version bins"
_HOUR_KEYS = "start_ms end_ms manifest_sha256 page_count first_aggregate_id last_aggregate_id aggregate_count"
_RECON = "source product_id window_start_ms window_end_ms interval response_sha256 expected_base_volume expected_quote_volume actual_base_volume actual_quote_volume actual_aggregate_trade_count official_constituent_trade_count result"


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("invalid profile handoff")


def _keys(value: Any, keys: str) -> dict[str, Any]:
    _require(type(value) is dict and set(value) == set(keys.split()))
    return value


def _int(value: Any, maximum: int = BIGINT_MAX, minimum: int = 0) -> int:
    _require(type(value) is int and minimum <= value <= maximum)
    return value


def _hex(value: Any) -> None:
    _require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None)


def _dec(value: Any) -> Decimal:
    _require(type(value) is str)
    number = _decimal(Decimal(value))
    _require(canonical_decimal_text(number) == value)
    return number


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _reject_number(_: str) -> None:
    raise ValueError("invalid profile handoff")


def _validate_publication_binding(spec: JobSpec, publication: VerifiedProfilePublication,
                                  *, allow_deleted: bool = False) -> None:
    """Validate persisted evidence consistency, not provenance or historical JSON keys."""
    try:
        _require(type(spec) is JobSpec and type(publication) is VerifiedProfilePublication)
        content = publication.content
        _require(all(getattr(spec, key) == getattr(content, key) for key in
                     ("product_id", "window_start_ms", "window_end_ms", "grid_id", "algorithm_version")))
        _require(content.product_id == _PRODUCT and content.algorithm_version == "vp-v1"
                 and content.grid_id == "btc_spot_usdt_10_v1" and content.bin_origin == 0 and content.bin_step == 10)
        manifest = _keys(publication.source_manifest.thaw(), "schema_version job_id config_sha256 hours")
        _require(_int(manifest["schema_version"]) == 1 and manifest["job_id"] == spec.id
                 and manifest["config_sha256"] == spec.config_sha256)
        hours = manifest["hours"]
        _require(type(hours) is list and len(hours) == 24)
        count, previous = 0, None
        for index, hour in enumerate(hours):
            hour = _keys(hour, _HOUR_KEYS)
            start = content.window_start_ms + index * _HOUR
            _require(_int(hour["start_ms"]) == start and _int(hour["end_ms"]) == start + _HOUR)
            _hex(hour["manifest_sha256"])
            _int(hour["page_count"], _U64_MAX, 1)
            accepted = _int(hour["aggregate_count"])
            first, last = hour["first_aggregate_id"], hour["last_aggregate_id"]
            if accepted == 0:
                _require(first is None and last is None)
            else:
                first, last = _int(first, _U64_MAX), _int(last, _U64_MAX)
                _require(first <= last and accepted == last - first + 1)
                _require(previous is None or first == previous + 1)
                previous = last
            count += accepted
        _require(count == content.aggregate_count)
        recon = _keys(publication.reconciliation.thaw(), _RECON)
        _require(recon["source"] == "BINANCE_SPOT_1D_KLINE" and recon["interval"] == "1d" and recon["result"] == "EXACT")
        _require(recon["product_id"] == content.product_id
                 and _int(recon["window_start_ms"]) == content.window_start_ms
                 and _int(recon["window_end_ms"]) == content.window_end_ms)
        _hex(recon["response_sha256"])
        for name in ("base_volume", "quote_volume"):
            _require(_dec(recon["expected_" + name]) == _dec(recon["actual_" + name]) == getattr(content, name))
        _require(_int(recon["actual_aggregate_trade_count"]) == count)
        official = _int(recon["official_constituent_trade_count"], _U64_MAX)
        _require((count == 0) == (official == 0))
        available = publication.source_available_at
        _require(type(available) is datetime and available.tzinfo is timezone.utc
                 and available.microsecond % 1000 == 0
                 and datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=content.window_end_ms)
                 <= available <= datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc))
        _require(publication.availability_basis == "OBSERVED"
                 and publication.raw_retention_state in (("PRESENT", "DELETED") if allow_deleted else ("PRESENT",)))
    except (ValueError, TypeError, KeyError, ArithmeticError, RecursionError):
        raise ValueError("invalid profile handoff") from None


@dataclass(frozen=True, slots=True)
class ParsedHandoff:
    spec: JobSpec
    publication: VerifiedProfilePublication

    def __post_init__(self) -> None:
        _require(type(self.spec) is JobSpec and type(self.publication) is VerifiedProfilePublication)
        content = self.publication.content
        _require(all(getattr(self.spec, key) == getattr(content, key) for key in
                     ("product_id", "window_start_ms", "window_end_ms", "grid_id", "algorithm_version")))


def encode_handoff(spec: JobSpec, publication: VerifiedProfilePublication) -> bytes:
    """Encode bound PRESENT DB evidence; never normalize assembler stdout for comparison."""
    _validate_publication_binding(spec, publication)
    available = publication.source_available_at
    assert available is not None  # Binding validator requires an exact UTC integer millisecond.
    elapsed = available - datetime(1970, 1, 1, tzinfo=timezone.utc)
    wire = {
        "schema_version": 1, "job_id": spec.id, "config_sha256": spec.config_sha256,
        "content": json.loads(publication.content.content_bytes),
        "content_sha256": publication.content_sha256,
        "hours": publication.source_manifest.thaw()["hours"],
        "reconciliation": publication.reconciliation.thaw(),
        "source_available_at_ms": elapsed.days * 86400000 + elapsed.seconds * 1000 + elapsed.microseconds // 1000,
        "availability_basis": publication.availability_basis, "raw_retention_state": publication.raw_retention_state,
    }
    # Same bounded UTF-8/canonical JSON domain as the wire parser.
    encoded = CanonicalJsonObject(wire).text.encode("utf-8")
    return encoded + b"\n"


def parse_handoff(raw: bytes) -> ParsedHandoff:
    """Validate bounded wire consistency, never attest source completeness."""
    _require(type(raw) is bytes and len(raw) <= MAX_FRAMED_HANDOFF_BYTES)
    try:
        wire = _keys(json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                               parse_float=_reject_number, parse_constant=_reject_number), _TOP)
        CanonicalJsonObject(wire)  # Shared UTF-8/NUL, depth, node and integer bounds.
        _require(_int(wire["schema_version"]) == 1)
        payload = _keys(wire["content"], _CONTENT)
        _require(_int(payload["schema_version"]) == 1 and payload["period"] == "1d" and payload["timezone"] == "UTC")
        _require(type(payload["bins"]) is list)
        bins = []
        for item in payload["bins"]:
            item = _keys(item, "bin_index base_volume quote_volume aggregate_count")
            bins.append(ProfileBin(_int(item["bin_index"], minimum=-(1 << 63)), _dec(item["base_volume"]),
                                   _dec(item["quote_volume"]), _int(item["aggregate_count"], minimum=1)))
        content = VolumeProfileContent(payload["product_id"], payload["window_start_ms"], payload["window_end_ms"],
                                       payload["grid_id"], _dec(payload["bin_origin"]), _dec(payload["bin_step"]),
                                       payload["algorithm_version"], tuple(bins))
        _hex(wire["content_sha256"])
        _require(content.content_sha256 == wire["content_sha256"])
        spec = JobSpec(wire["job_id"], content.product_id, content.window_start_ms, content.window_end_ms,
                       content.grid_id, content.algorithm_version, wire["config_sha256"])
        hours = wire["hours"]
        recon = wire["reconciliation"]
        available = _int(wire["source_available_at_ms"], 253402300799999, content.window_end_ms)
        _require(wire["availability_basis"] == "OBSERVED" and wire["raw_retention_state"] == "PRESENT")
        manifest = CanonicalJsonObject({"schema_version": 1, "job_id": spec.id,
                                       "config_sha256": spec.config_sha256, "hours": hours})
        publication = VerifiedProfilePublication(content, manifest, CanonicalJsonObject(recon),
            datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=available), "OBSERVED", "PRESENT")
        _validate_publication_binding(spec, publication)
        return ParsedHandoff(spec, publication)
    except (ValueError, TypeError, KeyError, ArithmeticError, RecursionError):
        raise ValueError("invalid profile handoff") from None
