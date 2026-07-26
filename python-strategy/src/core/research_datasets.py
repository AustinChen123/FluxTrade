from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Generator, Literal, Mapping, Protocol, Sequence, TypedDict

from sqlalchemy import func, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.core.db import SessionLocal
from src.core.orm_models import ResearchCandlestick, ResearchDataset
from src.core.product_registry import validate_product_id


_COLUMN_ALIASES = {
    "timestamp": ("timestamp", "time", "ts", "date", "datetime"),
    "open": ("open", "o"),
    "high": ("high", "h"),
    "low": ("low", "l"),
    "close": ("close", "c", "adj close"),
    "volume": ("volume", "vol", "v"),
}
_REQUIRED_COLUMNS = tuple(_COLUMN_ALIASES)
_SOURCE_CONTRACT_ALIASES = (
    "source_contract",
    "frontmonthcontract",
    "front_month_contract",
    "contract",
)
_INSERT_BATCH_SIZE = 5_000
_MAX_BIGINT = 9_223_372_036_854_775_807
TimestampFormat = Literal["epoch_milliseconds", "epoch_seconds", "iso8601"]


class ResearchDatasetConflictError(ValueError):
    """Raised when an existing immutable dataset ID has different content."""


class ResearchCsvValidationError(ValueError):
    """Raised when a research CSV cannot be imported without ambiguity."""


class _Digest(Protocol):
    def update(self, data: bytes, /) -> None: ...


class _ResearchCandleRow(TypedDict):
    timestamp: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    source_contract: str | None


@dataclass(frozen=True)
class ResearchDatasetSpec:
    dataset_id: str
    product_id: str
    timeframe: str
    source: str
    revision: str
    timestamp_format: TimestampFormat = "epoch_milliseconds"
    roll_policy: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        limits = {
            "dataset_id": 128,
            "product_id": 255,
            "timeframe": 32,
            "source": 128,
            "revision": 128,
        }
        for name, limit in limits.items():
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
            if len(value) > limit:
                raise ValueError(f"{name} must not exceed {limit} characters")
        if self.roll_policy is not None:
            if not self.roll_policy.strip():
                raise ValueError("roll_policy must be non-empty when provided")
            if len(self.roll_policy) > 128:
                raise ValueError("roll_policy must not exceed 128 characters")
        if self.timestamp_format not in (
            "epoch_milliseconds",
            "epoch_seconds",
            "iso8601",
        ):
            raise ValueError("unsupported timestamp_format")
        validate_product_id(self.product_id)


@dataclass(frozen=True)
class ResearchDatasetImportResult:
    dataset_id: str
    row_count: int
    checksum_sha256: str
    already_present: bool


@dataclass(frozen=True)
class _CsvSummary:
    row_count: int
    start_time: int
    end_time: int
    checksum_sha256: str


def _parse_timestamp(
    value: str | None,
    line_number: int,
    timestamp_format: TimestampFormat,
) -> int:
    raw = value.strip() if value is not None else ""
    if not raw:
        raise ResearchCsvValidationError(
            f"line {line_number}: timestamp is empty"
        )

    if timestamp_format != "iso8601":
        try:
            numeric = Decimal(raw)
        except InvalidOperation as exc:
            raise ResearchCsvValidationError(
                f"line {line_number}: invalid numeric timestamp"
            ) from exc
        if not numeric.is_finite():
            raise ResearchCsvValidationError(
                f"line {line_number}: timestamp must be finite"
            )
        milliseconds = (
            numeric * 1000
            if timestamp_format == "epoch_seconds"
            else numeric
        )
        integral = milliseconds.to_integral_value()
        if milliseconds != integral:
            raise ResearchCsvValidationError(
                f"line {line_number}: timestamp must resolve to whole milliseconds"
            )
        timestamp = int(integral)
        if timestamp < 0 or timestamp > _MAX_BIGINT:
            raise ResearchCsvValidationError(
                f"line {line_number}: timestamp is outside the supported range"
            )
        return timestamp

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResearchCsvValidationError(
            f"line {line_number}: invalid timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResearchCsvValidationError(
            f"line {line_number}: ISO timestamp must include a UTC offset"
        )
    parsed = parsed.astimezone(timezone.utc)
    if parsed.microsecond % 1000:
        raise ResearchCsvValidationError(
            f"line {line_number}: timestamp precision finer than milliseconds"
        )
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    timestamp = (
        delta.days * 86_400_000
        + delta.seconds * 1000
        + delta.microseconds // 1000
    )
    if timestamp < 0 or timestamp > _MAX_BIGINT:
        raise ResearchCsvValidationError(
            f"line {line_number}: timestamp is outside the supported range"
        )
    return timestamp


def _parse_decimal(value: str, column: str, line_number: int) -> Decimal:
    try:
        parsed = Decimal(value.strip())
    except (InvalidOperation, AttributeError) as exc:
        raise ResearchCsvValidationError(
            f"line {line_number}: invalid {column}"
        ) from exc
    if not parsed.is_finite():
        raise ResearchCsvValidationError(
            f"line {line_number}: {column} must be finite"
        )
    return parsed


def _canonical_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    canonical = format(value, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    return canonical


def _resolve_columns(fieldnames: Sequence[str] | None) -> dict[str, str]:
    if not fieldnames:
        raise ResearchCsvValidationError("CSV must include a header row")
    normalized = {name.strip().lower(): name for name in fieldnames}
    resolved: dict[str, str] = {}
    for standard, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                resolved[standard] = normalized[alias]
                break
    missing = set(_REQUIRED_COLUMNS) - set(resolved)
    if missing:
        columns = ", ".join(sorted(missing))
        raise ResearchCsvValidationError(
            f"CSV missing required columns: {columns}"
        )
    return resolved


def _resolve_source_contract_column(
    fieldnames: Sequence[str],
) -> str | None:
    normalized = {name.strip().lower(): name for name in fieldnames}
    for alias in _SOURCE_CONTRACT_ALIASES:
        if alias in normalized:
            return normalized[alias]
    return None


def _iter_csv_rows(
    path: Path,
    timestamp_format: TimestampFormat,
) -> Generator[_ResearchCandleRow, None, None]:
    previous_timestamp: int | None = None
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = _resolve_columns(reader.fieldnames)
        assert reader.fieldnames is not None
        source_contract_column = _resolve_source_contract_column(reader.fieldnames)
        for line_number, row in enumerate(reader, start=2):
            timestamp = _parse_timestamp(
                row[columns["timestamp"]],
                line_number,
                timestamp_format,
            )
            open_price = _parse_decimal(row[columns["open"]], "open", line_number)
            high = _parse_decimal(row[columns["high"]], "high", line_number)
            low = _parse_decimal(row[columns["low"]], "low", line_number)
            close = _parse_decimal(row[columns["close"]], "close", line_number)
            volume = _parse_decimal(row[columns["volume"]], "volume", line_number)
            source_contract = (
                row[source_contract_column].strip()
                if source_contract_column is not None
                and row[source_contract_column] is not None
                else ""
            )
            if len(source_contract) > 64:
                raise ResearchCsvValidationError(
                    f"line {line_number}: source_contract must not exceed 64 characters"
                )

            if previous_timestamp is not None and timestamp <= previous_timestamp:
                raise ResearchCsvValidationError(
                    f"line {line_number}: timestamps must be strictly increasing"
                )
            if high < max(open_price, low, close):
                raise ResearchCsvValidationError(
                    f"line {line_number}: high is below an OHLC value"
                )
            if low > min(open_price, high, close):
                raise ResearchCsvValidationError(
                    f"line {line_number}: low is above an OHLC value"
                )
            if volume < 0:
                raise ResearchCsvValidationError(
                    f"line {line_number}: volume must not be negative"
                )

            previous_timestamp = timestamp
            yield {
                "timestamp": timestamp,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "source_contract": source_contract or None,
            }


def _update_digest(
    digest: _Digest,
    row: _ResearchCandleRow,
) -> None:
    values = [
        str(row["timestamp"]),
        _canonical_decimal(row["open"]),
        _canonical_decimal(row["high"]),
        _canonical_decimal(row["low"]),
        _canonical_decimal(row["close"]),
        _canonical_decimal(row["volume"]),
        row["source_contract"] or "",
    ]
    digest.update(
        (
            json.dumps(values, ensure_ascii=True, separators=(",", ":"))
            + "\n"
        ).encode("ascii")
    )


def _scan_csv(path: Path, timestamp_format: TimestampFormat) -> _CsvSummary:
    digest = hashlib.sha256()
    row_count = 0
    start_time: int | None = None
    end_time: int | None = None
    for row in _iter_csv_rows(path, timestamp_format):
        timestamp = int(row["timestamp"])
        if start_time is None:
            start_time = timestamp
        end_time = timestamp
        row_count += 1
        _update_digest(digest, row)
    if row_count == 0 or start_time is None or end_time is None:
        raise ResearchCsvValidationError("CSV must contain at least one data row")
    return _CsvSummary(
        row_count=row_count,
        start_time=start_time,
        end_time=end_time,
        checksum_sha256=digest.hexdigest(),
    )


class ResearchDatasetImporter:
    """Transactionally import immutable research candles from a CSV file."""

    def __init__(self, session_factory=None, batch_size: int = _INSERT_BATCH_SIZE):
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        self._session_factory = session_factory or SessionLocal
        self._batch_size = batch_size

    def import_csv(
        self,
        file_path: str | Path,
        spec: ResearchDatasetSpec,
    ) -> ResearchDatasetImportResult:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError("research CSV does not exist")
        metadata_json = json.dumps(
            dict(spec.metadata),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        summary = _scan_csv(path, spec.timestamp_format)
        session: Session = self._session_factory()
        try:
            existing = session.get(ResearchDataset, spec.dataset_id)
            if existing is not None:
                self._verify_existing(
                    session,
                    existing,
                    spec,
                    summary,
                    metadata_json,
                )
                return ResearchDatasetImportResult(
                    dataset_id=spec.dataset_id,
                    row_count=summary.row_count,
                    checksum_sha256=summary.checksum_sha256,
                    already_present=True,
                )

            session.add(
                ResearchDataset(
                    id=spec.dataset_id,
                    product_id=spec.product_id,
                    timeframe=spec.timeframe,
                    source=spec.source,
                    revision=spec.revision,
                    timestamp_format=spec.timestamp_format,
                    checksum_sha256=summary.checksum_sha256,
                    roll_policy=spec.roll_policy,
                    start_time=summary.start_time,
                    end_time=summary.end_time,
                    row_count=summary.row_count,
                    quality_status="validated",
                    metadata_json=metadata_json,
                )
            )
            session.flush()
            second_summary = self._insert_rows(
                session,
                path,
                spec.dataset_id,
                spec.timestamp_format,
            )
            if second_summary != summary:
                raise ResearchCsvValidationError(
                    "research CSV changed while it was being imported"
                )
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = session.get(ResearchDataset, spec.dataset_id)
            if existing is None:
                raise
            self._verify_existing(
                session,
                existing,
                spec,
                summary,
                metadata_json,
            )
            return ResearchDatasetImportResult(
                dataset_id=spec.dataset_id,
                row_count=summary.row_count,
                checksum_sha256=summary.checksum_sha256,
                already_present=True,
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return ResearchDatasetImportResult(
            dataset_id=spec.dataset_id,
            row_count=summary.row_count,
            checksum_sha256=summary.checksum_sha256,
            already_present=False,
        )

    def _insert_rows(
        self,
        session: Session,
        path: Path,
        dataset_id: str,
        timestamp_format: TimestampFormat,
    ) -> _CsvSummary:
        digest = hashlib.sha256()
        batch: list[dict[str, object]] = []
        row_count = 0
        start_time: int | None = None
        end_time: int | None = None
        for row in _iter_csv_rows(path, timestamp_format):
            timestamp = int(row["timestamp"])
            if start_time is None:
                start_time = timestamp
            end_time = timestamp
            row_count += 1
            _update_digest(digest, row)
            batch.append({"dataset_id": dataset_id, **row})
            if len(batch) >= self._batch_size:
                session.execute(insert(ResearchCandlestick), batch)
                batch.clear()
        if batch:
            session.execute(insert(ResearchCandlestick), batch)
        if row_count == 0 or start_time is None or end_time is None:
            raise ResearchCsvValidationError("CSV must contain at least one data row")
        return _CsvSummary(
            row_count=row_count,
            start_time=start_time,
            end_time=end_time,
            checksum_sha256=digest.hexdigest(),
        )

    @staticmethod
    def _verify_existing(
        session: Session,
        existing: ResearchDataset,
        spec: ResearchDatasetSpec,
        summary: _CsvSummary,
        metadata_json: str,
    ) -> None:
        expected = (
            spec.product_id,
            spec.timeframe,
            spec.source,
            spec.revision,
            spec.timestamp_format,
            spec.roll_policy,
            summary.start_time,
            summary.end_time,
            summary.row_count,
            summary.checksum_sha256,
            metadata_json,
        )
        actual = (
            existing.product_id,
            existing.timeframe,
            existing.source,
            existing.revision,
            existing.timestamp_format,
            existing.roll_policy,
            existing.start_time,
            existing.end_time,
            existing.row_count,
            existing.checksum_sha256,
            existing.metadata_json,
            existing.quality_status,
        )
        expected = (*expected, "validated")
        actual_count = (
            session.query(func.count(ResearchCandlestick.timestamp))
            .filter(ResearchCandlestick.dataset_id == spec.dataset_id)
            .scalar()
        )
        if actual != expected or actual_count != summary.row_count:
            raise ResearchDatasetConflictError(
                "dataset_id already exists with different content or provenance"
            )
