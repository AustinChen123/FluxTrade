"""Bounded SQL readers for immutable browser result projections."""

from collections.abc import Callable
from decimal import Decimal
import re
from typing import Literal, TypedDict, cast

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from src.control_plane.backtest_result_http_contract import (
    BacktestResultQuery,
    IndexCursor,
    IndexCursorBinding,
    InvalidBacktestResultHttpRequest,
    TradesCursor,
    TradesCursorBinding,
    _valid_digest,
    _valid_job_id,
    encode_result_cursor,
    format_utc_milliseconds,
    verify_result_cursor,
    wire_decimal,
)
from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
    BacktestResultSummary,
)

_NONFINITE = (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"))
_FORMAL_TEXT_FIELDS = (
    "job_id",
    "strategy_id",
    "dataset_id",
    "subject_id",
    "input_digest",
    "result_digest",
    "product_id",
    "timeframe",
    "currency",
)
_FORMAL_TIME_FIELDS = ("start_time", "end_time", "completed_at")
_FORMAL_DECIMAL_FIELDS = (
    "total_pnl",
    "initial_balance",
    "net_pnl",
    "return_pct",
    "max_drawdown",
    "sharpe",
    "sortino",
    "calmar",
)


class BacktestResultsIndexItem(TypedDict):
    job_id: str
    subject_id: str
    dataset_id: str
    product_id: str
    timeframe: str
    started_at: str
    ended_at: str
    completed_at: str
    result_digest: str


class BacktestResultsIndexPage(TypedDict):
    items: list[BacktestResultsIndexItem]
    next_cursor: str | None
    revision: Literal[1]


class BacktestResultsTrade(TypedDict):
    id: str
    entry_time: str
    exit_time: str
    entry_price: str
    exit_price: str
    side: str
    quantity: str
    pnl: str
    fee: str


class BacktestResultsTradesPage(TypedDict):
    items: list[BacktestResultsTrade]
    next_cursor: str | None
    revision: Literal[1]


class BacktestResultsMetrics(TypedDict):
    net_pnl: str
    return_pct: str
    max_drawdown: str
    sharpe: str
    sortino: str
    calmar: str


class BacktestResultsEquityItem(TypedDict):
    timestamp: str
    equity: str
    drawdown: str


class BacktestResultsMonthlyReturn(TypedDict):
    month: str
    return_pct: str


class BacktestResultsDistributionItem(TypedDict):
    lower: str | None
    upper: str | None
    count: int


class BacktestResultsTradePage(TypedDict):
    items: list[BacktestResultsTrade]
    total_count: int
    next_cursor: str | None


class BacktestResultsDetail(TypedDict):
    job_id: str
    strategy_id: str
    subject_id: str
    dataset_id: str
    product_id: str
    timeframe: str
    started_at: str
    ended_at: str
    currency: str
    metrics: BacktestResultsMetrics
    equity: list[BacktestResultsEquityItem]
    monthly_returns: list[BacktestResultsMonthlyReturn]
    pnl_distribution: list[BacktestResultsDistributionItem]
    trade_page: BacktestResultsTradePage
    input_digest: str
    result_digest: str
    revision: Literal[1]


class BacktestResultsReadUnavailable(RuntimeError):
    """Sanitized SQL or stored-projection failure for the read service."""

    def __init__(self) -> None:
        super().__init__("browser_result_backend_unavailable")


class BacktestResultsUnavailable(RuntimeError):
    """A job or linked summary exists without a qualifying formal result."""

    def __init__(self) -> None:
        super().__init__("result_unavailable")


class BacktestResultsNotFound(RuntimeError):
    """Neither a qualifying result nor an existing job is present."""

    def __init__(self) -> None:
        super().__init__("result_not_found")


def _formal_result_qualification():
    """The sole SQL qualification classifier matching formal ORM constraints."""
    fields = (
        BacktestResultSummary.strategy_id,
        BacktestResultSummary.start_time,
        BacktestResultSummary.end_time,
        *(getattr(BacktestResultSummary, name) for name in _FORMAL_TEXT_FIELDS),
        *(getattr(BacktestResultSummary, name) for name in _FORMAL_TIME_FIELDS),
        *(getattr(BacktestResultSummary, name) for name in _FORMAL_DECIMAL_FIELDS),
    )
    finite = tuple(
        getattr(BacktestResultSummary, name).not_in(_NONFINITE)
        for name in _FORMAL_DECIMAL_FIELDS
    )
    return and_(
        *(field.is_not(None) for field in fields),
        BacktestResultSummary.subject_kind == "STRATEGY_ARTIFACT",
        BacktestResultSummary.initial_balance > 0,
        BacktestResultSummary.max_drawdown >= 0,
        *finite,
    )


def _index_item(row: BacktestResultSummary) -> BacktestResultsIndexItem:
    if (
        not _valid_job_id(row.job_id)
        or not _valid_digest(row.result_digest)
        or any(
            type(value) is not str or not value.strip()
            for value in (
                row.subject_id,
                row.dataset_id,
                row.product_id,
                row.timeframe,
            )
        )
        or type(row.start_time) is not int
        or type(row.end_time) is not int
        or type(row.completed_at) is not int
        or row.start_time > row.end_time
    ):
        raise ValueError
    return {
        "job_id": cast(str, row.job_id),
        "subject_id": cast(str, row.subject_id),
        "dataset_id": cast(str, row.dataset_id),
        "product_id": cast(str, row.product_id),
        "timeframe": cast(str, row.timeframe),
        "started_at": format_utc_milliseconds(row.start_time),
        "ended_at": format_utc_milliseconds(row.end_time),
        "completed_at": format_utc_milliseconds(row.completed_at),
        "result_digest": cast(str, row.result_digest),
    }


def _trade_item(job_id: str, row: BacktestClosedTrade) -> BacktestResultsTrade:
    if (
        not _valid_job_id(job_id)
        or type(row.sequence) is not int
        or row.sequence < 0
        or type(row.side) is not str
        or row.side not in ("LONG", "SHORT")
        or type(row.entry_time) is not int
        or type(row.exit_time) is not int
        or row.entry_time > row.exit_time
        or type(row.quantity) is not Decimal
        or row.quantity <= 0
    ):
        raise ValueError
    return {
        "id": f"{job_id}:{row.sequence}",
        "entry_time": format_utc_milliseconds(row.entry_time),
        "exit_time": format_utc_milliseconds(row.exit_time),
        "entry_price": wire_decimal(row.entry_price),
        "exit_price": wire_decimal(row.exit_price),
        "side": row.side,
        "quantity": wire_decimal(row.quantity),
        "pnl": wire_decimal(row.pnl),
        "fee": wire_decimal(row.fee),
    }


def _equity_item(row: BacktestEquitySample) -> BacktestResultsEquityItem:
    if (
        type(row.sequence) is not int
        or row.sequence < 0
        or type(row.timestamp) is not int
    ):
        raise ValueError
    if type(row.drawdown) is not Decimal or row.drawdown < 0:
        raise ValueError
    return {
        "timestamp": format_utc_milliseconds(row.timestamp),
        "equity": wire_decimal(row.equity),
        "drawdown": wire_decimal(row.drawdown),
    }


def _monthly_return_item(row: BacktestMonthlyReturn) -> BacktestResultsMonthlyReturn:
    if (
        type(row.month) is not str
        or re.fullmatch(r"[0-9]{4}-(?:0[1-9]|1[0-2])", row.month) is None
    ):
        raise ValueError
    return {"month": row.month, "return_pct": wire_decimal(row.return_pct)}


def _distribution_item(
    row: BacktestPnlDistribution,
) -> BacktestResultsDistributionItem:
    if (
        type(row.sequence) is not int
        or row.sequence < 0
        or type(row.count) is not int
        or row.count < 0
        or (row.lower is not None and type(row.lower) is not Decimal)
        or (row.upper is not None and type(row.upper) is not Decimal)
    ):
        raise ValueError
    return {
        "lower": None if row.lower is None else wire_decimal(row.lower),
        "upper": None if row.upper is None else wire_decimal(row.upper),
        "count": row.count,
    }


class BacktestResultsQueryService:
    """Own SQL sessions and project only approved plain result wire values."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        cursor_key: bytes,
        job_lookup: Callable[[str], object | None] | None = None,
    ):
        if type(cursor_key) is not bytes or len(cursor_key) != 32:
            raise ValueError("cursor_key must be exactly 32 bytes")
        self._session_factory = session_factory
        self._cursor_key = cursor_key
        self._job_lookup = job_lookup

    def list_index(self, query: BacktestResultQuery) -> BacktestResultsIndexPage:
        if type(query) is not BacktestResultQuery:
            raise InvalidBacktestResultHttpRequest() from None
        limit = 100 if query.limit is None else query.limit
        if type(limit) is not int or not 1 <= limit <= 500:
            raise InvalidBacktestResultHttpRequest() from None
        after = None
        if query.cursor is not None:
            after = verify_result_cursor(
                self._cursor_key, query.cursor, IndexCursorBinding()
            )
            if type(after) is not IndexCursor:
                raise InvalidBacktestResultHttpRequest() from None

        try:
            statement = select(BacktestResultSummary).where(
                _formal_result_qualification()
            )
            if after is not None:
                statement = statement.where(
                    or_(
                        BacktestResultSummary.completed_at < after.completed_at,
                        and_(
                            BacktestResultSummary.completed_at == after.completed_at,
                            BacktestResultSummary.job_id < after.job_id,
                        ),
                    )
                )
            statement = statement.order_by(
                BacktestResultSummary.completed_at.desc(),
                BacktestResultSummary.job_id.desc(),
            ).limit(limit + 1)
            with self._session_factory() as session:
                rows = session.scalars(statement).all()
                has_more = len(rows) > limit
                page_rows = rows[:limit]
                items = [_index_item(row) for row in page_rows]
                next_cursor = None
                if has_more:
                    last = page_rows[-1]
                    next_cursor = encode_result_cursor(
                        self._cursor_key,
                        IndexCursor(
                            cast(int, last.completed_at), cast(str, last.job_id)
                        ),
                    )
            return cast(
                BacktestResultsIndexPage,
                {"items": items, "next_cursor": next_cursor, "revision": 1},
            )
        except Exception:
            raise BacktestResultsReadUnavailable() from None

    def _qualified_summary(
        self, session: Session, job_id: str
    ) -> BacktestResultSummary:
        summary = session.scalars(
            select(BacktestResultSummary).where(
                _formal_result_qualification(),
                BacktestResultSummary.job_id == job_id,
            )
        ).one_or_none()
        if summary is not None:
            return summary
        linked_summary = session.scalars(
            select(BacktestResultSummary.id)
            .where(BacktestResultSummary.job_id == job_id)
            .limit(1)
        ).first()
        if linked_summary is not None or (
            self._job_lookup is not None and self._job_lookup(job_id) is not None
        ):
            raise BacktestResultsUnavailable()
        raise BacktestResultsNotFound()

    def _trade_page_items(
        self,
        session: Session,
        summary: BacktestResultSummary,
        job_id: str,
        digest: str,
        limit: int,
        after: TradesCursor | None,
    ) -> tuple[list[BacktestResultsTrade], str | None]:
        statement = select(BacktestClosedTrade).where(
            BacktestClosedTrade.summary_id == summary.id
        )
        if after is not None:
            statement = statement.where(BacktestClosedTrade.sequence > after.sequence)
        rows = session.scalars(
            statement.order_by(BacktestClosedTrade.sequence.asc()).limit(limit + 1)
        ).all()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        next_cursor = None
        if has_more:
            next_cursor = encode_result_cursor(
                self._cursor_key,
                TradesCursor(job_id, digest, page_rows[-1].sequence),
            )
        return [_trade_item(job_id, row) for row in page_rows], next_cursor

    def _trade_page(
        self,
        session: Session,
        summary: BacktestResultSummary,
        job_id: str,
        digest: str,
    ) -> BacktestResultsTradePage:
        count = session.scalar(
            select(func.count())
            .select_from(BacktestClosedTrade)
            .where(BacktestClosedTrade.summary_id == summary.id)
        )
        if type(count) is not int or count < 0:
            raise ValueError
        items, next_cursor = self._trade_page_items(
            session, summary, job_id, digest, 100, None
        )
        return {"items": items, "total_count": count, "next_cursor": next_cursor}

    def get_detail(self, job_id: str) -> BacktestResultsDetail:
        if not _valid_job_id(job_id):
            raise InvalidBacktestResultHttpRequest() from None
        try:
            with self._session_factory() as session:
                summary = self._qualified_summary(session, job_id)
                index_item = _index_item(summary)
                if (
                    type(summary.strategy_id) is not str
                    or not summary.strategy_id.strip()
                    or type(summary.currency) is not str
                    or not summary.currency.strip()
                    or not _valid_digest(summary.input_digest)
                ):
                    raise ValueError
                equity = session.scalars(
                    select(BacktestEquitySample)
                    .where(BacktestEquitySample.summary_id == summary.id)
                    .order_by(BacktestEquitySample.sequence.asc())
                ).all()
                monthly = session.scalars(
                    select(BacktestMonthlyReturn)
                    .where(BacktestMonthlyReturn.summary_id == summary.id)
                    .order_by(BacktestMonthlyReturn.month.asc())
                ).all()
                distribution = session.scalars(
                    select(BacktestPnlDistribution)
                    .where(BacktestPnlDistribution.summary_id == summary.id)
                    .order_by(BacktestPnlDistribution.sequence.asc())
                ).all()
                trade_page = self._trade_page(
                    session, summary, job_id, index_item["result_digest"]
                )
                result: BacktestResultsDetail = {
                    "job_id": index_item["job_id"],
                    "strategy_id": summary.strategy_id,
                    "subject_id": index_item["subject_id"],
                    "dataset_id": index_item["dataset_id"],
                    "product_id": index_item["product_id"],
                    "timeframe": index_item["timeframe"],
                    "started_at": index_item["started_at"],
                    "ended_at": index_item["ended_at"],
                    "currency": summary.currency,
                    "metrics": {
                        "net_pnl": wire_decimal(cast(Decimal, summary.net_pnl)),
                        "return_pct": wire_decimal(cast(Decimal, summary.return_pct)),
                        "max_drawdown": wire_decimal(
                            cast(Decimal, summary.max_drawdown)
                        ),
                        "sharpe": wire_decimal(cast(Decimal, summary.sharpe)),
                        "sortino": wire_decimal(cast(Decimal, summary.sortino)),
                        "calmar": wire_decimal(cast(Decimal, summary.calmar)),
                    },
                    "equity": [_equity_item(row) for row in equity],
                    "monthly_returns": [_monthly_return_item(row) for row in monthly],
                    "pnl_distribution": [
                        _distribution_item(row) for row in distribution
                    ],
                    "trade_page": trade_page,
                    "input_digest": cast(str, summary.input_digest),
                    "result_digest": index_item["result_digest"],
                    "revision": 1,
                }
            return result
        except (
            BacktestResultsUnavailable,
            BacktestResultsNotFound,
            InvalidBacktestResultHttpRequest,
        ):
            raise
        except Exception:
            raise BacktestResultsReadUnavailable() from None

    def list_trades(
        self, job_id: str, query: BacktestResultQuery
    ) -> BacktestResultsTradesPage:
        if not _valid_job_id(job_id) or type(query) is not BacktestResultQuery:
            raise InvalidBacktestResultHttpRequest() from None
        limit = 100 if query.limit is None else query.limit
        if type(limit) is not int or not 1 <= limit <= 500:
            raise InvalidBacktestResultHttpRequest() from None

        try:
            with self._session_factory() as session:
                summary = self._qualified_summary(session, job_id)

                digest = summary.result_digest
                if not _valid_job_id(summary.job_id) or not _valid_digest(digest):
                    raise ValueError
                after = None
                if query.cursor is not None:
                    after = verify_result_cursor(
                        self._cursor_key,
                        query.cursor,
                        TradesCursorBinding(job_id, cast(str, digest)),
                    )
                    if type(after) is not TradesCursor:
                        raise InvalidBacktestResultHttpRequest() from None

                items, next_cursor = self._trade_page_items(
                    session, summary, job_id, cast(str, digest), limit, after
                )
            return cast(
                BacktestResultsTradesPage,
                {"items": items, "next_cursor": next_cursor, "revision": 1},
            )
        except (
            BacktestResultsUnavailable,
            BacktestResultsNotFound,
            InvalidBacktestResultHttpRequest,
        ):
            raise
        except Exception:
            raise BacktestResultsReadUnavailable() from None
