"""Flow-neutral unitized performance for cash-spot replay accounts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, DecimalException, localcontext
from typing import Sequence

from src.core.backtest.external_funding import ExternalFundingApplication


ANNUALIZATION_SECONDS = 31_557_600
_ANNUALIZATION_MILLISECONDS = ANNUALIZATION_SECONDS * 1000
_CALCULATION_PRECISION = 50


@dataclass(frozen=True, slots=True)
class UnitizedNavSample:
    timestamp: int
    equity: Decimal
    units: Decimal
    nav: Decimal
    phase: str
    funding_event_id: str | None = None


@dataclass(frozen=True, slots=True)
class FlowNeutralPerformanceReport:
    initial_capital: Decimal
    external_contributions: Decimal
    total_contributed_capital: Decimal
    final_equity: Decimal | None
    net_pnl: Decimal | None
    ending_units: Decimal
    ending_nav: Decimal | None
    time_weighted_return: Decimal | None
    annualized_time_weighted_return: Decimal | None
    annualization_seconds: int
    start_timestamp: int | None
    end_timestamp: int | None
    duration_milliseconds: int
    unitized_max_drawdown: Decimal
    raw_max_drawdown: Decimal
    nav_samples: tuple[UnitizedNavSample, ...]
    raw_equity_samples: tuple[tuple[int, Decimal], ...]

    def metric_fields(self) -> dict[str, Decimal | int | None]:
        return {
            "initial_capital": self.initial_capital,
            "external_contributions": self.external_contributions,
            "total_contributed_capital": self.total_contributed_capital,
            "final_equity": self.final_equity,
            "net_pnl": self.net_pnl,
            "ending_units": self.ending_units,
            "ending_nav": self.ending_nav,
            "time_weighted_return": self.time_weighted_return,
            "annualized_time_weighted_return": (
                self.annualized_time_weighted_return
            ),
            "annualization_seconds": self.annualization_seconds,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "duration_milliseconds": self.duration_milliseconds,
            "unitized_max_drawdown": self.unitized_max_drawdown,
            "raw_max_drawdown": self.raw_max_drawdown,
        }

    def nav_return_samples(self) -> tuple[tuple[int, Decimal], ...]:
        return tuple((sample.timestamp, sample.nav) for sample in self.nav_samples)


class FlowNeutralPerformanceTracker:
    """Maintain contribution-neutral fund units and NAV from exact Decimal inputs."""

    def __init__(self, initial_capital: Decimal) -> None:
        if not isinstance(initial_capital, Decimal):
            raise TypeError("flow-neutral initial_capital must be Decimal")
        if not initial_capital.is_finite() or initial_capital <= 0:
            raise ValueError(
                "flow-neutral initial_capital must be finite and positive"
            )
        self._initial_capital = initial_capital
        self._external_contributions = Decimal("0")
        self._units = initial_capital
        self._current_nav = Decimal("1")
        self._last_equity = initial_capital
        self._peak_nav = Decimal("1")
        self._unitized_max_drawdown = Decimal("0")
        self._raw_peak = initial_capital
        self._raw_max_drawdown = Decimal("0")
        self._samples: list[UnitizedNavSample] = []
        self._applied_event_ids: set[str] = set()
        self._last_timestamp: int | None = None

    @property
    def unitized_max_drawdown(self) -> Decimal:
        return self._unitized_max_drawdown

    def record_funding(self, application: ExternalFundingApplication) -> None:
        event = application.event
        if event.event_id in self._applied_event_ids:
            raise ValueError(
                f"flow-neutral duplicate funding event: {event.event_id}"
            )
        if application.post_equity - application.pre_equity != event.amount:
            raise ValueError("flow-neutral funding application is not conserved")
        self._validate_observation(
            application.applied_at,
            application.pre_equity,
        )

        nav_before = self._advance_nav(application.pre_equity)
        try:
            with localcontext() as context:
                context.prec = _CALCULATION_PRECISION
                issued_units = event.amount / nav_before
                updated_units = self._units + issued_units
                updated_contributions = (
                    self._external_contributions + event.amount
                )
        except DecimalException as exc:
            raise ValueError("flow-neutral funding calculation failed") from exc
        if (
            not updated_units.is_finite()
            or updated_units <= 0
            or not updated_contributions.is_finite()
        ):
            raise ValueError("flow-neutral funding calculation is not finite")
        self._append_sample(
            timestamp=application.applied_at,
            equity=application.pre_equity,
            nav=nav_before,
            phase="pre_flow",
            funding_event_id=event.event_id,
        )
        self._units = updated_units
        self._external_contributions = updated_contributions
        self._applied_event_ids.add(event.event_id)
        self._append_sample(
            timestamp=application.applied_at,
            equity=application.post_equity,
            nav=nav_before,
            phase="post_flow",
            funding_event_id=event.event_id,
        )

    def observe(self, *, timestamp: int, equity: Decimal) -> None:
        self._validate_observation(timestamp, equity)
        if self._samples:
            latest = self._samples[-1]
            if (
                latest.timestamp == timestamp
                and latest.equity == equity
                and latest.units == self._units
            ):
                return
        nav = self._advance_nav(equity)
        self._append_sample(
            timestamp=timestamp,
            equity=equity,
            nav=nav,
            phase="valuation",
        )

    def report(self) -> FlowNeutralPerformanceReport:
        final_equity = self._samples[-1].equity if self._samples else None
        ending_nav = self._samples[-1].nav if self._samples else None
        start_timestamp = self._samples[0].timestamp if self._samples else None
        end_timestamp = self._samples[-1].timestamp if self._samples else None
        duration_milliseconds = (
            0
            if start_timestamp is None or end_timestamp is None
            else end_timestamp - start_timestamp
        )
        net_pnl = (
            None
            if final_equity is None
            else final_equity
            - self._initial_capital
            - self._external_contributions
        )
        time_weighted_return = (
            None if ending_nav is None else ending_nav - Decimal("1")
        )
        return FlowNeutralPerformanceReport(
            initial_capital=self._initial_capital,
            external_contributions=self._external_contributions,
            total_contributed_capital=(
                self._initial_capital + self._external_contributions
            ),
            final_equity=final_equity,
            net_pnl=net_pnl,
            ending_units=self._units,
            ending_nav=ending_nav,
            time_weighted_return=time_weighted_return,
            annualized_time_weighted_return=self._annualized_return(ending_nav),
            annualization_seconds=ANNUALIZATION_SECONDS,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            duration_milliseconds=duration_milliseconds,
            unitized_max_drawdown=self._unitized_max_drawdown,
            raw_max_drawdown=self._raw_max_drawdown,
            nav_samples=tuple(self._samples),
            raw_equity_samples=tuple(
                (sample.timestamp, sample.equity) for sample in self._samples
            ),
        )

    def _append_sample(
        self,
        *,
        timestamp: int,
        equity: Decimal,
        nav: Decimal,
        phase: str,
        funding_event_id: str | None = None,
    ) -> None:
        if not nav.is_finite() or nav <= 0:
            raise ValueError("flow-neutral NAV must be finite and positive")
        self._peak_nav = max(self._peak_nav, nav)
        with localcontext() as context:
            context.prec = _CALCULATION_PRECISION
            drawdown = (self._peak_nav - nav) / self._peak_nav
        self._unitized_max_drawdown = max(
            self._unitized_max_drawdown,
            drawdown,
        )
        self._raw_peak = max(self._raw_peak, equity)
        self._raw_max_drawdown = max(
            self._raw_max_drawdown,
            self._raw_peak - equity,
        )
        self._samples.append(
            UnitizedNavSample(
                timestamp=timestamp,
                equity=equity,
                units=self._units,
                nav=nav,
                phase=phase,
                funding_event_id=funding_event_id,
            )
        )
        self._current_nav = nav
        self._last_equity = equity
        self._last_timestamp = timestamp

    def _advance_nav(self, equity: Decimal) -> Decimal:
        if self._last_equity <= 0 or self._current_nav <= 0:
            raise ValueError("flow-neutral valuation anchor must remain positive")
        with localcontext() as context:
            context.prec = _CALCULATION_PRECISION
            return self._current_nav * (equity / self._last_equity)

    def _validate_observation(self, timestamp: int, equity: Decimal) -> None:
        if type(timestamp) is not int or timestamp < 0:
            raise ValueError("flow-neutral timestamp must be non-negative")
        if self._last_timestamp is not None and timestamp < self._last_timestamp:
            raise ValueError("flow-neutral timestamps must be non-decreasing")
        if not isinstance(equity, Decimal):
            raise TypeError("flow-neutral equity must be Decimal")
        if not equity.is_finite() or equity <= 0:
            raise ValueError("flow-neutral equity must be finite and positive")

    def _annualized_return(self, ending_nav: Decimal | None) -> Decimal | None:
        if ending_nav is None or len(self._samples) < 2:
            return None
        duration_ms = self._samples[-1].timestamp - self._samples[0].timestamp
        if duration_ms <= 0:
            return None
        with localcontext() as context:
            context.prec = _CALCULATION_PRECISION
            exponent = Decimal(_ANNUALIZATION_MILLISECONDS) / Decimal(duration_ms)
            return (ending_nav.ln() * exponent).exp() - Decimal("1")


def return_metric_inputs(
    equity_samples: Sequence[tuple[int, Decimal]],
    initial_balance: Decimal,
    start_time: int,
    end_time: int,
    report: FlowNeutralPerformanceReport | None,
) -> tuple[Sequence[tuple[int, Decimal]], Decimal, int, int]:
    """Select raw equity or contribution-neutral NAV for return statistics."""
    if (
        report is None
        or report.start_timestamp is None
        or report.end_timestamp is None
    ):
        return equity_samples, initial_balance, start_time, end_time
    return (
        report.nav_return_samples(),
        Decimal("1"),
        report.start_timestamp,
        report.end_timestamp,
    )
