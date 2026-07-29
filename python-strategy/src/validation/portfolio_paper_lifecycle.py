"""Portfolio-aware paper lifecycle evidence over the shared runtime."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from src.core.adapters.simulated import SimulatedAdapter
from src.core.clock import BacktestClock
from src.core.engine import StrategyEngine
from src.core.mocks.account_service import BacktestAccountService
from src.core.portfolio_runtime import PortfolioDefinition
from src.core.repositories import LiveOrderRepository
from src.validation.paper_lifecycle import (
    ET,
    PaperLifecycleReport,
    PaperScenarioReport,
    _adapter,
    _assert_position,
    _assert_reconciliation_resolved,
    _assert_working_protection,
    _candle,
    _et_candle,
    _finalize_report,
    _instrument_evidence,
    _protected_entry,
    _session_factory,
    _validate_paper_product,
)
from src.validation.strategy_evidence import (
    StrategyEvidenceIdentity,
    require_verified_portfolio_identity,
)


def run_portfolio_paper_lifecycle(
    workspace: Path,
    *,
    portfolio_factory: Callable[[], PortfolioDefinition],
    scenario_quantities: Mapping[str, Decimal] | None = None,
) -> PaperLifecycleReport:
    """Exercise every sleeve through the shared portfolio execution runtime."""

    baseline = portfolio_factory()
    _validate_paper_product(baseline.product_id)
    identity = require_verified_portfolio_identity(baseline)
    quantities = _scenario_quantities(baseline, scenario_quantities)
    workspace.mkdir(parents=True, exist_ok=True)

    reports: list[PaperScenarioReport] = []
    for index, sleeve in enumerate(baseline.sleeves):
        for scenario in ("stop_loss", "take_profit"):
            reports.append(
                _run_protection_scenario(
                    workspace / f"{scenario}_{index}.db",
                    scenario=scenario,
                    portfolio_factory=portfolio_factory,
                    expected_identity=identity,
                    target_strategy_id=sleeve.strategy.strategy_id,
                    quantity=quantities[sleeve.strategy.strategy_id],
                )
            )
    reports.append(
        _run_working_entry_restart_scenario(
            workspace / "working_entry_restart.db",
            portfolio_factory=portfolio_factory,
            expected_identity=identity,
            quantities=quantities,
        )
    )
    reports.append(
        _run_hard_flat_scenario(
            workspace / "hard_flat.db",
            portfolio_factory=portfolio_factory,
            expected_identity=identity,
            quantities=quantities,
        )
    )
    return PaperLifecycleReport(
        instrument=_instrument_evidence(baseline.product_id),
        scenarios=tuple(reports),
    )


def _run_protection_scenario(
    database_path: Path,
    *,
    scenario: str,
    portfolio_factory: Callable[[], PortfolioDefinition],
    expected_identity: StrategyEvidenceIdentity,
    target_strategy_id: str,
    quantity: Decimal,
) -> PaperScenarioReport:
    definition = _verified_portfolio(portfolio_factory, expected_identity)
    strategy_ids = _strategy_ids(definition)
    if target_strategy_id not in strategy_ids:
        raise ValueError("paper target sleeve is absent from portfolio replay")
    session_factory = _session_factory(
        database_path,
        definition.product_id,
        (definition.portfolio_id, *strategy_ids),
    )
    adapter = _adapter(definition.product_id)
    clock = BacktestClock()
    engine = _engine(session_factory, adapter, clock, definition)
    entry_candle = _candle(definition.product_id, 1_800_000_000_000)
    clock.set_time(entry_candle.timestamp / 1_000)
    if not engine.process_signal(
        _protected_entry(
            target_strategy_id,
            definition.product_id,
            entry_candle.timestamp,
            quantity=quantity,
        ),
        entry_candle,
    ):
        raise AssertionError(f"{scenario} portfolio paper entry was not submitted")

    fill_candle = _candle(
        definition.product_id,
        entry_candle.timestamp + 300_000,
    )
    clock.set_time(fill_candle.timestamp / 1_000)
    engine.on_backtest_market_data(fill_candle)
    _assert_position(
        adapter,
        definition.product_id,
        target_strategy_id,
        "LONG",
        quantity,
    )
    _assert_working_protection(
        adapter,
        definition.product_id,
        target_strategy_id,
        quantity,
    )

    restarted = _engine(
        session_factory,
        adapter,
        clock,
        _verified_portfolio(portfolio_factory, expected_identity),
    )
    restart_reconcile = (
        restarted.execution_engine.reconcile_recoverable_client_orders()
    )
    _assert_reconciliation_resolved(
        restart_reconcile,
        context=f"{scenario} portfolio restart reconciliation",
    )

    trigger = (
        _candle(
            definition.product_id,
            fill_candle.timestamp + 300_000,
            open_="20000",
            high="20001",
            low="19994",
            close="19995",
        )
        if scenario == "stop_loss"
        else _candle(
            definition.product_id,
            fill_candle.timestamp + 300_000,
            open_="20000",
            high="20006",
            low="19999",
            close="20005",
        )
    )
    clock.set_time(trigger.timestamp / 1_000)
    restarted.on_backtest_market_data(trigger)
    return _finalize_report(
        session_factory,
        restarted.execution_engine,
        adapter,
        definition.product_id,
        strategy_ids,
        f"{scenario}:{target_strategy_id}",
        int(restart_reconcile["unresolved_count"]),
        int(restart_reconcile["verification_blocked_count"]),
        driver="portfolio_synthetic_protected_entry",
        strategy_identity=expected_identity,
    )


def _run_working_entry_restart_scenario(
    database_path: Path,
    *,
    portfolio_factory: Callable[[], PortfolioDefinition],
    expected_identity: StrategyEvidenceIdentity,
    quantities: Mapping[str, Decimal],
) -> PaperScenarioReport:
    definition = _verified_portfolio(portfolio_factory, expected_identity)
    strategy_ids = _strategy_ids(definition)
    _require_scenario_capacity(definition, quantities)
    session_factory = _session_factory(
        database_path,
        definition.product_id,
        (definition.portfolio_id, *strategy_ids),
    )
    adapter = _adapter(definition.product_id)
    clock = BacktestClock()
    engine = _engine(session_factory, adapter, clock, definition)
    entry_candle = _candle(definition.product_id, 1_800_000_000_000)
    clock.set_time(entry_candle.timestamp / 1_000)
    order_ids: list[str] = []
    for strategy_id in strategy_ids:
        signal = _protected_entry(
            strategy_id,
            definition.product_id,
            entry_candle.timestamp,
            quantity=quantities[strategy_id],
        ).model_copy(
            update={
                "price": Decimal("1000"),
                "metadata": {"evidence": "portfolio_working_entry_restart"},
            }
        )
        if not engine.process_signal(signal, entry_candle):
            raise AssertionError(
                f"working portfolio entry was not submitted: {strategy_id}"
            )
        working = [
            order
            for order in adapter.get_open_orders(
                definition.product_id,
                strategy_id,
            )
            if str(order.type) == "limit"
        ]
        if len(working) != 1:
            raise AssertionError(
                f"working portfolio entry was not retained: {strategy_id}"
            )
        order_ids.append(str(working[0].id))

    restarted = _engine(
        session_factory,
        adapter,
        clock,
        _verified_portfolio(portfolio_factory, expected_identity),
    )
    restart_reconcile = (
        restarted.execution_engine.reconcile_recoverable_client_orders()
    )
    _assert_reconciliation_resolved(
        restart_reconcile,
        context="working portfolio restart reconciliation",
    )
    for order_id in order_ids:
        if not restarted.execution_engine.cancel_order(order_id):
            raise AssertionError(
                f"working portfolio entry could not be cancelled: {order_id}"
            )
    return _finalize_report(
        session_factory,
        restarted.execution_engine,
        adapter,
        definition.product_id,
        strategy_ids,
        "working_entry_restart",
        int(restart_reconcile["unresolved_count"]),
        int(restart_reconcile["verification_blocked_count"]),
        driver="portfolio_working_entry",
        strategy_identity=expected_identity,
    )


def _run_hard_flat_scenario(
    database_path: Path,
    *,
    portfolio_factory: Callable[[], PortfolioDefinition],
    expected_identity: StrategyEvidenceIdentity,
    quantities: Mapping[str, Decimal],
) -> PaperScenarioReport:
    definition = _verified_portfolio(portfolio_factory, expected_identity)
    strategy_ids = _strategy_ids(definition)
    _require_scenario_capacity(definition, quantities)
    session_factory = _session_factory(
        database_path,
        definition.product_id,
        (definition.portfolio_id, *strategy_ids),
    )
    adapter = _adapter(definition.product_id)
    clock = BacktestClock()
    engine = _engine(session_factory, adapter, clock, definition)
    prior_bar = _et_candle(
        definition.product_id,
        datetime(2026, 9, 8, 16, 25, tzinfo=ET),
    )
    for strategy_id in strategy_ids:
        clock.set_time(prior_bar.timestamp / 1_000)
        if not engine.process_signal(
            _protected_entry(
                strategy_id,
                definition.product_id,
                prior_bar.timestamp,
                quantity=quantities[strategy_id],
                stop_loss=Decimal("19900"),
                take_profit=Decimal("20100"),
            ),
            prior_bar,
        ):
            raise AssertionError(
                f"hard-flat portfolio entry was not submitted: {strategy_id}"
            )

    fill_candle = _et_candle(
        definition.product_id,
        datetime(2026, 9, 8, 16, 30, tzinfo=ET),
    )
    clock.set_time(fill_candle.timestamp / 1_000)
    engine.on_backtest_market_data(fill_candle)
    for strategy_id in strategy_ids:
        _assert_position(
            adapter,
            definition.product_id,
            strategy_id,
            "LONG",
            quantities[strategy_id],
        )
        _assert_working_protection(
            adapter,
            definition.product_id,
            strategy_id,
            quantities[strategy_id],
        )

    restarted = _engine(
        session_factory,
        adapter,
        clock,
        _verified_portfolio(portfolio_factory, expected_identity),
    )
    restart_reconcile = (
        restarted.execution_engine.reconcile_recoverable_client_orders()
    )
    _assert_reconciliation_resolved(
        restart_reconcile,
        context="hard-flat portfolio restart reconciliation",
    )

    decision_bar = _et_candle(
        definition.product_id,
        datetime(2026, 9, 8, 16, 35, tzinfo=ET),
    )
    clock.set_time(decision_bar.timestamp / 1_000)
    restarted.on_backtest_market_data(decision_bar, decision_bar)
    exit_fill = _et_candle(
        definition.product_id,
        datetime(2026, 9, 8, 16, 40, tzinfo=ET),
    )
    clock.set_time(exit_fill.timestamp / 1_000)
    restarted.on_backtest_market_data(exit_fill)
    return _finalize_report(
        session_factory,
        restarted.execution_engine,
        adapter,
        definition.product_id,
        strategy_ids,
        "hard_flat_1640_et",
        int(restart_reconcile["unresolved_count"]),
        int(restart_reconcile["verification_blocked_count"]),
        driver="portfolio_strategy_engine",
        strategy_identity=expected_identity,
    )


def _engine(
    session_factory,
    adapter: SimulatedAdapter,
    clock: BacktestClock,
    definition: PortfolioDefinition,
) -> StrategyEngine:
    repository = LiveOrderRepository(db_session_factory=session_factory)
    account_service = BacktestAccountService(adapter=adapter)
    engine = StrategyEngine(
        None,
        clock,
        order_repository=repository,
        account_service=account_service,
        adapter=adapter,
        db_session_factory=session_factory,
        audit_external_orders=True,
        is_backtest=True,
    )
    engine.execution_engine.default_quantity = Decimal("1")
    engine.add_portfolio(definition)
    return engine


def _verified_portfolio(
    portfolio_factory: Callable[[], PortfolioDefinition],
    expected_identity: StrategyEvidenceIdentity,
) -> PortfolioDefinition:
    definition = portfolio_factory()
    identity = require_verified_portfolio_identity(definition)
    if identity != expected_identity:
        raise ValueError("portfolio paper replay identity changed between scenarios")
    return definition


def _strategy_ids(definition: PortfolioDefinition) -> tuple[str, ...]:
    return tuple(
        sleeve.strategy.strategy_id for sleeve in definition.sleeves
    )


def _scenario_quantities(
    definition: PortfolioDefinition,
    supplied: Mapping[str, Decimal] | None,
) -> dict[str, Decimal]:
    strategy_ids = set(_strategy_ids(definition))
    quantities = (
        {strategy_id: Decimal("1") for strategy_id in strategy_ids}
        if supplied is None
        else {
            strategy_id: Decimal(str(quantity))
            for strategy_id, quantity in supplied.items()
        }
    )
    if set(quantities) != strategy_ids:
        raise ValueError(
            "portfolio paper scenario quantities must name every sleeve exactly"
        )
    for strategy_id, quantity in quantities.items():
        if (
            not quantity.is_finite()
            or quantity <= 0
            or quantity != quantity.to_integral_value()
        ):
            raise ValueError(
                "portfolio paper scenario quantity must be a positive integer: "
                f"{strategy_id}"
            )
    _require_scenario_capacity(definition, quantities)
    return quantities


def _require_scenario_capacity(
    definition: PortfolioDefinition,
    quantities: Mapping[str, Decimal],
) -> None:
    total_quantity = sum(quantities.values(), Decimal("0"))
    if definition.max_gross_quantity < total_quantity:
        raise ValueError(
            "portfolio paper lifecycle requires max_gross_quantity to cover "
            f"the {total_quantity}-contract scenario"
        )
