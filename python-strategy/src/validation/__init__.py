"""Operational evidence helpers kept outside the trading runtime."""

from src.validation.strategy_evidence import (
    HistoricalParityReport,
    ShadowRunReport,
    StrategyEvidenceIdentity,
    load_portfolio,
    load_strategy,
    portfolio_evidence_identity,
    require_verified_portfolio_identity,
    require_verified_strategy_identity,
    run_portfolio_shadow_evidence,
    run_shadow_evidence,
    strategy_evidence_identity,
    verify_historical_stream_parity,
    verify_portfolio_historical_stream_parity,
    verify_portfolio_shadow_evidence_bundle,
    verify_shadow_evidence_bundle,
)
from src.validation.paper_lifecycle import (
    PaperFillEvidence,
    PaperInstrumentEvidence,
    PaperLifecycleReport,
    PaperOrderEvidence,
    PaperScenarioReport,
    run_paper_lifecycle,
)
from src.validation.portfolio_paper_lifecycle import (
    run_portfolio_paper_lifecycle,
)

__all__ = [
    "HistoricalParityReport",
    "PaperFillEvidence",
    "PaperInstrumentEvidence",
    "PaperLifecycleReport",
    "PaperOrderEvidence",
    "PaperScenarioReport",
    "ShadowRunReport",
    "StrategyEvidenceIdentity",
    "load_portfolio",
    "load_strategy",
    "portfolio_evidence_identity",
    "require_verified_portfolio_identity",
    "require_verified_strategy_identity",
    "run_paper_lifecycle",
    "run_portfolio_paper_lifecycle",
    "run_portfolio_shadow_evidence",
    "run_shadow_evidence",
    "strategy_evidence_identity",
    "verify_historical_stream_parity",
    "verify_portfolio_historical_stream_parity",
    "verify_portfolio_shadow_evidence_bundle",
    "verify_shadow_evidence_bundle",
]
