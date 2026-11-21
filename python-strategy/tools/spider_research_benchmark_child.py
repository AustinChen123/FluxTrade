"""Run one no-store research replay and print only bounded benchmark metadata."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

from src.core.backtest.spider_historical_research_entry import run_historical_research
from src.core.backtest.spider_historical_research_output import (
    ResearchFailureResult,
    ResearchRunResult,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes


def summarize(result: ResearchRunResult | ResearchFailureResult) -> dict[str, object]:
    if isinstance(result, ResearchFailureResult):
        return {
            "outcome": result.outcome,
            "research_classification": result.research_classification,
            "admission_state": result.admission_state,
            "failure_classification": result.failure_classification,
            "reason": result.reason,
            "primary": result.primary,
            "last_processed_boundary": result.last_processed_boundary,
            "last_fully_recorded_row_ordinal": result.last_fully_recorded_row_ordinal,
        }
    return {
        "outcome": result.outcome,
        "research_classification": result.research_classification,
        "admission_state": result.admission_state,
        "report_row_count": len(result.report_rows),
        "report_sha256": sha256(canonical_bytes(list(result.report_rows))).hexdigest(),
        "final_owner_evidence_sha256": sha256(
            canonical_bytes(result.final_owner_evidence)
        ).hexdigest(),
        "scheduler_observation_sha256": sha256(
            canonical_bytes(result.final_scheduler_observation)
        ).hexdigest(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    result = run_historical_research(raw)
    print(json.dumps(summarize(result), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
