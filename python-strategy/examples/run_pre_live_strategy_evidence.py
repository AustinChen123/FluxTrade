from __future__ import annotations

import argparse
import json
import sys
from collections import deque
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]
load_dotenv(ROOT.parent / ".env")

from src.core.redis_factory import create_redis_client  # noqa: E402
from src.core.product_registry import to_stream_key  # noqa: E402
from src.validation.paper_lifecycle import run_paper_lifecycle  # noqa: E402
from src.validation.portfolio_paper_lifecycle import (  # noqa: E402
    run_portfolio_paper_lifecycle,
)
from src.validation.portfolio_paper_forward import (  # noqa: E402
    run_portfolio_paper_forward,
    validate_portfolio_paper_forward_run,
)
from src.core.data_sources.csv_source import CsvDataSource  # noqa: E402
from src.validation.strategy_evidence import (  # noqa: E402
    load_portfolio,
    load_strategy,
    run_portfolio_shadow_evidence,
    run_shadow_evidence,
    verify_portfolio_historical_stream_parity,
    verify_portfolio_shadow_evidence_bundle,
    verify_shadow_evidence_bundle,
    verify_historical_stream_parity,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect strategy evidence without authorizing live orders."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parity = subparsers.add_parser("historical-parity")
    _add_strategy_arguments(parity)
    parity.add_argument("--source-1m", type=Path, required=True)
    parity.add_argument("--reference-5m", type=Path, required=True)

    portfolio_parity = subparsers.add_parser("portfolio-historical-parity")
    _add_portfolio_arguments(portfolio_parity)
    portfolio_parity.add_argument("--source-1m", type=Path, required=True)
    portfolio_parity.add_argument("--reference-5m", type=Path, required=True)

    shadow = subparsers.add_parser("shadow")
    _add_strategy_arguments(shadow)
    shadow.add_argument("--duration-seconds", type=float, required=True)
    shadow.add_argument("--output", type=Path, required=True)
    shadow.add_argument("--env-file", type=Path)

    shadow_verify = subparsers.add_parser("verify-shadow")
    _add_strategy_arguments(shadow_verify)
    shadow_verify.add_argument("--bundle", type=Path, required=True)

    portfolio_shadow = subparsers.add_parser("portfolio-shadow")
    _add_portfolio_arguments(portfolio_shadow)
    portfolio_shadow.add_argument("--duration-seconds", type=float, required=True)
    portfolio_shadow.add_argument("--output", type=Path, required=True)
    portfolio_shadow.add_argument("--env-file", type=Path)

    portfolio_shadow_verify = subparsers.add_parser("verify-portfolio-shadow")
    _add_portfolio_arguments(portfolio_shadow_verify)
    portfolio_shadow_verify.add_argument("--bundle", type=Path, required=True)

    paper = subparsers.add_parser("paper-lifecycle")
    _add_strategy_arguments(paper)
    paper.add_argument("--workspace", type=Path, required=True)

    portfolio_paper = subparsers.add_parser("portfolio-paper-lifecycle")
    _add_portfolio_arguments(portfolio_paper)
    portfolio_paper.add_argument("--workspace", type=Path, required=True)
    portfolio_paper.add_argument("--scenario-quantities", type=Path)

    paper_forward = subparsers.add_parser("portfolio-paper-forward")
    _add_portfolio_arguments(paper_forward)
    paper_forward.add_argument("--workspace", type=Path, required=True)
    paper_forward.add_argument("--warmup-5m", type=Path, required=True)
    paper_forward.add_argument("--duration-seconds", type=float, required=True)
    paper_forward.add_argument("--run-id", required=True)
    paper_forward.add_argument("--output", type=Path, required=True)
    return parser


def _add_strategy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--strategy-dir", type=Path, required=True)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--product-id", required=True)


def _add_portfolio_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--strategy-dir", type=Path, required=True)
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--portfolio-config", type=Path, required=True)


def _load_portfolio_config(path: Path) -> dict[str, object]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("portfolio config must be a JSON object")
    return parsed


def _load_scenario_quantities(path: Path | None) -> dict[str, Decimal] | None:
    if path is None:
        return None
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("scenario quantities must be a JSON object")
    return {
        str(strategy_id): Decimal(str(quantity))
        for strategy_id, quantity in parsed.items()
    }


def main() -> None:
    args = _parser().parse_args()
    if args.command == "historical-parity":
        factory = lambda: load_strategy(  # noqa: E731
            args.strategy_dir,
            args.strategy_id,
            args.product_id,
        )
        report = verify_historical_stream_parity(
            args.source_1m,
            args.reference_5m,
            product_id=args.product_id,
            strategy_factory=factory,
        )
    elif args.command == "portfolio-historical-parity":
        portfolio_factory = lambda: load_portfolio(  # noqa: E731
            args.strategy_dir,
            args.portfolio_id,
            args.product_id,
            config=_load_portfolio_config(args.portfolio_config),
        )
        report = verify_portfolio_historical_stream_parity(
            args.source_1m,
            args.reference_5m,
            product_id=args.product_id,
            portfolio_factory=portfolio_factory,
        )
    elif args.command == "shadow":
        factory = lambda: load_strategy(  # noqa: E731
            args.strategy_dir,
            args.strategy_id,
            args.product_id,
        )
        if args.env_file is not None:
            load_dotenv(args.env_file, override=True)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as output:
            report = run_shadow_evidence(
                create_redis_client(),
                source_stream_key=to_stream_key(args.product_id, "1m"),
                decision_stream_key=to_stream_key(args.product_id, "5m"),
                strategy=factory(),
                output=output,
                duration_seconds=args.duration_seconds,
            )
    elif args.command == "portfolio-shadow":
        if args.env_file is not None:
            load_dotenv(args.env_file, override=True)
        portfolio = load_portfolio(
            args.strategy_dir,
            args.portfolio_id,
            args.product_id,
            config=_load_portfolio_config(args.portfolio_config),
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as output:
            report = run_portfolio_shadow_evidence(
                create_redis_client(),
                source_stream_key=to_stream_key(args.product_id, "1m"),
                decision_stream_key=to_stream_key(args.product_id, "5m"),
                portfolio=portfolio,
                output=output,
                duration_seconds=args.duration_seconds,
            )
    elif args.command == "verify-shadow":
        factory = lambda: load_strategy(  # noqa: E731
            args.strategy_dir,
            args.strategy_id,
            args.product_id,
        )
        with args.bundle.open(encoding="utf-8") as bundle:
            report = verify_shadow_evidence_bundle(
                bundle,
                strategy_factory=factory,
            )
    elif args.command == "verify-portfolio-shadow":
        portfolio_factory = lambda: load_portfolio(  # noqa: E731
            args.strategy_dir,
            args.portfolio_id,
            args.product_id,
            config=_load_portfolio_config(args.portfolio_config),
        )
        with args.bundle.open(encoding="utf-8") as bundle:
            report = verify_portfolio_shadow_evidence_bundle(
                bundle,
                portfolio_factory=portfolio_factory,
            )
    elif args.command == "paper-lifecycle":
        factory = lambda: load_strategy(  # noqa: E731
            args.strategy_dir,
            args.strategy_id,
            args.product_id,
        )
        report = run_paper_lifecycle(
            args.workspace,
            product_id=args.product_id,
            strategy_id=args.strategy_id,
            hard_flat_strategy_factory=factory,
        )
    elif args.command == "portfolio-paper-lifecycle":
        portfolio_factory = lambda: load_portfolio(  # noqa: E731
            args.strategy_dir,
            args.portfolio_id,
            args.product_id,
            config=_load_portfolio_config(args.portfolio_config),
        )
        report = run_portfolio_paper_lifecycle(
            args.workspace,
            portfolio_factory=portfolio_factory,
            scenario_quantities=_load_scenario_quantities(
                args.scenario_quantities
            ),
        )
    else:
        portfolio_factory = lambda: load_portfolio(  # noqa: E731
            args.strategy_dir,
            args.portfolio_id,
            args.product_id,
            config=_load_portfolio_config(args.portfolio_config),
        )
        source = CsvDataSource(
            str(args.warmup_5m),
            product_id=args.product_id,
            timeframe="5m",
        )
        available = source.get_available_range(args.product_id, "5m")
        if available is None:
            raise ValueError("paper-forward warmup source is empty")
        baseline = portfolio_factory()
        required = max(
            int(sleeve.strategy.requirements.lookback_window)
            for sleeve in baseline.sleeves
        )
        required = max(required, 1)
        warmup = list(
            deque(
                source.get_candles(
                    args.product_id,
                    "5m",
                    available[0],
                    available[1],
                ),
                maxlen=required,
            )
        )
        validate_portfolio_paper_forward_run(
            args.workspace,
            run_id=args.run_id,
            definition=baseline,
            warmup_candles=warmup,
            duration_seconds=args.duration_seconds,
        )
        workspace_path = args.workspace.resolve()
        output_path = args.output.resolve()
        if output_path == workspace_path or workspace_path in output_path.parents:
            raise ValueError(
                "paper-forward output must be outside its managed workspace"
            )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as output:
            report = run_portfolio_paper_forward(
                args.workspace,
                run_id=args.run_id,
                portfolio_factory=lambda: baseline,
                warmup_candles=warmup,
                output=output,
                duration_seconds=args.duration_seconds,
            )
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
