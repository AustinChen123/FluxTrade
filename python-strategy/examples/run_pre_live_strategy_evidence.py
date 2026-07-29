from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]
load_dotenv(ROOT.parent / ".env")

from src.core.redis_factory import create_redis_client  # noqa: E402
from src.core.product_registry import to_stream_key  # noqa: E402
from src.validation.paper_lifecycle import run_paper_lifecycle  # noqa: E402
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
    else:
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
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
