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
    load_strategy,
    run_shadow_evidence,
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

    shadow = subparsers.add_parser("shadow")
    _add_strategy_arguments(shadow)
    shadow.add_argument("--duration-seconds", type=float, required=True)
    shadow.add_argument("--output", type=Path, required=True)
    shadow.add_argument("--env-file", type=Path)

    shadow_verify = subparsers.add_parser("verify-shadow")
    _add_strategy_arguments(shadow_verify)
    shadow_verify.add_argument("--bundle", type=Path, required=True)

    paper = subparsers.add_parser("paper-lifecycle")
    _add_strategy_arguments(paper)
    paper.add_argument("--workspace", type=Path, required=True)
    return parser


def _add_strategy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--strategy-dir", type=Path, required=True)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--product-id", required=True)


def main() -> None:
    args = _parser().parse_args()
    factory = lambda: load_strategy(  # noqa: E731
        args.strategy_dir,
        args.strategy_id,
        args.product_id,
    )
    if args.command == "historical-parity":
        report = verify_historical_stream_parity(
            args.source_1m,
            args.reference_5m,
            product_id=args.product_id,
            strategy_factory=factory,
        )
    elif args.command == "shadow":
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
    elif args.command == "verify-shadow":
        with args.bundle.open(encoding="utf-8") as bundle:
            report = verify_shadow_evidence_bundle(
                bundle,
                strategy_factory=factory,
            )
    else:
        report = run_paper_lifecycle(
            args.workspace,
            product_id=args.product_id,
            strategy_id=args.strategy_id,
            hard_flat_strategy_factory=factory,
        )
    print(json.dumps(report.to_dict(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
