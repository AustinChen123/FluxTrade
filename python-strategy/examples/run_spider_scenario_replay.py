"""Transport-only command for a bounded, locally admitted Spider scenario."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--scenario-selector", required=True)
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument("--historical-input")
    input_group.add_argument("--execution-input")
    input_group.add_argument("--floating-exit-input")
    input_group.add_argument("--gross-execution-input")
    args = parser.parse_args(argv)
    try:
        from src.core.backtest.spider_scenario_run import run_spider_scenario

        historical_input = None
        execution_input = None
        floating_exit_input = None
        gross_execution_input = None
        if args.historical_input is not None:
            try:
                historical_input = Path(args.historical_input).read_bytes()
            except OSError:
                result = run_spider_scenario(args.output_root, args.run_id, args.scenario_selector, b"")
            else:
                result = run_spider_scenario(args.output_root, args.run_id, args.scenario_selector, historical_input)
        elif args.execution_input is not None:
            try:
                execution_input = Path(args.execution_input).read_bytes()
            except OSError:
                execution_input = b""
            result = run_spider_scenario(
                args.output_root, args.run_id, args.scenario_selector,
                execution_input=execution_input,
            )
        elif args.floating_exit_input is not None:
            try:
                floating_exit_input = Path(args.floating_exit_input).read_bytes()
            except OSError:
                floating_exit_input = b""
            result = run_spider_scenario(
                args.output_root, args.run_id, args.scenario_selector,
                floating_exit_input=floating_exit_input,
            )
        elif args.gross_execution_input is not None:
            try:
                gross_execution_input = Path(args.gross_execution_input).read_bytes()
            except OSError:
                gross_execution_input = b""
            result = run_spider_scenario(
                args.output_root, args.run_id, args.scenario_selector,
                gross_execution_input=gross_execution_input,
            )
        else:
            result = run_spider_scenario(args.output_root, args.run_id, args.scenario_selector)
    except Exception as error:
        invalid = type(error) is ValueError and error.args == ("INVALID_INVOCATION",)
        sys.stderr.write("INVALID_INVOCATION\n" if invalid else "INTERNAL_FAILURE\n")
        return 2 if invalid else 3
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    if (args.execution_input is not None or args.floating_exit_input is not None
            or args.gross_execution_input is not None):
        if result["outcome"] == "REJECTED":
            return 2
        if result["outcome"] != "ADMITTED":
            return 3
        return 1 if result["endpoint_status"] == "INCOMPLETE" else (
            0 if result["endpoint_status"] in (
                "PAID_FLAT", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT",
            ) else 3
        )
    return {"ADMITTED": 0, "REJECTED": 2, "FAILED": 3, "DURABILITY_UNKNOWN": 3}[result["outcome"]]


if __name__ == "__main__":
    raise SystemExit(main())
