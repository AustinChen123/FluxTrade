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
    args = parser.parse_args(argv)
    try:
        from src.core.backtest.spider_scenario_run import run_spider_scenario

        result = run_spider_scenario(args.output_root, args.run_id, args.scenario_selector)
    except Exception as error:
        invalid = type(error) is ValueError and error.args == ("INVALID_INVOCATION",)
        sys.stderr.write("INVALID_INVOCATION\n" if invalid else "INTERNAL_FAILURE\n")
        return 2 if invalid else 3
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return {"ADMITTED": 0, "REJECTED": 2, "FAILED": 3, "DURABILITY_UNKNOWN": 3}[result["outcome"]]


if __name__ == "__main__":
    raise SystemExit(main())
