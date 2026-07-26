from __future__ import annotations

import argparse
import json

from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import a validated, immutable research candle dataset."
    )
    parser.add_argument("csv_path")
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--timeframe", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument(
        "--timestamp-format",
        choices=("epoch_milliseconds", "epoch_seconds", "iso8601"),
        default="epoch_milliseconds",
    )
    parser.add_argument("--roll-policy")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = ResearchDatasetImporter().import_csv(
        args.csv_path,
        ResearchDatasetSpec(
            dataset_id=args.dataset_id,
            product_id=args.product_id,
            timeframe=args.timeframe,
            source=args.source,
            revision=args.revision,
            timestamp_format=args.timestamp_format,
            roll_policy=args.roll_policy,
        ),
    )
    print(
        json.dumps(
            {
                "dataset_id": result.dataset_id,
                "row_count": result.row_count,
                "checksum_sha256": result.checksum_sha256,
                "already_present": result.already_present,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
