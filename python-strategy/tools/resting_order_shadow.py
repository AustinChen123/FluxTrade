import argparse
import json
import sys

from src.core.adapters.okx_public_candles import default_http_get, fetch_closed_1m
from src.core.resting_order_shadow import replay_shadow, run_shadow


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run or replay a public resting-order shadow simulation"
    )
    parser.add_argument("--pack", required=True)
    parser.add_argument("--replay")
    parser.add_argument("--id")
    parser.add_argument("--params")
    parser.add_argument("--inst")
    parser.add_argument("--maker-fee", type=float)
    parser.add_argument("--taker-fee", type=float)
    parser.add_argument("--start-ms", type=int)
    parser.add_argument("--minutes", type=int)
    parser.add_argument("--out")
    args = parser.parse_args()
    try:
        if args.replay:
            code, message = replay_shadow(replay=args.replay, pack=args.pack)
            print(message, file=sys.stderr if code else sys.stdout)
            return code
        required = (
            args.id,
            args.params,
            args.inst,
            args.maker_fee,
            args.taker_fee,
            args.start_ms,
            args.minutes,
            args.out,
        )
        if any(value is None for value in required):
            parser.error(
                "live mode requires --id --params --inst --maker-fee --taker-fee --start-ms --minutes --out"
            )
        with open(args.params, encoding="utf-8") as stream:
            params = json.load(stream)
        run_shadow(
            pack=args.pack,
            catalog_id=args.id,
            params=params,
            inst=args.inst,
            maker_fee=args.maker_fee,
            taker_fee=args.taker_fee,
            start_ms=args.start_ms,
            minutes=args.minutes,
            out=args.out,
            fetch_window=lambda inst, start, end: fetch_closed_1m(
                inst, start, end, default_http_get
            ),
        )
        return 0
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
