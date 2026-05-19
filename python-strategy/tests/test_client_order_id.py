import pytest

from src.core.client_order_id import (
    MAX_BINANCE_LENGTH,
    generate_client_order_id,
    is_valid_client_order_id,
    parse_client_order_id,
    to_exchange_format,
)


def test_generate_client_order_id_uses_canonical_format() -> None:
    coid = generate_client_order_id(
        "strategy_1",
        "worker_a",
        "entry",
        clock_ns=lambda: 1704067200000000000,
    )

    parts = coid.split("-")
    assert parts[:3] == ["strategy_1", "worker_a", "entry"]
    assert parts[3].isdigit()
    assert int(parts[3]) >= 1704067200000000000
    assert parse_client_order_id(coid).strategy_id == "strategy_1"
    assert parse_client_order_id(coid).instance_id == "worker_a"
    assert parse_client_order_id(coid).action == "entry"
    assert parse_client_order_id(coid).ts_ns >= 1704067200000000000


def test_generate_client_order_id_is_unique_for_repeated_clock_values() -> None:
    coids = {
        generate_client_order_id(
            "strategy_1",
            "worker_a",
            "entry",
            clock_ns=lambda: 1704067200000000000,
        )
        for _ in range(100)
    }

    assert len(coids) == 100
    assert len({parse_client_order_id(coid).ts_ns for coid in coids}) == 100


@pytest.mark.parametrize(
    "coid",
    [
        "",
        "missing-parts",
        "strategy-worker-entry-not_numeric",
        "strategy-worker--1704067200000000000",
        "strategy-worker-entry-1704067200000000000-extra",
    ],
)
def test_invalid_client_order_id_rejected(coid: str) -> None:
    assert is_valid_client_order_id(coid) is False
    with pytest.raises(ValueError):
        parse_client_order_id(coid)


def test_binance_exchange_format_is_deterministic_and_within_limit() -> None:
    coid = generate_client_order_id(
        "strategy_1",
        "worker_a",
        "entry",
        clock_ns=lambda: 1704067200000000000,
    )

    exchange_id = to_exchange_format(coid, "binance")

    assert exchange_id == to_exchange_format(coid, "BINANCE")
    assert len(exchange_id) <= MAX_BINANCE_LENGTH
    assert exchange_id.startswith("strategy-")


def test_binance_exchange_format_avoids_collisions_for_many_strategies() -> None:
    exchange_ids = {
        to_exchange_format(
            generate_client_order_id(
                f"strategy_{idx}",
                "worker_a",
                "entry",
                clock_ns=lambda idx=idx: 1704067200000000000 + idx,
            ),
            "binance",
        )
        for idx in range(1000)
    }

    assert len(exchange_ids) == 1000


def test_other_exchange_uses_canonical_when_safe() -> None:
    coid = generate_client_order_id(
        "strategy_1",
        "worker_a",
        "entry",
        clock_ns=lambda: 1704067200000000000,
    )

    assert to_exchange_format(coid, "kraken") == coid
