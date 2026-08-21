from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.core.adapters.rithmic_ledger_position import (
    project_rithmic_ledger_positions,
)
from src.core.interfaces.exchange import ExchangeError
from src.core.models import PositionSide


ACCOUNT_ID = "ACCOUNT"
NQ_PRODUCT = "RITHMIC:NQ-202609"
ES_PRODUCT = "RITHMIC:ES-202609"
PRODUCTS = {
    ("CME", "NQU6"): NQ_PRODUCT,
    ("CME", "ESU6"): ES_PRODUCT,
}


def position(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "exchange": "CME",
        "symbol": "NQU6",
        "net_quantity": "1",
        "average_open_fill_price": "20000.2500",
        "open_pnl": "10.5000",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def snapshot(
    positions: list[SimpleNamespace],
    *,
    account_id: object = ACCOUNT_ID,
) -> SimpleNamespace:
    return SimpleNamespace(account_id=account_id, positions=positions)


def project(snapshot_value: SimpleNamespace):
    return project_rithmic_ledger_positions(
        snapshot_value,
        account_id=ACCOUNT_ID,
        products_by_native_identity=PRODUCTS,
    )


def test_projects_signed_positions_in_source_order_with_exact_decimals():
    actual = project(
        snapshot(
            [
                position(
                    exchange=" cme ",
                    symbol=" nqu6 ",
                    net_quantity="1.234567890123456789",
                ),
                position(
                    symbol="ESU6",
                    net_quantity="-2.50",
                    average_open_fill_price=None,
                    open_pnl=None,
                ),
            ],
            account_id=" ACCOUNT ",
        )
    )

    assert [item.product_id for item in actual] == [NQ_PRODUCT, ES_PRODUCT]
    assert [item.strategy_id for item in actual] == ["LIVE", "LIVE"]
    assert [item.side for item in actual] == [PositionSide.LONG, PositionSide.SHORT]
    assert actual[0].quantity.as_tuple() == Decimal("1.234567890123456789").as_tuple()
    assert actual[1].quantity.as_tuple() == Decimal("2.50").as_tuple()
    assert actual[0].entry_price.as_tuple() == Decimal("20000.2500").as_tuple()
    assert actual[0].unrealized_pnl.as_tuple() == Decimal("10.5000").as_tuple()
    assert actual[1].entry_price == Decimal("0")
    assert actual[1].unrealized_pnl == Decimal("0")


@pytest.mark.parametrize("net_quantity", ["0", "-0", "0.000"])
def test_zero_is_omitted_before_mapping_and_value_validation(net_quantity: str):
    before = dict(PRODUCTS)

    assert (
        project(
            snapshot(
                [
                    position(
                        exchange="UNKNOWN",
                        symbol="UNKNOWN",
                        net_quantity=net_quantity,
                        average_open_fill_price="bad-price",
                        open_pnl="bad-pnl",
                    )
                ]
            )
        )
        == []
    )
    assert PRODUCTS == before


@pytest.mark.parametrize(
    ("snapshot_value", "message"),
    [
        (
            snapshot(
                [position(net_quantity="bad")],
                account_id="OTHER",
            ),
            "rithmic_ledger_account_id_mismatch",
        ),
        (
            snapshot(
                [position(exchange="UNKNOWN", symbol="UNKNOWN", net_quantity="bad")]
            ),
            "rithmic_ledger_position_value_invalid: exchange=UNKNOWN symbol=UNKNOWN",
        ),
        (
            snapshot(
                [
                    position(
                        exchange="UNKNOWN",
                        symbol="UNKNOWN",
                        average_open_fill_price="bad-price",
                        open_pnl="bad-pnl",
                    )
                ]
            ),
            "rithmic_ledger_position_instrument_unmapped: "
            "exchange=UNKNOWN symbol=UNKNOWN",
        ),
    ],
)
def test_compound_failures_keep_exact_precedence(
    snapshot_value: SimpleNamespace,
    message: str,
):
    before = dict(PRODUCTS)

    with pytest.raises(ExchangeError) as exc_info:
        project(snapshot_value)

    assert str(exc_info.value) == message
    assert PRODUCTS == before


@pytest.mark.parametrize(
    "remote",
    [
        position(net_quantity="NaN"),
        position(net_quantity="Infinity"),
        position(average_open_fill_price="NaN"),
        position(open_pnl="-Infinity"),
        position(average_open_fill_price="not-a-number"),
        position(open_pnl="not-a-number"),
    ],
)
def test_invalid_or_nonfinite_position_values_fail_without_mapping_mutation(
    remote: SimpleNamespace,
):
    before = dict(PRODUCTS)

    with pytest.raises(ExchangeError, match="rithmic_ledger_position_value_invalid"):
        project(snapshot([remote]))

    assert PRODUCTS == before


def test_success_does_not_mutate_native_identity_mapping():
    before = dict(PRODUCTS)

    project(snapshot([position()]))

    assert PRODUCTS == before
