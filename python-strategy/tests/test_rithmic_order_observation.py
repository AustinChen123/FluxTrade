from decimal import Decimal, InvalidOperation
from types import SimpleNamespace

import pytest

from src.core.adapters.rithmic_order_observation import (
    RithmicUnmappedOrderEvent,
    project_rithmic_order_event,
    project_rithmic_order_snapshot,
    resolve_rithmic_order_event_identity,
)
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
)


ACCOUNT_ID = "ACCOUNT-A"
PRODUCT_ID = "RITHMIC:NQ-202609"
NATIVE_IDENTITY = ("CME", "NQU6")


def snapshot(**overrides):
    values = {
        "client_order_id": "client-1",
        "basket_id": "basket-1",
        "exchange_order_id": "exchange-1",
        "status": "OPEN",
        "notification_type": "STATUS",
        "quantity": "2",
        "filled_quantity": "1",
        "average_fill_price": "20000.25",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def event(**overrides):
    values = {
        "account_id": "ACCOUNT-B",
        "client_order_id": "provider-parent-client-1",
        "basket_id": "basket-1",
        "original_basket_id": "parent-1",
        "linked_basket_ids": "stop-1,target-1",
        "exchange_order_id": "exchange-1",
        "exchange": "cme",
        "symbol": "nqu6",
        "status": "partially_filled",
        "price": "20000.25",
        "trigger_price": "19998.25",
        "price_type": "stop_market",
        "bracket_type": "target_and_stop_static",
        "notification_type": "STATUS",
        "cumulative_filled_quantity": "1.12345678901234567890123456789",
        "cumulative_average_price": "20000.2500000000000000000000001",
        "last_fill_quantity": "0.12345678901234567890123456789",
        "last_fill_price": "20000.1250000000000000000000001",
        "timestamp_ms": 1_700_000_000_000,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.parametrize(
    ("status", "filled", "expected"),
    [
        ("OPEN", "0", "open"),
        ("OPEN", "1", "partially_filled"),
        ("OPEN", "2", "partially_filled"),
        ("OPEN_PENDING", "0", "open"),
        ("OPEN_PENDING", "1", "partially_filled"),
        ("OPEN_PENDING", "2", "partially_filled"),
        ("NEW", "0", "open"),
        ("NEW", "1", "partially_filled"),
        ("NEW", "2", "partially_filled"),
        ("SUBMITTED", "0", "open"),
        ("SUBMITTED", "1", "partially_filled"),
        ("SUBMITTED", "2", "partially_filled"),
        ("ACCEPTED", "0", "open"),
        ("ACCEPTED", "1", "partially_filled"),
        ("ACCEPTED", "2", "partially_filled"),
        ("open pending", "1", "partially_filled"),
        ("open-pending", "1", "partially_filled"),
        ("PARTIAL", "1", "partially_filled"),
        ("PARTIALLY_FILLED", "1", "partially_filled"),
        ("PARTIALLYFILLED", "1", "partially_filled"),
        ("partially filled", "1", "partially_filled"),
        ("COMPLETE", "2", "filled"),
        ("COMPLETED", "2", "filled"),
        ("FILLED", "2", "filled"),
        ("CANCEL", "0", "cancelled"),
        ("CANCEL", "1", "cancelled"),
        ("CANCEL", "2", "cancelled"),
        ("CANCELED", "0", "cancelled"),
        ("CANCELED", "1", "cancelled"),
        ("CANCELED", "2", "cancelled"),
        ("CANCELLED", "0", "cancelled"),
        ("CANCELLED", "1", "cancelled"),
        ("CANCELLED", "2", "cancelled"),
        ("REJECT", "0", "rejected"),
        ("REJECT", "1", "rejected"),
        ("REJECT", "2", "rejected"),
        ("REJECTED", "0", "rejected"),
        ("REJECTED", "1", "rejected"),
        ("REJECTED", "2", "rejected"),
        ("FAILED", "0", "rejected"),
        ("FAILED", "1", "rejected"),
        ("FAILED", "2", "rejected"),
        ("EXPIRED", "0", "rejected"),
        ("EXPIRED", "1", "rejected"),
        ("EXPIRED", "2", "rejected"),
        ("cancelled", "1", "cancelled"),
        ("rejected", "1", "rejected"),
    ],
)
def test_snapshot_status_alias_ledger_is_literal(status, filled, expected):
    projected = project_rithmic_order_snapshot(
        snapshot(status=status, filled_quantity=filled),
        account_id=ACCOUNT_ID,
    )

    assert projected.status == expected


@pytest.mark.parametrize(
    ("status", "filled", "message"),
    [
        ("PARTIAL", "0", "unsupported_rithmic_order_snapshot_status"),
        ("PARTIAL", "2", "unsupported_rithmic_order_snapshot_status"),
        ("PARTIALLY_FILLED", "0", "unsupported_rithmic_order_snapshot_status"),
        ("PARTIALLY_FILLED", "2", "unsupported_rithmic_order_snapshot_status"),
        ("PARTIALLYFILLED", "0", "unsupported_rithmic_order_snapshot_status"),
        ("PARTIALLYFILLED", "2", "unsupported_rithmic_order_snapshot_status"),
        ("COMPLETE", "0", "unsupported_rithmic_order_snapshot_status"),
        ("COMPLETE", "1", "unsupported_rithmic_order_snapshot_status"),
        ("COMPLETED", "0", "unsupported_rithmic_order_snapshot_status"),
        ("COMPLETED", "1", "unsupported_rithmic_order_snapshot_status"),
        ("FILLED", "0", "unsupported_rithmic_order_snapshot_status"),
        ("FILLED", "1", "unsupported_rithmic_order_snapshot_status"),
        ("UNKNOWN", "0", "unsupported_rithmic_order_snapshot_status"),
        ("UNKNOWN", "1", "unsupported_rithmic_order_snapshot_status"),
        ("UNKNOWN", "2", "unsupported_rithmic_order_snapshot_status"),
        ("OPEN", "-1", "invalid_rithmic_order_snapshot_quantities"),
        ("OPEN", "3", "invalid_rithmic_order_snapshot_quantities"),
    ],
)
def test_snapshot_status_and_fill_rejections_are_exact(status, filled, message):
    with pytest.raises(ExchangeError, match=message):
        project_rithmic_order_snapshot(
            snapshot(status=status, filled_quantity=filled),
            account_id=ACCOUNT_ID,
        )


@pytest.mark.parametrize("quantity", ["0", "-1"])
def test_snapshot_nonpositive_quantity_is_rejected(quantity):
    with pytest.raises(
        ExchangeError,
        match="invalid_rithmic_order_snapshot_quantities",
    ):
        project_rithmic_order_snapshot(
            snapshot(quantity=quantity),
            account_id=ACCOUNT_ID,
        )


@pytest.mark.parametrize("basket_id", [None, "", "   "])
def test_snapshot_requires_a_usable_basket_identity(basket_id):
    with pytest.raises(
        ExchangeError,
        match="rithmic_order_snapshot_basket_id_required",
    ):
        project_rithmic_order_snapshot(
            snapshot(basket_id=basket_id),
            account_id=ACCOUNT_ID,
        )


@pytest.mark.parametrize(
    ("notification", "filled", "expected", "message"),
    [
        (None, "1", "partially_filled", None),
        ("STATUS", "1", "partially_filled", None),
        ("CANCEL", "0", "cancelled", None),
        ("CANCEL", "1", "cancelled", None),
        ("CANCEL", "2", None, "invalid_rithmic_cancel_snapshot_quantities"),
        ("REJECT", "0", "rejected", None),
        ("REJECT", "1", "rejected", None),
        ("REJECT", "2", None, "invalid_rithmic_reject_snapshot_quantities"),
        (" cancel ", "1", "cancelled", None),
        (" reject ", "1", "rejected", None),
    ],
)
def test_snapshot_notification_precedence_is_literal(
    notification,
    filled,
    expected,
    message,
):
    remote = snapshot(
        status="UNKNOWN",
        notification_type=notification,
        filled_quantity=filled,
    )
    if message is not None:
        with pytest.raises(ExchangeError, match=message):
            project_rithmic_order_snapshot(remote, account_id=ACCOUNT_ID)
        return
    if notification in {None, "STATUS"}:
        remote.status = "OPEN"

    assert (
        project_rithmic_order_snapshot(remote, account_id=ACCOUNT_ID).status == expected
    )


@pytest.mark.parametrize(
    ("overrides", "error_type", "message"),
    [
        ({"quantity": "not-decimal", "filled_quantity": "bad"}, InvalidOperation, None),
        (
            {"filled_quantity": "not-decimal", "status": "UNKNOWN"},
            InvalidOperation,
            None,
        ),
        (
            {
                "filled_quantity": "3",
                "status": "UNKNOWN",
                "notification_type": "CANCEL",
            },
            ExchangeError,
            "invalid_rithmic_order_snapshot_quantities",
        ),
        (
            {
                "filled_quantity": "2",
                "status": "UNKNOWN",
                "notification_type": "CANCEL",
            },
            ExchangeError,
            "invalid_rithmic_cancel_snapshot_quantities",
        ),
        (
            {
                "quantity": "0",
                "filled_quantity": "not-decimal",
                "notification_type": "REJECT",
            },
            InvalidOperation,
            None,
        ),
        (
            {
                "filled_quantity": "-1",
                "status": "UNKNOWN",
                "notification_type": "REJECT",
            },
            ExchangeError,
            "invalid_rithmic_order_snapshot_quantities",
        ),
        (
            {
                "filled_quantity": "3",
                "status": "UNKNOWN",
                "notification_type": "REJECT",
            },
            ExchangeError,
            "invalid_rithmic_order_snapshot_quantities",
        ),
        (
            {
                "quantity": "0",
                "filled_quantity": "0",
                "status": "UNKNOWN",
                "notification_type": "REJECT",
            },
            ExchangeError,
            "invalid_rithmic_order_snapshot_quantities",
        ),
        (
            {"status": "UNKNOWN", "average_fill_price": "not-decimal"},
            ExchangeError,
            "unsupported_rithmic_order_snapshot_status",
        ),
        ({"average_fill_price": "not-decimal"}, InvalidOperation, None),
    ],
)
def test_snapshot_compound_failure_precedence(overrides, error_type, message):
    with pytest.raises(error_type, match=message):
        project_rithmic_order_snapshot(
            snapshot(**overrides),
            account_id=ACCOUNT_ID,
        )


def test_snapshot_uses_configured_account_and_preserves_raw_provider_values():
    projected = project_rithmic_order_snapshot(
        snapshot(
            exchange_order_id=123,
            quantity="2.00000000000000000000000000000",
            filled_quantity="1.12345678901234567890123456789",
            average_fill_price="20000.2500000000000000000000001",
        ),
        account_id=ACCOUNT_ID,
    )

    assert projected == ExchangeOrderSnapshot(
        client_order_id="client-1",
        exchange_order_id="basket-1",
        status="partially_filled",
        filled_quantity=Decimal("1.12345678901234567890123456789"),
        average_price=Decimal("20000.2500000000000000000000001"),
        raw={
            "basket_id": "basket-1",
            "exchange_order_id": 123,
            "quantity": "2.00000000000000000000000000000",
            "account_id": "ACCOUNT-A",
        },
    )
    assert projected.filled_quantity is not None
    assert projected.average_price is not None
    assert (
        projected.filled_quantity.as_tuple()
        == Decimal("1.12345678901234567890123456789").as_tuple()
    )
    assert (
        projected.average_price.as_tuple()
        == Decimal("20000.2500000000000000000000001").as_tuple()
    )


def test_snapshot_missing_fill_is_the_existing_scale_free_zero():
    projected = project_rithmic_order_snapshot(
        snapshot(filled_quantity=None, average_fill_price=None),
        account_id=ACCOUNT_ID,
    )

    assert projected.filled_quantity is not None
    assert projected.filled_quantity.as_tuple() == Decimal("0").as_tuple()
    assert projected.average_price is None


@pytest.mark.parametrize("missing_field", ["quantity", "filled_quantity", "status"])
def test_snapshot_missing_required_field_preserves_attribute_error(missing_field):
    remote = snapshot()
    delattr(remote, missing_field)

    with pytest.raises(AttributeError, match=missing_field):
        project_rithmic_order_snapshot(remote, account_id=ACCOUNT_ID)


def test_snapshot_quantity_is_accessed_before_filled_quantity():
    remote = snapshot()
    del remote.quantity
    del remote.filled_quantity

    with pytest.raises(AttributeError, match="quantity"):
        project_rithmic_order_snapshot(remote, account_id=ACCOUNT_ID)


def test_event_identity_resolves_normalized_native_key_without_mutating_map():
    products: dict[tuple[str, str], str] = {NATIVE_IDENTITY: PRODUCT_ID}

    resolved = resolve_rithmic_order_event_identity(
        event(),
        account_id=ACCOUNT_ID,
        products_by_native_identity=products,
    )

    assert resolved == (PRODUCT_ID, NATIVE_IDENTITY)
    assert products == {NATIVE_IDENTITY: PRODUCT_ID}


def test_unmapped_event_uses_configured_account_not_provider_account():
    products: dict[tuple[str, str], str] = {NATIVE_IDENTITY: PRODUCT_ID}

    with pytest.raises(RithmicUnmappedOrderEvent) as caught:
        resolve_rithmic_order_event_identity(
            event(symbol="ESU6"),
            account_id=ACCOUNT_ID,
            products_by_native_identity=products,
        )

    assert caught.value.account_id == "ACCOUNT-A"
    assert caught.value.exchange == "CME"
    assert caught.value.symbol == "ESU6"
    assert products == {NATIVE_IDENTITY: PRODUCT_ID}


def test_event_identity_uppercases_without_trimming_provider_values():
    products: dict[tuple[str, str], str] = {NATIVE_IDENTITY: PRODUCT_ID}

    with pytest.raises(RithmicUnmappedOrderEvent) as caught:
        resolve_rithmic_order_event_identity(
            event(exchange=" cme ", symbol="nqu6 "),
            account_id=ACCOUNT_ID,
            products_by_native_identity=products,
        )

    assert caught.value.exchange == " CME "
    assert caught.value.symbol == "NQU6 "


def test_event_projection_preserves_provider_account_and_exact_decimals():
    projected = project_rithmic_order_event(
        event(),
        product_id=PRODUCT_ID,
        client_order_id="resolved-child-client-1",
        native_identity=NATIVE_IDENTITY,
    )

    assert projected == ExchangeOrderEvent(
        status="partially_filled",
        product_id=PRODUCT_ID,
        client_order_id="resolved-child-client-1",
        exchange_order_id="basket-1",
        cumulative_filled_quantity=Decimal("1.12345678901234567890123456789"),
        cumulative_average_price=Decimal("20000.2500000000000000000000001"),
        last_fill_quantity=Decimal("0.12345678901234567890123456789"),
        last_fill_price=Decimal("20000.1250000000000000000000001"),
        event_timestamp=1_700_000_000_000,
        raw={
            "basket_id": "basket-1",
            "native_parent_client_order_id": "provider-parent-client-1",
            "original_basket_id": "parent-1",
            "linked_basket_ids": "stop-1,target-1",
            "exchange_order_id": "exchange-1",
            "account_id": "ACCOUNT-B",
            "exchange": "CME",
            "symbol": "NQU6",
            "price": "20000.25",
            "trigger_price": "19998.25",
            "price_type": "stop_market",
            "bracket_type": "target_and_stop_static",
            "notification_type": "STATUS",
        },
    )
    assert projected.cumulative_filled_quantity is not None
    assert projected.cumulative_average_price is not None
    assert projected.last_fill_quantity is not None
    assert projected.last_fill_price is not None
    assert (
        projected.cumulative_filled_quantity.as_tuple()
        == Decimal("1.12345678901234567890123456789").as_tuple()
    )
    assert (
        projected.cumulative_average_price.as_tuple()
        == Decimal("20000.2500000000000000000000001").as_tuple()
    )
    assert (
        projected.last_fill_quantity.as_tuple()
        == Decimal("0.12345678901234567890123456789").as_tuple()
    )
    assert (
        projected.last_fill_price.as_tuple()
        == Decimal("20000.1250000000000000000000001").as_tuple()
    )


@pytest.mark.parametrize(
    "field",
    [
        "cumulative_filled_quantity",
        "cumulative_average_price",
        "last_fill_quantity",
        "last_fill_price",
    ],
)
def test_event_decimal_failures_keep_invalid_operation_taxonomy(field):
    with pytest.raises(InvalidOperation):
        project_rithmic_order_event(
            event(**{field: "not-decimal"}),
            product_id=PRODUCT_ID,
            client_order_id="client-1",
            native_identity=NATIVE_IDENTITY,
        )


@pytest.mark.parametrize(
    ("field", "projected_field"),
    [
        ("cumulative_filled_quantity", "cumulative_filled_quantity"),
        ("cumulative_average_price", "cumulative_average_price"),
        ("last_fill_quantity", "last_fill_quantity"),
        ("last_fill_price", "last_fill_price"),
    ],
)
def test_event_optional_decimal_none_is_not_coerced_to_zero(field, projected_field):
    projected = project_rithmic_order_event(
        event(**{field: None}),
        product_id=PRODUCT_ID,
        client_order_id="client-1",
        native_identity=NATIVE_IDENTITY,
    )

    assert getattr(projected, projected_field) is None


@pytest.mark.parametrize("missing_field", ["status", "basket_id", "timestamp_ms"])
def test_event_missing_required_field_preserves_attribute_error(missing_field):
    remote = event()
    delattr(remote, missing_field)

    with pytest.raises(AttributeError, match=missing_field):
        project_rithmic_order_event(
            remote,
            product_id=PRODUCT_ID,
            client_order_id="client-1",
            native_identity=NATIVE_IDENTITY,
        )
