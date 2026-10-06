from decimal import Decimal

import pytest

from src.core.adapters.rithmic_order_status import _classify_status


@pytest.mark.parametrize(
    ("status", "quantity", "filled", "expected"),
    [
        (" OPEN ", "2", "0", ("open", False)),
        ("open", "2", "2", (None, None)),
        ("OPEN-PENDING", "2", "1", ("partially_filled", False)),
        ("OPEN PENDING", "2", "0", ("open", False)),
        ("new", "2", "0", ("open", False)),
        ("submitted", "2", "0", ("open", False)),
        ("accepted", "2", "0", ("open", False)),
        ("modified", "2", "0", ("open", False)),
        ("partial", "2", "1", ("partially_filled", False)),
        ("PARTIALLY-FILLED", "2", "1", ("partially_filled", False)),
        ("partiallyfilled", "2", "1", ("partially_filled", False)),
        ("partial", "2", "0", (None, None)),
        ("partial", "2", "2", (None, None)),
        ("partially_filled", "2", "2", (None, None)),
        ("filled", "2", "2", ("filled", True)),
        ("filled", "2", "1", (None, None)),
        ("closed", "2", "2", ("filled", True)),
        ("closed", "2", "0", (None, None)),
        ("completed", "2", "2", ("filled", True)),
        ("complete", "2", "2", ("filled", True)),
        ("complete", "2", "0", (None, None)),
        ("cancel", "2", "1", ("cancelled", True)),
        ("canceled", "2", "0", ("cancelled", True)),
        ("cancelled", "2", "1", ("cancelled", True)),
        ("cancelled", "2", "2", (None, None)),
        ("reject", "2", "1", ("failed", True)),
        ("rejected", "2", "0", ("failed", True)),
        ("failed", "2", "1", ("failed", True)),
        ("expired", "2", "1", ("failed", True)),
        ("rejected", "2", "2", (None, None)),
    ],
)
def test_supported_raw_aliases_use_one_quantity_aware_interpretation(
    status, quantity, filled, expected
):
    result = _classify_status(
        status, Decimal(filled), Decimal(quantity), notification_type="GENERIC"
    )
    assert (result.economic_status, result.terminal_order) == expected


@pytest.mark.parametrize(
    ("status", "notification", "filled", "expected"),
    [
        ("unknown", "FILL", "1", ("partially_filled", False)),
        ("unknown", "FILL", "2", ("filled", True)),
        ("unknown", "FILL", "0", (None, None)),
        ("anything", "CANCEL", "0", ("cancelled", True)),
        ("anything", "REJECT", "1", ("failed", True)),
        ("anything", "CANCEL", "2", (None, None)),
        ("anything", "REJECT", "2", (None, None)),
    ],
)
def test_exchange_notifications_override_raw_status_only_when_defined(
    status, notification, filled, expected
):
    # Synthetic application-contract cases, not provider-valid captures.
    # Enum names are from the official schema; fill progress follows the
    # owned native decoder's cumulative-quantity rule.
    result = _classify_status(
        status, Decimal(filled), Decimal("2"), notification_type=notification
    )
    assert (result.economic_status, result.terminal_order) == expected


@pytest.mark.parametrize(
    ("status", "filled", "expected_status"),
    [
        ("complete", "0", None),
        ("complete", "1", None),
        ("complete", "2", "filled"),
        ("open", "0", None),
        ("partial", "1", None),
        ("cancelled", "1", "cancelled"),
        ("rejected", "1", "failed"),
        ("cancelled", "2", None),
        ("unknown", "2", None),
    ],
)
def test_complete_notification_is_terminal_but_not_fill_evidence(
    status, filled, expected_status
):
    # The licensed SampleOrder.py treats COMPLETE as order completion; it does
    # not establish a full execution without consistent raw status/fill data.
    result = _classify_status(
        status, Decimal(filled), Decimal("2"), notification_type="COMPLETE"
    )
    assert result.economic_status == expected_status
    assert result.terminal_order is True


@pytest.mark.parametrize(
    ("quantity", "filled", "unfilled", "notification", "status", "expected"),
    [
        ("2", "1", None, "FILL", "unknown", ("partially_filled", False)),
        ("2", "2", None, "FILL", "unknown", ("filled", True)),
        ("2", "1", "1", "FILL", "unknown", ("partially_filled", False)),
        ("2", "2", "0", "FILL", "unknown", ("filled", True)),
        ("2", "1", "0", "FILL", "unknown", (None, None)),
        ("2", "2", "1", "FILL", "unknown", (None, None)),
        ("2", "1", "2", "FILL", "unknown", (None, None)),
        ("2", "1", "-1", "FILL", "unknown", (None, None)),
        ("2", "1", "3", "FILL", "unknown", (None, None)),
        ("2", "1", "NaN", "FILL", "unknown", (None, None)),
        ("2", "1", "Infinity", "STATUS", "open", (None, None)),
    ],
)
def test_unfilled_presence_and_fill_total_consistency(
    quantity, filled, unfilled, notification, status, expected
):
    result = _classify_status(
        status,
        Decimal(filled),
        Decimal(quantity),
        notification_type=notification,
        unfilled_quantity=Decimal(unfilled) if unfilled is not None else None,
    )
    assert (result.economic_status, result.terminal_order) == expected


@pytest.mark.parametrize(
    ("quantity", "filled"),
    [
        ("0", "0"),
        ("-1", "0"),
        ("NaN", "0"),
        ("Infinity", "0"),
        ("2", "-1"),
        ("2", "3"),
        ("2", "NaN"),
        ("2", "Infinity"),
    ],
)
def test_invalid_or_nonfinite_quantity_evidence_is_unresolved(quantity, filled):
    result = _classify_status("cancelled", Decimal(filled), Decimal(quantity))
    assert (result.economic_status, result.terminal_order) == (None, None)


@pytest.mark.parametrize(
    ("quantity", "filled"),
    [("0", "0"), ("NaN", "0"), ("2", "3"), ("2", "NaN")],
)
def test_complete_notification_does_not_override_invalid_quantity_evidence(
    quantity, filled
):
    result = _classify_status(
        "complete",
        Decimal(filled),
        Decimal(quantity),
        notification_type="COMPLETE",
    )
    assert (result.economic_status, result.terminal_order) == (None, None)


@pytest.mark.parametrize("status", [None, "", "unrecognized", "FA"])
def test_unknown_raw_status_stays_unresolved_without_special_notification(status):
    result = _classify_status(status, Decimal("0"), Decimal("2"))
    assert (result.economic_status, result.terminal_order) == (None, None)
