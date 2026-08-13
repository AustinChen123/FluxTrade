from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.execution_ambiguous_submit_adoption import (
    adopt_pending_conditional_order_before_submit,
    adopt_order_after_ambiguous_submit_error,
)
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderLookupUnsupported,
    ExchangeOrderSnapshot,
    NetworkError,
)


def _subject(*, client_order_id="client-1"):
    return SimpleNamespace(
        order=SimpleNamespace(
            client_order_id=client_order_id,
            product_id="TEST:PRODUCT",
            type="limit",
        ),
        adapter=MagicMock(),
        process_order_event=MagicMock(),
    )


def _adopt(subject, error, *, submit_attempted=True):
    return adopt_order_after_ambiguous_submit_error(
        adapter=subject.adapter,
        process_exchange_order_event=subject.process_order_event,
        order=subject.order,
        error=error,
        submit_attempted=submit_attempted,
    )


def _adopt_pending(subject):
    return adopt_pending_conditional_order_before_submit(
        adapter=subject.adapter,
        process_exchange_order_event=subject.process_order_event,
        order=subject.order,
    )


@pytest.mark.parametrize(
    ("client_order_id", "lookup", "expected"),
    [
        (None, None, None),
        (
            "client-1",
            ExchangeOrderLookupUnsupported("unsupported"),
            {
                "order_id": "order-1",
                "order_type": "limit",
                "reason": "verification_blocked_order_lookup_unsupported",
            },
        ),
        (
            "client-1",
            ExchangeError("lookup failed"),
            {
                "order_id": "order-1",
                "order_type": "limit",
                "reason": "verification_blocked_order_lookup_failed",
                "error": "lookup failed",
            },
        ),
        ("client-1", None, None),
    ],
)
def test_pending_conditional_pre_submit_lookup_classification_is_exact(
    client_order_id,
    lookup,
    expected,
):
    subject = _subject(client_order_id=client_order_id)
    subject.order.id = "order-1"
    if isinstance(lookup, Exception):
        subject.adapter.get_order_by_client_id.side_effect = lookup
    else:
        subject.adapter.get_order_by_client_id.return_value = lookup

    assert _adopt_pending(subject) == expected

    if client_order_id is None:
        subject.adapter.get_order_by_client_id.assert_not_called()
    else:
        subject.adapter.get_order_by_client_id.assert_called_once_with(
            "client-1",
            "TEST:PRODUCT",
            order_type="limit",
        )
    subject.process_order_event.assert_not_called()


@pytest.mark.parametrize("action", ["applied", "unknown_status"])
def test_pending_conditional_snapshot_is_converted_and_processed_once(action):
    subject = _subject()
    subject.order.id = "order-1"
    snapshot = ExchangeOrderSnapshot(
        client_order_id="client-1",
        exchange_order_id="exchange-1",
        status="open",
    )
    event_result = {"action": action, "marker": object()}
    subject.adapter.get_order_by_client_id.return_value = snapshot
    subject.process_order_event.return_value = event_result

    result = _adopt_pending(subject)

    if action == "applied":
        assert result is None
    else:
        assert result == {
            "order_id": "order-1",
            "order_type": "limit",
            "reason": "unknown_status",
            "event_result": event_result,
        }
    event = subject.process_order_event.call_args.args[0]
    assert event.product_id == "TEST:PRODUCT"
    assert event.client_order_id == "client-1"
    assert event.exchange_order_id == "exchange-1"
    assert event.status == "open"
    subject.process_order_event.assert_called_once()


@pytest.mark.parametrize(
    ("submit_attempted", "error", "client_order_id", "expected"),
    [
        (
            False,
            NetworkError("timeout"),
            "client-1",
            {"action": "submit_not_attempted", "verification_blocked": False},
        ),
        (
            True,
            ExchangeError("rejected"),
            "client-1",
            {"action": "not_ambiguous", "verification_blocked": False},
        ),
        (
            True,
            NetworkError("timeout"),
            None,
            {
                "action": "verification_blocked_missing_client_order_id",
                "verification_blocked": True,
            },
        ),
    ],
)
def test_pre_lookup_classification_never_queries_adapter(
    submit_attempted,
    error,
    client_order_id,
    expected,
):
    subject = _subject(client_order_id=client_order_id)

    assert _adopt(subject, error, submit_attempted=submit_attempted) == expected

    subject.adapter.get_order_by_client_id.assert_not_called()
    subject.process_order_event.assert_not_called()


@pytest.mark.parametrize(
    ("lookup", "expected"),
    [
        (
            None,
            {
                "action": "verification_blocked_order_snapshot_missing",
                "verification_blocked": True,
            },
        ),
        (
            ExchangeOrderLookupUnsupported("unsupported"),
            {
                "action": "verification_blocked_order_lookup_unsupported",
                "verification_blocked": True,
            },
        ),
        (
            ExchangeError("lookup failed"),
            {
                "action": "verification_blocked_order_lookup_failed",
                "reason": "lookup failed",
                "verification_blocked": True,
            },
        ),
    ],
)
def test_lookup_failure_classification_is_exact(lookup, expected):
    subject = _subject()
    if isinstance(lookup, Exception):
        subject.adapter.get_order_by_client_id.side_effect = lookup
    else:
        subject.adapter.get_order_by_client_id.return_value = lookup

    assert _adopt(subject, NetworkError("timeout")) == expected

    subject.adapter.get_order_by_client_id.assert_called_once_with(
        "client-1",
        "TEST:PRODUCT",
        order_type="limit",
    )
    subject.process_order_event.assert_not_called()


@pytest.mark.parametrize(
    ("action", "verification_blocked", "unresolved"),
    [
        ("unknown_status", True, False),
        ("unresolved_missing_fill_price", False, True),
    ],
)
def test_non_applied_event_preserves_result_and_classifies_action(
    action,
    verification_blocked,
    unresolved,
):
    subject = _subject()
    snapshot = ExchangeOrderSnapshot(
        client_order_id="client-1",
        exchange_order_id="exchange-1",
        status="open",
    )
    event_result = {"action": action, "marker": object()}
    subject.adapter.get_order_by_client_id.return_value = snapshot
    subject.process_order_event.return_value = event_result

    result = _adopt(subject, NetworkError("timeout"))

    assert result == {
        "action": action,
        "event_result": event_result,
        "verification_blocked": verification_blocked,
        "unresolved": unresolved,
    }
    event = subject.process_order_event.call_args.args[0]
    assert event.product_id == "TEST:PRODUCT"
    assert event.client_order_id == "client-1"
    assert event.exchange_order_id == "exchange-1"
    assert event.status == "open"
    subject.process_order_event.assert_called_once()


@pytest.mark.parametrize(
    "terminal_state",
    ["cancelled", "rejected", "expired", "failed", "liquidated"],
)
def test_terminal_applied_snapshot_uses_snapshot_exchange_identity_fallback(
    terminal_state,
):
    subject = _subject()
    snapshot = ExchangeOrderSnapshot(
        client_order_id="client-1",
        exchange_order_id="exchange-terminal",
        status="cancelled",
    )
    event_result = {
        "action": "applied",
        "state": terminal_state,
        "exchange_order_id": None,
    }
    subject.adapter.get_order_by_client_id.return_value = snapshot
    subject.process_order_event.return_value = event_result

    assert _adopt(subject, NetworkError("timeout")) == {
        "action": "terminal_after_submit_error",
        "event_result": event_result,
        "exchange_order_id": "exchange-terminal",
        "verification_blocked": False,
        "terminal": True,
    }


@pytest.mark.parametrize(
    ("snapshot_exchange_id", "event_exchange_id", "expected"),
    [
        (
            None,
            None,
            {
                "action": (
                    "verification_blocked_order_snapshot_missing_exchange_order_id"
                ),
                "verification_blocked": True,
            },
        ),
        (
            "snapshot-id",
            None,
            {
                "action": "adopted",
                "exchange_order_id": "snapshot-id",
                "verification_blocked": False,
            },
        ),
        (
            "snapshot-id",
            "event-id",
            {
                "action": "adopted",
                "exchange_order_id": "event-id",
                "verification_blocked": False,
            },
        ),
    ],
)
def test_applied_nonterminal_snapshot_requires_and_prefers_event_exchange_identity(
    snapshot_exchange_id,
    event_exchange_id,
    expected,
):
    subject = _subject()
    snapshot = ExchangeOrderSnapshot(
        client_order_id="client-1",
        exchange_order_id=snapshot_exchange_id,
        status="open",
    )
    event_result = {
        "action": "applied",
        "state": "open",
        "exchange_order_id": event_exchange_id,
    }
    subject.adapter.get_order_by_client_id.return_value = snapshot
    subject.process_order_event.return_value = event_result

    result = _adopt(subject, NetworkError("timeout"))

    assert result == {**expected, "event_result": event_result}
    subject.process_order_event.assert_called_once()
