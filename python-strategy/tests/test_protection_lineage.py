from types import SimpleNamespace

import pytest

from src.core.protection_lineage import protection_lineage_is_valid


def _rows(*, child_side="sell", linked="absent", sibling_side="buy"):
    entry = SimpleNamespace(
        id="entry-1",
        type="market",
        side="buy",
        product_id="TEST:BTC-USD",
        exchange_id="test",
        strategy_id="strategy-1",
        account_profile="paper",
        account_id="TEST_ACCOUNT_001",
        intent_payload={"system_placed_protection": "SL"},
    )
    payload = {"pending_entry_order_id": "entry-1"}
    if linked != "absent":
        payload["linked_order_id"] = linked
    child = SimpleNamespace(
        id="child-1",
        type="stop_loss",
        side=child_side,
        product_id=entry.product_id,
        exchange_id=entry.exchange_id,
        strategy_id=entry.strategy_id,
        account_profile=entry.account_profile,
        account_id=entry.account_id,
        intent_payload=payload,
    )
    sibling = None
    if isinstance(linked, str) and linked and linked != "absent":
        sibling = SimpleNamespace(
            id=linked,
            type="take_profit",
            side=sibling_side,
            product_id=entry.product_id,
            exchange_id=entry.exchange_id,
            strategy_id=entry.strategy_id,
            account_profile=entry.account_profile,
            account_id=entry.account_id,
            intent_payload={
                "pending_entry_order_id": "entry-1",
                "linked_order_id": "child-1",
            },
        )
    return child, entry, sibling


def _valid(child, entry, sibling):
    return protection_lineage_is_valid(
        order=child,
        entry=entry,
        sibling=sibling,
        event_product_id="TEST:BTC-USD",
    )


def test_absent_optional_link_is_valid():
    child, entry, sibling = _rows()
    assert _valid(child, entry, sibling)


def test_reciprocal_opposite_side_sibling_is_valid():
    child, entry, sibling = _rows(linked="child-2", sibling_side="sell")
    assert _valid(child, entry, sibling)


@pytest.mark.parametrize("malformed", [4, [], {}, True, ""])
def test_malformed_present_sibling_link_is_invalid(malformed):
    child, entry, sibling = _rows(linked=malformed)
    assert not _valid(child, entry, sibling)


@pytest.mark.parametrize(
    ("child_side", "linked", "sibling_side"),
    [
        ("buy", "absent", "buy"),  # child is on entry side
        ("unknown", "absent", "buy"),  # child side is not recognized
        ("sell", "child-2", "buy"),  # sibling is on entry side
        ("sell", "child-2", "unknown"),  # sibling side is not recognized
    ],
)
def test_unknown_or_wrong_side_relationship_is_invalid(
    child_side, linked, sibling_side
):
    child, entry, sibling = _rows(
        child_side=child_side,
        linked=linked,
        sibling_side=sibling_side,
    )
    assert not _valid(child, entry, sibling)


def test_missing_referenced_sibling_is_invalid():
    child, entry, _sibling = _rows(linked="missing-child")
    assert not _valid(child, entry, None)


def test_nonreciprocal_sibling_parent_is_invalid():
    child, entry, sibling = _rows(linked="child-2", sibling_side="sell")
    assert sibling is not None
    sibling.intent_payload["linked_order_id"] = "other-child"
    assert not _valid(child, entry, sibling)
