"""Pure historical product report projection over normalized evidence values."""

from typing import Any, TypedDict

_FINAL = "artifact:endpoint.json#/final_owner_evidence"


class ProductOwnerValues(TypedDict):
    position_contracts: Any
    mark_price: Any
    notional_usd: Any
    open_orders: tuple[dict[str, Any], ...]


class FinalOwnerEvidenceValues(TypedDict):
    account_cash: Any
    account_equity: Any
    account_available_equity: Any
    account_gross_realized: Any
    account_total_fees: Any
    products_by_id: dict[str, ProductOwnerValues]


NormalizedConfigurationContext = dict[str, Any]
NormalizedHistoricalContext = dict[str, Any]


def project_historical_report(
    *, run_id: str, product_order: tuple[str, ...], terminal_reason: str,
    fill_source_event_ids_by_product: tuple[tuple[str, ...], ...],
    journal_evidence_refs_by_product: tuple[tuple[str, ...], ...],
    final_owner_evidence: FinalOwnerEvidenceValues,
    configuration_context: NormalizedConfigurationContext,
    historical_context: NormalizedHistoricalContext,
) -> tuple[dict[str, Any], ...]:
    """Build report rows from already-resolved values without persistence access."""
    owner = final_owner_evidence
    return tuple(dict(
        schema_version="spider_product_report_v1", run_id=run_id, product_id=product,
        terminal_reason=terminal_reason,
        position_contracts=owner["products_by_id"][product]["position_contracts"],
        mark_price=owner["products_by_id"][product]["mark_price"],
        notional_usd=owner["products_by_id"][product]["notional_usd"],
        open_orders=list(owner["products_by_id"][product]["open_orders"]),
        committed_execution_refs=["SOURCE:" + event_id for event_id in fills],
        source_evidence_refs=[*refs, _FINAL],
        account_cash=owner["account_cash"], account_equity=owner["account_equity"],
        account_available_equity=owner["account_available_equity"],
        account_gross_realized=owner["account_gross_realized"],
        account_total_fees=owner["account_total_fees"],
        configuration_context=configuration_context, historical_context=historical_context,
    ) for product, fills, refs in zip(
        product_order, fill_source_event_ids_by_product, journal_evidence_refs_by_product, strict=True
    ))
