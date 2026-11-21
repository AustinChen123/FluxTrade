"""No-I/O research evidence sink and result DTOs for historical Spider runs."""

from dataclasses import dataclass
from typing import Any, cast

from src.core.backtest.spider_historical_ports import EvidenceSink
from src.core.backtest.spider_historical_report_projection import (
    FinalOwnerEvidenceValues, project_historical_report,
)


@dataclass(frozen=True, slots=True)
class ResearchRunResult:
    outcome: str
    research_classification: str
    admission_state: str
    report_rows: tuple[dict[str, Any], ...]
    initial_owner_evidence: dict[str, Any]
    final_owner_evidence: dict[str, Any]
    final_scheduler_observation: dict[str, Any]
    normalized_context: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ResearchFailureResult:
    outcome: str
    research_classification: str
    admission_state: str
    failure_classification: str
    reason: str
    primary: dict[str, Any] | None
    last_processed_boundary: dict[str, Any] | None
    last_fully_recorded_row_ordinal: int | None


class ResearchEvidenceSink(EvidenceSink):
    """Retain only projection inputs; never retain rows or expose a writer capability."""

    def __init__(self, run_id: str, products: tuple[str, ...]):
        self._run_id = run_id
        self._products = products
        self._projection_inputs = (tuple(() for _ in products), tuple(() for _ in products))
        self._last_processed_boundary: dict[str, Any] | None = None
        self._last_recorded_ordinal: int | None = None
        self.failure_result: ResearchFailureResult | None = None

    @staticmethod
    def rejected(classification: str, reason: str, primary: dict[str, Any] | None) -> ResearchFailureResult:
        return ResearchFailureResult(
            "REJECTED", "RESEARCH_ONLY", "NOT_ADMITTED", classification, reason,
            primary, None, None,
        )

    def checkpoint(self, boundary: dict[str, Any], kind: str) -> None:
        self._last_processed_boundary = dict(
            ordinal=boundary["ordinal"], kind=kind, event_key=boundary["barrier_id"],
        )

    def note_boundary(self, boundary: dict[str, Any], kind: str) -> None:
        self._last_processed_boundary = dict(
            ordinal=boundary["ordinal"], kind=kind, event_key=boundary["barrier_id"],
        )

    def persist(self, row: dict[str, Any]) -> None:
        return None

    def retain(self, row: dict[str, Any]) -> None:
        fills, refs = self._projection_inputs
        fill_delta: list[list[str]] = [[] for _ in self._products]
        ref_delta: list[list[str]] = [[] for _ in self._products]
        payload = row["payload"]
        for index, product in enumerate(self._products):
            related = False
            if row["record_kind"] == "HISTORICAL_MARKET_RESULT":
                item = next((value for value in payload["result"]["products"]
                             if value["product_id"] == product), None)
                if item is not None:
                    related = True
                    fill_delta[index].extend(fill["source_event_id"] for fill in item["fills"])
            elif row["record_kind"] == "SOURCE_GROUP_RESULT":
                related = any(member.get("payload", {}).get("product_id") == product
                              for member in payload["request"]["members"])
            if related:
                ref_delta[index].append(f"journal:{row['journal_seq']}")
        next_inputs = (
            tuple(fills[index] + tuple(fill_delta[index]) for index in range(len(self._products))),
            tuple(refs[index] + tuple(ref_delta[index]) for index in range(len(self._products))),
        )
        self._projection_inputs = next_inputs
        self._last_recorded_ordinal = row["journal_seq"]

    def fail(self, classification: str, reason: str, primary: dict[str, Any] | None,
             last_processed_boundary: dict[str, Any] | None,
             last_fully_recorded_row_ordinal: int | None) -> ResearchFailureResult:
        outcome = "DURABILITY_UNKNOWN" if classification == "DURABILITY_UNKNOWN" else "FAILED"
        result = ResearchFailureResult(
            outcome, "RESEARCH_ONLY", "NOT_ADMITTED", classification, reason, primary,
            last_processed_boundary, last_fully_recorded_row_ordinal,
        )
        self.failure_result = result
        return result

    def finish(self, initial_owner_evidence: dict[str, Any], final_owner_evidence: dict[str, Any],
               final_scheduler_observation: dict[str, Any], terminal: str,
               normalized_context: dict[str, Any]) -> ResearchRunResult:
        inspection = final_owner_evidence["inspection"]
        trading = final_owner_evidence["trading_fact"]["immutable_payload"]
        positions = final_owner_evidence["positions_fact"]["immutable_payload"]["rows"]
        orders = final_owner_evidence["open_orders_fact"]["immutable_payload"]["rows"]
        by_product = {}
        for product in self._products:
            position = next((row for row in positions if row["product_id"] == product), None)
            by_product[product] = dict(
                position_contracts=position["position_contracts"] if position is not None else "0",
                mark_price=position["last_price"] if position is not None else None,
                notional_usd=position["notional_usd"] if position is not None else "0",
                open_orders=tuple(row for row in orders if row["product_id"] == product),
            )
        final_values = dict(
            account_cash=inspection["cash"], account_equity=trading["equity"],
            account_available_equity=trading["available_equity"],
            account_gross_realized=inspection["gross_realized"],
            account_total_fees=inspection["total_fees"], products_by_id=by_product,
        )
        rows = project_historical_report(
            run_id=self._run_id, product_order=self._products, terminal_reason=terminal,
            fill_source_event_ids_by_product=self._projection_inputs[0],
            journal_evidence_refs_by_product=self._projection_inputs[1],
            final_owner_evidence=cast(FinalOwnerEvidenceValues, final_values),
            configuration_context=normalized_context["configuration_context"],
            historical_context=normalized_context["historical_context"],
        )
        return ResearchRunResult(
            "COMPLETE", "RESEARCH_ONLY", "NOT_ADMITTED", rows, initial_owner_evidence,
            final_owner_evidence, final_scheduler_observation, normalized_context,
        )
