"""Typed, stateless wire conversion for the closed synthetic native session."""
import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Literal, NotRequired, TypeAlias, TypedDict, cast

import fluxtrade_core as _native

Product = Literal["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A"]
Profile = Literal["SYNTHETIC_BTC_ETH_V1", "SYNTHETIC_GOLDEN_CANCEL_V1", "SYNTHETIC_MIN_CASH_V1", "SYNTHETIC_P1_LIQUIDATION_V1"]
Side = Literal["LONG", "SHORT"]
ObservedSide = Literal["buy", "sell"]
Ordering = Literal["S_order_v1", "S_order_v1_reverse_execution_cancel_effective"]
Reason = Literal["EXPLICIT_SCENARIO", "RISK_SHORTFALL", "MMR_BREACH", "SPEC_MIGRATION", "UNSUPPORTED"]
Kind = Literal["MARKET", "EARN", "TRADING", "POSITIONS", "OPEN_ORDERS"]
PayloadKind = Literal["EXECUTION_FACT", "TRANSPORT_ACK", "MARKET_SNAPSHOT", "EARN_SNAPSHOT", "TRADING_SNAPSHOT", "POSITION_SNAPSHOT", "OPEN_ORDER_SNAPSHOT"]
Lifecycle = Literal["RISK_STABLE", "AWAITING_CANCEL_EFFECTIVE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT"]


class Account(TypedDict):
    venue: str
    environment: str
    account: str
    subaccount: NotRequired[str | None]


class Stamp(TypedDict):
    source_sequence: NotRequired[int | None]
    event_id: str
    effective_at: int
    causal_parent_ids: list[str]
    ordering_contract_id: str
    scenario_ordinal: int


class Mark(TypedDict):
    product_id: Literal["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    valid_from: int
    valid_to: int
    mark: Decimal


class Context(TypedDict):
    expected_before: str
    expected_after: str
    rows: list[Mark]


class Execution(TypedDict):
    namespace: str
    product_id: Product
    external_execution_id: str
    order_id: str
    side: Side
    price: Decimal
    quantity_contracts: Decimal
    liquidity: Literal["SYNTHETIC_TAKER"]
    fee_asset: NotRequired[str | None]
    reported_fee: NotRequired[Decimal | None]
    matching_effective_at: int
    candidate_id: str
    source_id: str
    visible_at: int
    expected_account_version: int
    expected_order_version: int
    spec_version: str
    rule_data_version: str


class Intent(TypedDict):
    intent_id: str
    client_order_id: str
    config_id: str
    product_id: Product
    strategy_id: str
    side: Side
    order_type: Literal["LIMIT", "MARKET"]
    quantity_contracts: Decimal
    limit_price: NotRequired[Decimal | None]
    reduce_only: bool
    requested_at: int


class Target(TypedDict):
    target_order_id: str
    reason: Reason


class Effect(Target):
    detecting_event_id: str


class CancelRequest(TypedDict):
    targets: list[Target]


class CancelEffect(TypedDict):
    effects: list[Effect]


class ContextMember(TypedDict):
    kind: Literal["CONTEXT_MARKS"]
    stamp: Stamp
    payload: Context


class ExecutionMember(TypedDict):
    kind: Literal["EXECUTION"]
    stamp: Stamp
    payload: Execution


class IntentMember(TypedDict):
    kind: Literal["INTENT"]
    stamp: Stamp
    payload: Intent


class CancelRequestMember(TypedDict):
    kind: Literal["CANCEL_REQUEST"]
    stamp: Stamp
    payload: CancelRequest


class CancelEffectMember(TypedDict):
    kind: Literal["CANCEL_EFFECT"]
    stamp: Stamp
    payload: CancelEffect


Member = ContextMember | ExecutionMember | IntentMember | CancelRequestMember | CancelEffectMember


class Group(TypedDict):
    schema_version: Literal["scenario_group_v1"]
    group_id: str
    account_key: Account
    ordering_contract_id: Ordering
    group_effective_at: int
    declared_member_count: int
    members: list[Member]


class Reference(TypedDict):
    namespace: Literal["SOURCE", "LIQUIDATION", "SNAPSHOT"]
    fact_id: str


class Rejection(TypedDict):
    event_id: str
    reason: str


class GroupResult(TypedDict):
    schema_version: Literal["group_result_v1"]
    classification: Literal["COMMITTED", "REJECTED", "FAULT"]
    group_id: str
    group_digest: NotRequired[str]
    committed_references: list[Reference]
    rejections: list[Rejection]
    failure: NotRequired[str]
    account_version_before: int
    account_version_after: int
    gate_after: Literal["RUNNING", "FAILED"]
    gate_failure: NotRequired[str]
    lifecycle_after: Lifecycle
    owner_state_digest: str
    result_digest: str


class SnapshotRequest(TypedDict):
    schema_version: Literal["snapshot_request_v1"]
    account_key: Account
    snapshot_id: str
    snapshot_kind: Kind
    capture_mode: Literal["OWNER_CURRENT", "FROZEN_POLL_FIXTURE"]
    fixture_key: NotRequired[str | None]
    captured_at: int
    continuation_id: NotRequired[str | None]


class Transport(TypedDict):
    route: Literal["REST", "WS"]
    operation: Literal["ORDER", "CANCEL"]
    client_order_id: str
    order_id: NotRequired[str | None]
    code: str
    message: NotRequired[str | None]
    product_id: NotRequired[str | None]
    side: NotRequired[ObservedSide | None]
    limit_price: NotRequired[Decimal | None]
    size_contracts: NotRequired[Decimal | None]


class Projection(TypedDict):
    schema_version: Literal["delivery_projection_v1"]
    reference: Reference
    payload_kind: PayloadKind
    occurrence_index: int
    schedule_sequence: int
    visible_at: int
    continuation_id: NotRequired[str | None]
    transport: NotRequired[Transport | None]


class ExecutionFact(TypedDict):
    order_id: str
    owner_client_order_id: str
    policy_client_order_id: str
    product_id: Product
    state: Literal["partially_filled", "filled"]
    side: ObservedSide
    limit_price: Decimal
    fill_price: Decimal
    original_size_contracts: Decimal
    cumulative_filled_size_contracts: Decimal
    contract_value: Decimal
    execution_effective_at: int
    commit_account_version: int
    spec_version: str
    rule_data_version: str


class Market(TypedDict):
    product_id: str
    price: Decimal
    contract_value: Decimal
    lot_size: Decimal
    minimum_size: Decimal
    price_increment: Decimal
    high_low_ratio: Decimal
    state: Literal["live"]
    instrument_code: int


class MarketSnapshot(TypedDict):
    markets: list[Market]


class Failure(TypedDict):
    outcome: Literal["FAILURE"]
    reason: Literal["SYNTHETIC_FAILURE"]


class Earn(TypedDict):
    outcome: Literal["SUCCESS"]
    earn: Decimal


class Trading(TypedDict):
    outcome: Literal["SUCCESS"]
    equity: Decimal
    available_equity: Decimal


class Position(TypedDict):
    product_id: str
    margin_mode: Literal["cross"]
    position_contracts: Decimal
    last_price: NotRequired[Decimal]
    notional_usd: NotRequired[Decimal]


class OpenOrder(TypedDict):
    order_id: str
    client_order_id: str
    product_id: str
    state: Literal["live", "partially_filled"]
    side: ObservedSide
    limit_price: Decimal
    original_size_contracts: Decimal
    cumulative_filled_size_contracts: Decimal
    created_at: int


class Positions(TypedDict):
    outcome: Literal["SUCCESS"]
    rows: list[Position]


class OpenOrders(TypedDict):
    outcome: Literal["SUCCESS"]
    rows: list[OpenOrder]


SnapshotPayload = MarketSnapshot | Failure | Earn | Trading | Positions | OpenOrders


class SnapshotFact(TypedDict):
    schema_version: Literal["snapshot_fact_v1"]
    reference: Reference
    request_digest: str
    snapshot_kind: Kind
    captured_account_version: NotRequired[int]
    snapshot_as_of: int
    continuation_id: NotRequired[str]
    immutable_payload: SnapshotPayload
    payload_digest: str


class Delivery(TypedDict):
    account_key: Account
    delivery_id: str
    source_fact_id: str
    source_namespace: Literal["SOURCE", "LIQUIDATION", "SNAPSHOT"]
    payload_kind: PayloadKind
    occurrence_index: int
    schedule_sequence: int
    immutable_payload: SnapshotPayload | ExecutionFact | Transport
    payload_digest: str
    snapshot_version: NotRequired[int]
    snapshot_as_of: NotRequired[int]
    visible_at: int
    continuation_id: NotRequired[str]


class Inspection(TypedDict):
    schema_version: Literal["inspect_state_v1"]
    account_key: Account
    profile_id: Profile
    config_id: str
    account_version: int
    valuation_context_id: str
    gate: Literal["RUNNING", "FAILED"]
    gate_failure: NotRequired[str]
    lifecycle: Lifecycle
    cash: Decimal
    gross_realized: Decimal
    total_fees: Decimal
    positions_digest: str
    orders_digest: str
    reservations_digest: str
    owner_state_digest: str


_Json: TypeAlias = str | int | bool | None | list["_Json"] | dict[str, "_Json"]
_MONEY = frozenset("mark price quantity_contracts reported_fee limit_price size_contracts fill_price original_size_contracts cumulative_filled_size_contracts contract_value lot_size minimum_size price_increment high_low_ratio earn equity available_equity position_contracts last_price notional_usd cash gross_realized total_fees".split())
_OPTIONAL = frozenset("subaccount source_sequence fee_asset reported_fee limit_price fixture_key continuation_id transport order_id message product_id side size_contracts".split())
_INTEGER = frozenset("source_sequence effective_at scenario_ordinal valid_from valid_to matching_effective_at visible_at expected_account_version expected_order_version requested_at group_effective_at declared_member_count occurrence_index schedule_sequence captured_at captured_account_version snapshot_as_of snapshot_version execution_effective_at commit_account_version instrument_code created_at account_version account_version_before account_version_after".split())


def _decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise _native.ScenarioReplayInputError("INVALID_SCHEMA")
    if not value:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _convert(value: object, *, reading: bool, field: str = "") -> object:
    if isinstance(value, Mapping):
        return {key: _convert(item, reading=reading, field=key) for key, item in value.items()
                if not (item is None and key in _OPTIONAL and not reading)}
    if isinstance(value, list):
        return [_convert(item, reading=reading) for item in value]
    if field in _MONEY and value is not None:
        if reading and isinstance(value, str):
            return Decimal(value)
        if not reading and isinstance(value, Decimal):
            return _decimal(value)
        raise _native.ScenarioReplayInputError("INVALID_SCHEMA")
    if field in _INTEGER and type(value) is not int:
        raise _native.ScenarioReplayInputError("INVALID_SCHEMA")
    if value is None or type(value) in (str, int, bool):
        return value
    raise _native.ScenarioReplayInputError("INVALID_SCHEMA")


def _encode(value: object) -> str:
    return json.dumps(_convert(value, reading=False), ensure_ascii=False, separators=(",", ":"))


def _decode(value: str) -> object:
    return _convert(cast(_Json, json.loads(value)), reading=True)


class ScenarioCodec:
    __slots__ = ("_session",)

    def __init__(self, profile: Profile, account: Account) -> None:
        self._session = _native._SyntheticScenarioReplaySession(profile, _encode(account))

    def apply_group(self, request: Group) -> GroupResult:
        return cast(GroupResult, _decode(self._session.apply_group(_encode(request))))

    def capture_snapshot(self, request: SnapshotRequest) -> SnapshotFact:
        return cast(SnapshotFact, _decode(self._session.capture_snapshot(_encode(request))))

    def build_delivery(self, request: Projection) -> Delivery:
        return cast(Delivery, _decode(self._session.build_delivery(_encode(request))))

    def inspect_state(self) -> Inspection:
        return cast(Inspection, _decode(self._session.inspect_state()))


def _native_failure(exc: Exception) -> dict[str, str] | None:
    kinds = {_native.ScenarioReplayInputError: "INPUT", _native.ScenarioReplayLookupError: "LOOKUP",
             _native.ScenarioReplayConflictError: "CONFLICT", _native.ScenarioReplayInvariantError: "INVARIANT"}
    kind = kinds.get(type(exc))
    return None if kind is None else {"kind": kind, "reason": str(exc.args[0])}
