import { parseUtcTimestamp } from "../../shared/time/utc";

export type SpiderOrderCategory = "ALL" | "CURRENT" | "PENDING" | "HISTORY";

export type SpiderLocalOrder = {
  local_order_id: string;
  client_order_id: string | null;
  source_client_order_id: string;
  provider_wire_client_order_id: string;
  provider_order_id: string | null;
  provider_order_id_status: "RECORDED" | "NOT_RECORDED";
  strategy_id: string;
  account_profile: string;
  account_id: string;
  exchange_id: string;
  source_product_id: string;
  product_id: string;
  role: string;
  plan_id: string;
  action_id: string;
  operation_id: string;
  generation: number;
  configuration_sha256: string;
  transmission_state: string;
  order_status: string;
  category: Exclude<SpiderOrderCategory, "ALL">;
  order_status_recognized: boolean;
  order_type: string;
  side: string;
  quantity: string;
  quantity_unit: "LOCAL_INSTRUMENT_QUANTITY";
  price: string | null;
  trigger_price: string | null;
  filled_quantity: string | null;
  filled_price: string | null;
  created_timestamp_ms: number;
  submitted_at: string | null;
  acked_at: string | null;
  last_reconciled_at: string | null;
  trade_count: number;
};

export type SpiderLocalTrade = {
  local_trade_id: string;
  local_order_id: string;
  provider_trade_id: string | null;
  provider_trade_id_status: "RECORDED" | "NOT_RECORDED";
  product_id: string;
  side: string;
  price: string;
  quantity: string;
  quantity_unit: "LOCAL_INSTRUMENT_QUANTITY";
  fee: string | null;
  fee_asset: string | null;
  fee_evidence: "RECORDED" | "INCOMPLETE";
  timestamp_ms: number;
};

export type SpiderOrderFilters = {
  category: SpiderOrderCategory;
  local_order_id: string | null;
  order_created_from_ms: number | null;
  order_created_to_ms: number | null;
};

type SpiderPageBase = {
  schema_version: string;
  scope: { environment: string; execution_scope_id: string; strategy_id: string };
  source: "SQL_LOCAL_FACTS";
  provider_reconciliation: "NOT_ASSERTED";
  observed_at: string;
  filters: SpiderOrderFilters;
  view_version: string;
  limit: number;
  offset: number;
  total_count: number;
  next_offset: number | null;
};

export type SpiderOrdersPage = SpiderPageBase & {
  schema_version: "spider_local_orders_v1";
  orders: SpiderLocalOrder[];
};

export type SpiderTradesPage = SpiderPageBase & {
  schema_version: "spider_local_trades_v1";
  local_order_id: string;
  trades: SpiderLocalTrade[];
};

const ORDER_KEYS = [
  "local_order_id", "client_order_id", "source_client_order_id", "provider_wire_client_order_id",
  "provider_order_id", "provider_order_id_status", "strategy_id", "account_profile", "account_id",
  "exchange_id", "source_product_id", "product_id", "role", "plan_id", "action_id", "operation_id",
  "generation", "configuration_sha256", "transmission_state", "order_status", "category",
  "order_status_recognized", "order_type", "side", "quantity", "quantity_unit", "price",
  "trigger_price", "filled_quantity", "filled_price", "created_timestamp_ms", "submitted_at",
  "acked_at", "last_reconciled_at", "trade_count"
];
const TRADE_KEYS = [
  "local_trade_id", "local_order_id", "provider_trade_id", "provider_trade_id_status", "product_id",
  "side", "price", "quantity", "quantity_unit", "fee", "fee_asset", "fee_evidence", "timestamp_ms"
];
const BASE_KEYS = [
  "schema_version", "scope", "source", "provider_reconciliation", "observed_at", "filters",
  "view_version", "limit", "offset", "total_count", "next_offset"
];
const MIN_UTC_MILLISECONDS = -62_135_596_800_000;
const MAX_UTC_MILLISECONDS = 253_402_300_799_999;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length &&
    actual.every((key, index) => key === [...expected].sort()[index]);
}

function text(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function nullableText(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function int(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function utcMilliseconds(value: unknown): value is number {
  return Number.isSafeInteger(value) &&
    (value as number) >= MIN_UTC_MILLISECONDS &&
    (value as number) <= MAX_UTC_MILLISECONDS;
}

function nullableDecimal(value: unknown): value is string | null {
  return value === null || (typeof value === "string" &&
    /^(?:0|[1-9]\d*)(?:\.\d*[1-9])?$|^-(?:(?:[1-9]\d*)(?:\.\d*[1-9])?|0\.\d*[1-9])$/.test(value));
}

function decimal(value: unknown): value is string {
  return typeof value === "string" && nullableDecimal(value);
}

function utcInstant(value: unknown): value is string {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$/.test(value)) {
    return false;
  }
  const year = Number(value.slice(0, 4));
  return year >= 1 && parseUtcTimestamp(value) !== null;
}

function nullableUtcInstant(value: unknown): value is string | null {
  return value === null || utcInstant(value);
}

function validFilters(value: unknown): value is SpiderOrderFilters {
  if (!record(value) || !exactKeys(value, [
    "category", "local_order_id", "order_created_from_ms", "order_created_to_ms"
  ])) return false;
  return ["ALL", "CURRENT", "PENDING", "HISTORY"].includes(value.category as string) &&
    nullableText(value.local_order_id) &&
    (value.order_created_from_ms === null || utcMilliseconds(value.order_created_from_ms)) &&
    (value.order_created_to_ms === null || utcMilliseconds(value.order_created_to_ms));
}

function validOrder(value: unknown): value is SpiderLocalOrder {
  if (!record(value) || !exactKeys(value, ORDER_KEYS)) return false;
  const nullableIDs = ["client_order_id", "provider_order_id"];
  const nullableMoney = ["price", "trigger_price", "filled_quantity", "filled_price"];
  const nullableTimes = ["submitted_at", "acked_at", "last_reconciled_at"];
  return text(value.local_order_id) && nullableIDs.every((key) => nullableText(value[key])) &&
    text(value.source_client_order_id) && text(value.provider_wire_client_order_id) &&
    value.provider_order_id_status !== undefined &&
    ["RECORDED", "NOT_RECORDED"].includes(value.provider_order_id_status as string) &&
    ["strategy_id", "account_profile", "account_id", "exchange_id", "source_product_id", "product_id",
      "role", "plan_id", "action_id", "operation_id", "transmission_state",
      "order_status", "order_type", "side"].every((key) => text(value[key])) &&
    typeof value.configuration_sha256 === "string" && /^[a-f0-9]{64}$/.test(value.configuration_sha256) &&
    (value.provider_order_id === null) === (value.provider_order_id_status === "NOT_RECORDED") &&
    int(value.generation) && ["CURRENT", "PENDING", "HISTORY"].includes(value.category as string) &&
    typeof value.order_status_recognized === "boolean" &&
    decimal(value.quantity) && nullableMoney.every((key) => nullableDecimal(value[key])) &&
    value.quantity_unit === "LOCAL_INSTRUMENT_QUANTITY" && utcMilliseconds(value.created_timestamp_ms) &&
    nullableTimes.every((key) => nullableUtcInstant(value[key])) && int(value.trade_count);
}

function validTrade(value: unknown): value is SpiderLocalTrade {
  if (!record(value) || !exactKeys(value, TRADE_KEYS)) return false;
  return text(value.local_trade_id) && text(value.local_order_id) &&
    nullableText(value.provider_trade_id) &&
    ["RECORDED", "NOT_RECORDED"].includes(value.provider_trade_id_status as string) &&
    ((value.provider_trade_id === null) === (value.provider_trade_id_status === "NOT_RECORDED")) &&
    text(value.product_id) && text(value.side) && decimal(value.price) &&
    decimal(value.quantity) && value.quantity_unit === "LOCAL_INSTRUMENT_QUANTITY" &&
    nullableDecimal(value.fee) && nullableText(value.fee_asset) &&
    ["RECORDED", "INCOMPLETE"].includes(value.fee_evidence as string) &&
    ((value.fee !== null && value.fee_asset !== null) === (value.fee_evidence === "RECORDED")) &&
    utcMilliseconds(value.timestamp_ms);
}

function validBase(value: Record<string, unknown>, schema: string): boolean {
  if (!exactKeys(value, [...BASE_KEYS, schema === "spider_local_orders_v1" ? "orders" : "local_order_id", ...(schema === "spider_local_trades_v1" ? ["trades"] : [])])) return false;
  const scope = value.scope;
  const filters = value.filters;
  return value.schema_version === schema && record(scope) &&
    exactKeys(scope, ["environment", "execution_scope_id", "strategy_id"]) &&
    text(scope.environment) && text(scope.execution_scope_id) && text(scope.strategy_id) &&
    value.source === "SQL_LOCAL_FACTS" && value.provider_reconciliation === "NOT_ASSERTED" &&
    utcInstant(value.observed_at) && validFilters(filters) &&
    typeof value.view_version === "string" && /^[a-f0-9]{64}$/.test(value.view_version) &&
    int(value.limit) && value.limit >= 1 && value.limit <= 100 && int(value.offset) &&
    int(value.total_count) &&
    (value.next_offset === null || (int(value.next_offset) && value.next_offset > (value.offset as number)));
}

export function decodeSpiderOrdersPage(value: unknown, expectedStrategyId: string): SpiderOrdersPage {
  if (!record(value) || !validBase(value, "spider_local_orders_v1")) throw new Error("invalid_response");
  const scope = value.scope as Record<string, unknown>;
  const orders = value.orders;
  const offset = value.offset as number;
  const totalCount = value.total_count as number;
  const nextOffset = value.next_offset as number | null;
  if (scope.strategy_id !== expectedStrategyId || !Array.isArray(orders) || !orders.every(validOrder) ||
      orders.some((order) => order.strategy_id !== expectedStrategyId) ||
      offset > totalCount || orders.length > (value.limit as number) ||
      !Number.isSafeInteger(offset + orders.length) ||
      offset + orders.length > totalCount ||
      (nextOffset !== null) !== (offset + orders.length < totalCount) ||
      (nextOffset !== null && nextOffset !== offset + orders.length)) throw new Error("invalid_response");
  return value as SpiderOrdersPage;
}

export function decodeSpiderTradesPage(
  value: unknown,
  expectedStrategyId: string,
  expectedOrderId: string,
  expectedViewVersion?: string
): SpiderTradesPage {
  if (!record(value) || !validBase(value, "spider_local_trades_v1")) throw new Error("invalid_response");
  const scope = value.scope as Record<string, unknown>;
  const trades = value.trades;
  const offset = value.offset as number;
  const totalCount = value.total_count as number;
  const nextOffset = value.next_offset as number | null;
  if (scope.strategy_id !== expectedStrategyId || value.local_order_id !== expectedOrderId ||
      (expectedViewVersion !== undefined && value.view_version !== expectedViewVersion) ||
      !Array.isArray(trades) || !trades.every(validTrade) ||
      offset > totalCount || trades.length > (value.limit as number) ||
      !Number.isSafeInteger(offset + trades.length) ||
      offset + trades.length > totalCount ||
      trades.some((trade) => trade.local_order_id !== expectedOrderId) ||
      (nextOffset !== null) !== (offset + trades.length < totalCount) ||
      (nextOffset !== null && nextOffset !== offset + trades.length)) throw new Error("invalid_response");
  return value as SpiderTradesPage;
}

export function parseSpiderUtcRange(
  fromText: string,
  toText: string
): { from: number | null; to: number | null } | null {
  const fromRaw = fromText.trim();
  const toRaw = toText.trim();
  if ((fromRaw === "") !== (toRaw === "")) return null;
  if (fromRaw === "" && toRaw === "") return { from: null, to: null };
  const strictUtc = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|\+00:00)$/;
  if (!strictUtc.test(fromRaw) || !strictUtc.test(toRaw) ||
      Number(fromRaw.slice(0, 4)) < 1 || Number(toRaw.slice(0, 4)) < 1) return null;
  const from = parseUtcTimestamp(fromRaw);
  const to = parseUtcTimestamp(toRaw);
  return from !== null && to !== null && from < to ? { from, to } : null;
}
