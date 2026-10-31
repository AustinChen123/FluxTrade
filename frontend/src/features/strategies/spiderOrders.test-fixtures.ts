export const spiderTestOrder = {
  local_order_id: "local-order-001", client_order_id: "client-001", source_client_order_id: "source-001",
  provider_wire_client_order_id: "wire-001", provider_order_id: null, provider_order_id_status: "NOT_RECORDED",
  strategy_id: "exact-strategy", account_profile: "profile-a", account_id: "account-a", exchange_id: "okx",
  source_product_id: "BTC-USDT-SWAP", product_id: "BTC-USDT-SWAP", role: "GRID", plan_id: "plan-old",
  action_id: "action-old", operation_id: "operation-old", generation: 2, configuration_sha256: "a".repeat(64),
  transmission_state: "SEND_UNKNOWN", order_status: "FUTURE_STATUS", category: "PENDING",
  order_status_recognized: false, order_type: "LIMIT", side: "SHORT", quantity: "0.01",
  quantity_unit: "LOCAL_INSTRUMENT_QUANTITY", price: "12345.67", trigger_price: null,
  filled_quantity: "0", filled_price: null, created_timestamp_ms: 1791763200123,
  submitted_at: "2026-10-11T12:00:00.123456Z", acked_at: null, last_reconciled_at: null, trade_count: 2
} as const;

export const spiderTestTrade = {
  local_trade_id: "local-trade-001", local_order_id: spiderTestOrder.local_order_id, provider_trade_id: null,
  provider_trade_id_status: "NOT_RECORDED", product_id: spiderTestOrder.product_id, side: "SHORT", price: "12345.67",
  quantity: "0.002", quantity_unit: "LOCAL_INSTRUMENT_QUANTITY", fee: "-0.00000012", fee_asset: "USDT",
  fee_evidence: "RECORDED", timestamp_ms: 1791763200456
} as const;

export const spiderTestOrdersPage = {
  schema_version: "spider_local_orders_v1",
  scope: { environment: "local-test", execution_scope_id: "scope-1", strategy_id: "exact-strategy" },
  source: "SQL_LOCAL_FACTS", provider_reconciliation: "NOT_ASSERTED",
  observed_at: "2026-10-12T01:02:03.123456Z",
  filters: { category: "ALL", local_order_id: null, order_created_from_ms: null, order_created_to_ms: null },
  view_version: "b".repeat(64), limit: 20, offset: 0, total_count: 1, next_offset: null,
  orders: [spiderTestOrder]
} as const;

export const spiderTestTradesPage = {
  schema_version: "spider_local_trades_v1",
  scope: spiderTestOrdersPage.scope,
  source: spiderTestOrdersPage.source,
  provider_reconciliation: spiderTestOrdersPage.provider_reconciliation,
  observed_at: spiderTestOrdersPage.observed_at,
  filters: spiderTestOrdersPage.filters,
  view_version: spiderTestOrdersPage.view_version,
  limit: spiderTestOrdersPage.limit,
  offset: spiderTestOrdersPage.offset,
  total_count: spiderTestOrdersPage.total_count,
  next_offset: spiderTestOrdersPage.next_offset,
  local_order_id: spiderTestOrder.local_order_id,
  trades: [spiderTestTrade]
} as const;
