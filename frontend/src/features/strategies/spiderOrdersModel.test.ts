import { describe, expect, it } from "vitest";

import {
  decodeSpiderOrdersPage,
  decodeSpiderTradesPage,
  parseSpiderUtcRange
} from "./spiderOrdersModel";
import {
  spiderTestOrder as order,
  spiderTestOrdersPage,
  spiderTestTrade as trade,
  spiderTestTradesPage
} from "./spiderOrders.test-fixtures";

describe("closed Spider local-fact DTO decoder", () => {
  it("preserves Decimal strings and distinct nullable local/provider identities", () => {
    const page = decodeSpiderOrdersPage(spiderTestOrdersPage, "exact-strategy");
    const tradePage = decodeSpiderTradesPage(spiderTestTradesPage, "exact-strategy", order.local_order_id, page.view_version);
    expect(page.orders[0].quantity).toBe("0.01");
    expect(page.orders[0].price).toBe("12345.67");
    expect(page.orders[0].source_client_order_id).toBe("source-001");
    expect(page.orders[0].provider_order_id).toBeNull();
    expect(page.orders[0].order_status).toBe("FUTURE_STATUS");
    expect(page.orders[0].order_status_recognized).toBe(false);
    expect(tradePage.trades[0].fee).toBe("-0.00000012");
    expect(tradePage.trades[0].provider_trade_id).toBeNull();
    expect(tradePage.view_version).toBe(page.view_version);
  });

  it.each([
    ["float monetary field", { ...order, price: 12.5 }],
    ["missing required order quantity", { ...order, quantity: null }],
    ["missing source client order id", { ...order, source_client_order_id: null }],
    ["missing provider wire client order id", { ...order, provider_wire_client_order_id: null }],
    ["exponent decimal", { ...order, price: "1.2e3" }],
    ["unknown order field", { ...order, pnl: "1" }],
    ["wrong strategy scope", order]
  ])("rejects %s instead of guessing or coercing", (label, value) => {
    const strategyId = label === "wrong strategy scope" ? "other-strategy" : "exact-strategy";
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage, orders: [value]
    }, strategyId)).toThrow("invalid_response");
  });

  it("rejects a trade whose local linkage or parent view version differs", () => {
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage, local_order_id: order.local_order_id,
      trades: [{ ...trade, local_order_id: "another-order" }]
    }, "exact-strategy", order.local_order_id)).toThrow("invalid_response");
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage, local_order_id: order.local_order_id,
      view_version: "c".repeat(64)
    }, "exact-strategy", order.local_order_id, spiderTestOrdersPage.view_version)).toThrow("invalid_response");
  });

  it("rejects missing trade quantity or price while preserving nullable optional IDs and order prices", () => {
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage, trades: [{ ...trade, quantity: null }]
    }, "exact-strategy", order.local_order_id)).toThrow("invalid_response");
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage, trades: [{ ...trade, price: null }]
    }, "exact-strategy", order.local_order_id)).toThrow("invalid_response");
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage, orders: [{ ...order, price: null, trigger_price: null, filled_price: null }]
    }, "exact-strategy")).not.toThrow();
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage, orders: [{ ...order, client_order_id: null, provider_order_id: null }]
    }, "exact-strategy")).not.toThrow();
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage, trades: [{ ...trade, provider_trade_id: null }]
    }, "exact-strategy", order.local_order_id)).not.toThrow();
  });

  it("accepts epoch milliseconds across the supported UTC year range and rejects out-of-range values", () => {
    const minimum = -62_135_596_800_000;
    const maximum = 253_402_300_799_999;
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage,
      observed_at: "0001-01-01T00:00:00.000001Z",
      filters: { ...spiderTestOrdersPage.filters, order_created_from_ms: minimum, order_created_to_ms: maximum },
      orders: [{ ...order, created_timestamp_ms: minimum, submitted_at: "0001-01-01T00:00:00Z" }]
    }, "exact-strategy")).not.toThrow();
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage,
      observed_at: "9999-12-31T23:59:59.999999Z",
      trades: [{ ...trade, timestamp_ms: maximum }]
    }, "exact-strategy", order.local_order_id)).not.toThrow();
    for (const timestamp of [minimum - 1, maximum + 1, Number.MAX_SAFE_INTEGER]) {
      expect(() => decodeSpiderOrdersPage({
        ...spiderTestOrdersPage, orders: [{ ...order, created_timestamp_ms: timestamp }]
      }, "exact-strategy")).toThrow("invalid_response");
    }
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage,
      observed_at: "0000-12-31T23:59:59Z"
    }, "exact-strategy")).toThrow("invalid_response");
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage,
      observed_at: "10000-01-01T00:00:00Z"
    }, "exact-strategy")).toThrow("invalid_response");
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage,
      observed_at: "2026-02-30T12:00:00Z"
    }, "exact-strategy")).toThrow("invalid_response");
  });

  it("rejects mixed strategy rows and inconsistent pagination counts", () => {
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage,
      orders: [{ ...order, strategy_id: "another-strategy" }]
    }, "exact-strategy")).toThrow("invalid_response");
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage, total_count: 0
    }, "exact-strategy")).toThrow("invalid_response");
    expect(() => decodeSpiderOrdersPage({
      ...spiderTestOrdersPage, offset: 2, total_count: 1, orders: [], next_offset: null
    }, "exact-strategy")).toThrow("invalid_response");
    expect(() => decodeSpiderTradesPage({
      ...spiderTestTradesPage, offset: 0, total_count: 0
    }, "exact-strategy", order.local_order_id)).toThrow("invalid_response");
  });
});

describe("strict UTC creation range", () => {
  it("accepts valid paired UTC millisecond inputs and leaves no-range values null", () => {
    expect(parseSpiderUtcRange("2026-10-11T12:00:00.123Z", "2026-10-11T13:00:00Z"))
      .toEqual({ from: Date.parse("2026-10-11T12:00:00.123Z"), to: Date.parse("2026-10-11T13:00:00Z") });
    expect(parseSpiderUtcRange("", "")).toEqual({ from: null, to: null });
  });

  it.each([
    ["one endpoint", "2026-10-11T12:00:00Z", ""],
    ["year zero", "0000-12-31T00:00:00Z", "0001-01-01T00:00:00Z"],
    ["year out of range", "9999-12-31T00:00:00Z", "10000-01-01T00:00:00Z"],
    ["too many fractional digits", "2026-10-11T12:00:00.1234Z", "2026-10-11T13:00:00Z"],
    ["non-UTC offset", "2026-10-11T12:00:00+01:00", "2026-10-11T13:00:00Z"],
    ["reversed range", "2026-10-11T13:00:00Z", "2026-10-11T12:00:00Z"],
    ["invalid calendar date", "2026-02-30T12:00:00Z", "2026-03-01T12:00:00Z"]
  ])("rejects %s", (_label, from, to) => {
    expect(parseSpiderUtcRange(from, to)).toBeNull();
  });
});
