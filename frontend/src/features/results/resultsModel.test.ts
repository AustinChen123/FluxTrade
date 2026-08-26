import { describe, expect, it } from "vitest";

import type { ClosedTrade } from "../../shared/trading/closedTrade";
import {
  mergeTradeItems,
  validDistributionBuckets,
  validLoadedTradePage,
  validTradePage,
  validateEquitySamples
} from "./resultsModel";

const trade: ClosedTrade = {
  id: "trade-1",
  side: "LONG",
  quantity: "1",
  entryTime: "2026-01-01T00:00:00Z",
  entryPrice: "100.00",
  exitTime: "2026-01-01T00:05:00Z",
  exitPrice: "101.00",
  fee: "0.25",
  pnl: "0.75"
};

const equitySample = {
  timestamp: "2026-01-01T00:00:00Z",
  equity: "100000.00",
  drawdown: "0.00"
};

describe("resultsModel", () => {
  it("preserves an exact valid equity sequence by identity", () => {
    const samples = [
      equitySample,
      {
        timestamp: "2026-01-01T00:05:00Z",
        equity: "100000.0000000000000000000001",
        drawdown: "0.0000000000000000000001"
      }
    ];

    expect(validateEquitySamples(samples)).toBe(samples);
  });

  it.each(["0x10", "1e2", "", "Infinity"])(
    "rejects non-decimal equity value %j",
    (equity) => {
      expect(validateEquitySamples([{ ...equitySample, equity }])).toBeNull();
    }
  );

  it.each([
    ["duplicate", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
    ["descending", "2026-01-01T00:05:00Z", "2026-01-01T00:00:00Z"]
  ])("rejects %s equity timestamps", (_name, first, second) => {
    expect(
      validateEquitySamples([
        { ...equitySample, timestamp: first },
        { ...equitySample, timestamp: second }
      ])
    ).toBeNull();
  });

  it.each([-1, 1.5, Number.NaN])(
    "rejects invalid distribution count %s",
    (count) => {
      expect(
        validDistributionBuckets([{ lower: null, upper: "0", count }])
      ).toBe(false);
    }
  );

  it("distinguishes a page shape from a complete loaded page", () => {
    const partialWithoutCursor = {
      items: [trade],
      totalCount: 2,
      nextCursor: null
    };

    expect(validTradePage(partialWithoutCursor)).toBe(true);
    expect(validLoadedTradePage(partialWithoutCursor)).toBe(false);
    expect(
      validLoadedTradePage({ items: [], totalCount: 0, nextCursor: null })
    ).toBe(true);
  });

  it("rejects malformed cursors, totals, IDs, and duplicate IDs", () => {
    expect(
      validTradePage({ items: [trade], totalCount: 1, nextCursor: " " })
    ).toBe(false);
    expect(
      validTradePage({ items: [trade], totalCount: 0, nextCursor: null })
    ).toBe(false);
    expect(
      validTradePage({
        items: [{ ...trade, id: "" }],
        totalCount: 1,
        nextCursor: null
      })
    ).toBe(false);
    expect(
      validTradePage({
        items: [trade, { ...trade }],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
  });

  it.each([
    ["negative total", { items: [], totalCount: -1, nextCursor: null }],
    ["fractional total", { items: [], totalCount: 0.5, nextCursor: null }],
    ["blank cursor", { items: [], totalCount: 1, nextCursor: " " }],
    [
      "complete page with a cursor",
      { items: [trade], totalCount: 1, nextCursor: "cursor-1" }
    ],
    [
      "incomplete page without a cursor",
      { items: [trade], totalCount: 2, nextCursor: null }
    ]
  ] satisfies Array<
    [
      string,
      {
        items: ClosedTrade[];
        totalCount: number;
        nextCursor: string | null;
      }
    ]
  >)(
    "rejects an invalid loaded-page state: %s",
    (_name, page) => {
      expect(validLoadedTradePage(page)).toBe(false);
    }
  );

  it("merges new and identical trades but rejects a conflicting duplicate", () => {
    const second = { ...trade, id: "trade-2" };

    const merged = mergeTradeItems([trade], [{ ...trade }, second]);
    expect(merged).toEqual([trade, second]);
    expect(merged?.[0]).toBe(trade);
    expect(mergeTradeItems([trade], [{ ...trade, pnl: "999.00" }])).toBeNull();
  });

  it.each([
    ["side", { side: "SHORT" }],
    ["quantity", { quantity: "2" }],
    ["entryTime", { entryTime: "2026-01-01T00:01:00Z" }],
    ["entryPrice", { entryPrice: "100.0" }],
    ["exitTime", { exitTime: "2026-01-01T00:06:00Z" }],
    ["exitPrice", { exitPrice: "101.0" }],
    ["fee", { fee: "0.250" }],
    ["pnl", { pnl: "0.750" }]
  ] satisfies Array<[string, Partial<ClosedTrade>]>)(
    "rejects a duplicate ID whose %s differs by strict value",
    (_field, change) => {
      expect(mergeTradeItems([trade], [{ ...trade, ...change }])).toBeNull();
    }
  );

  it("rejects duplicate IDs within one incoming page before merging", () => {
    expect(
      validTradePage({
        items: [trade, trade],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
    expect(
      validTradePage({
        items: [trade, { ...trade }],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
    expect(
      validTradePage({
        items: [trade, { ...trade, fee: "0.250" }],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
  });
});
