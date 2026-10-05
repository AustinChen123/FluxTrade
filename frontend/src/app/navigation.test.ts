import { describe, expect, it } from "vitest";

import {
  parseDemoMode,
  parseNavigation,
  serializeNavigation,
  type View
} from "./navigation";

describe("parseDemoMode", () => {
  it.each([
    ["/console", true, false],
    ["/console?demo=0", true, false],
    ["/console?demo=1", false, false],
    ["/console?demo=1", true, true],
    ["/console?demo=1&demo=0", true, true],
    ["/console?demo=0&demo=1", true, false]
  ] as const)("classifies %s with dev=%s", (relativeUrl, dev, expected) => {
    expect(
      parseDemoMode(new URL(relativeUrl, "https://console.example"), dev)
    ).toBe(expected);
  });
});

describe("parseNavigation", () => {
  it.each([
    ["", "research", null],
    ["?view=unknown&trade=ignored", "research", null],
    ["?view=research&trade=ignored", "research", null],
    ["?view=results&trade=ignored", "results", null],
    ["?view=strategies&trade=ignored", "strategies", null],
    ["?view=trades", "trades", null],
    ["?view=trades&trade=", "trades", null],
    ["?view=trades&trade=%01bad", "trades", null],
    [`?view=trades&trade=${"x".repeat(257)}`, "trades", null],
    ["?view=trades&result=run-1&trade=trade-1", "trades", "trade-1"],
    [
      "?view=trades&view=results&result=run-1&trade=first&trade=second",
      "trades",
      "first"
    ],
    ["?view=unknown&view=trades&trade=valid", "research", null],
    ["?view=trades&result=run-1&trade=%01bad&trade=good", "trades", null],
    ["?view=trades&result=run-1&trade=good&trade=%01bad", "trades", "good"],
    ["?view=trades&result=run-1&trade=&trade=good", "trades", null],
    ["?view=trades&result=run-1&trade=A+B", "trades", "A B"]
  ] as const)(
    "classifies %s as %s with trade %s",
    (search, view, inspectedTradeId) => {
      const selectedResultId = new URLSearchParams(search).get("result");
      expect(parseNavigation(search)).toEqual({
        view,
        selectedResultId:
          view === "results" || view === "trades" ? selectedResultId : null,
        inspectedTradeId:
          view === "trades" && selectedResultId !== null
            ? inspectedTradeId
            : null
      });
    }
  );
});

describe("result navigation identity", () => {
  it.each([
    ["?view=results&result=run-1", "results", "run-1", null],
    [
      "?view=trades&result=run-1&trade=run-1%3A7",
      "trades",
      "run-1",
      "run-1:7"
    ],
    ["?view=trades&trade=run-1%3A7", "trades", null, null],
    ["?view=research&result=run-1&trade=run-1%3A7", "research", null, null],
    [
      "?view=strategies&result=run-1&trade=run-1%3A7",
      "strategies",
      null,
      null
    ],
    ["?view=results&result=&result=run-2", "results", null, null],
    ["?view=results&result=first&result=second", "results", "first", null],
    [
      "?view=trades&result=first&result=second&trade=t-1",
      "trades",
      "first",
      "t-1"
    ],
    ["?view=trades&result=   &trade=t-1", "trades", null, null],
    ["?view=trades&result=bad%00id&trade=t-1", "trades", null, null],
    ["?view=trades&result=bad%7Fid&trade=t-1", "trades", null, null],
    ["?view=trades&result=bad%C2%80id&trade=t-1", "trades", null, null],
    [`?view=results&result=${"x".repeat(128)}`, "results", "x".repeat(128), null],
    [`?view=results&result=${"x".repeat(129)}`, "results", null, null]
  ] as const)("parses %s", (search, view, selectedResultId, inspectedTradeId) => {
    expect(parseNavigation(search)).toEqual({
      view,
      selectedResultId,
      inspectedTradeId
    });
  });

  it.each([
    ["research", null, null, "/console?keep=1#anchor"],
    [
      "strategies",
      "run-1",
      "trade-1",
      "/console?keep=1&view=strategies#anchor"
    ],
    [
      "results",
      "run-1",
      "trade-1",
      "/console?result=run-1&keep=1&view=results#anchor"
    ],
    ["results", "bad\u0001", "trade-1", "/console?keep=1&view=results#anchor"],
    [
      "trades",
      "run-1",
      "trade-1",
      "/console?result=run-1&trade=trade-1&keep=1&view=trades#anchor"
    ],
    [
      "trades",
      null,
      "trade-1",
      "/console?keep=1&view=trades#anchor"
    ],
    [
      "trades",
      "run-1",
      "bad\u009f",
      "/console?result=run-1&trade=bad%C2%9F&keep=1&view=trades#anchor"
    ]
  ] as const)(
    "serializes %s with result %s and trade %s",
    (view, resultId, tradeId, expectedUrl) => {
      const result = serializeNavigation(
        new URL(
          "/console?result=old&trade=old&keep=1#anchor",
          "https://console.example"
        ),
        view,
        tradeId,
        resultId
      );
      expect(result.relativeUrl).toBe(expectedUrl);
      expect(result.selectedResultId).toBe(
        new URL(
          result.relativeUrl,
          "https://console.example"
        ).searchParams.get("result")
      );
      expect(result.inspectedTradeId).toBe(
        view === "trades" && resultId ? tradeId : null
      );
    }
  );

  it("preserves a valid existing result for three-argument callers", () => {
    const result = serializeNavigation(
      new URL(
        "/console?view=results&result=run-1&trade=old",
        "https://console.example"
      ),
      "results",
      null
    );
    expect(result.relativeUrl).toBe("/console?view=results&result=run-1");
  });
});

describe("serializeNavigation", () => {
  it.each([
    [
      "/console?keep=1#anchor",
      "research",
      null,
      null,
      "/console?keep=1#anchor"
    ],
    [
      "/console?view=results&keep=1#anchor",
      "strategies",
      null,
      null,
      "/console?view=strategies&keep=1#anchor"
    ],
    [
      "/console?keep=1#anchor",
      "trades",
      "trade-1",
      "trade-1",
      "/console?keep=1&view=trades&trade=trade-1&result=run-1#anchor"
    ],
    [
      "/console?view=results&trade=old&keep=1#anchor",
      "trades",
      "new",
      "new",
      "/console?view=trades&trade=new&keep=1&result=run-1#anchor"
    ],
    [
      "/console?view=results&view=trades&trade=old&trade=older&keep=1&keep=2#anchor",
      "trades",
      "new",
      "new",
      "/console?view=trades&trade=new&keep=1&keep=2&result=run-1#anchor"
    ],
    [
      "/console?view=results&view=trades&trade=old&trade=older&keep=1&keep=2#anchor",
      "research",
      null,
      null,
      "/console?keep=1&keep=2#anchor"
    ],
    [
      "/console?trade=old&keep=1#anchor",
      "trades",
      "A B",
      "A B",
      "/console?trade=A+B&keep=1&view=trades&result=run-1#anchor"
    ],
    [
      "/console?trade=old&keep=1#anchor",
      "trades",
      "invalid\u0001",
      null,
      "/console?keep=1&view=trades&result=run-1#anchor"
    ],
    [
      "/console?keep=a%20b&tilde=~#a%20b",
      "strategies",
      null,
      null,
      "/console?keep=a+b&tilde=%7E&view=strategies#a%20b"
    ]
  ] as const)(
    "serializes %s with %s/%s",
    (relativeUrl, view, requestedTradeId, inspectedTradeId, expectedUrl) => {
      const result = serializeNavigation(
        new URL(relativeUrl, "https://console.example"),
        view as View,
        requestedTradeId,
        view === "trades" ? "run-1" : null
      );

      expect(result).toEqual({
        selectedResultId: view === "trades" ? "run-1" : null,
        inspectedTradeId,
        relativeUrl: expectedUrl
      });
    }
  );
});
