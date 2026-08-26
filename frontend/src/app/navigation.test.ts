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
    ["?view=trades&trade=trade-1", "trades", "trade-1"],
    [
      "?view=trades&view=results&trade=first&trade=second",
      "trades",
      "first"
    ],
    ["?view=unknown&view=trades&trade=valid", "research", null],
    ["?view=trades&trade=%01bad&trade=good", "trades", null],
    ["?view=trades&trade=good&trade=%01bad", "trades", "good"],
    ["?view=trades&trade=&trade=good", "trades", null],
    ["?view=trades&trade=A+B", "trades", "A B"]
  ] as const)(
    "classifies %s as %s with trade %s",
    (search, view, inspectedTradeId) => {
      expect(parseNavigation(search)).toEqual({ view, inspectedTradeId });
    }
  );
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
      "/console?keep=1&view=trades&trade=trade-1#anchor"
    ],
    [
      "/console?view=results&trade=old&keep=1#anchor",
      "trades",
      "new",
      "new",
      "/console?view=trades&trade=new&keep=1#anchor"
    ],
    [
      "/console?view=results&view=trades&trade=old&trade=older&keep=1&keep=2#anchor",
      "trades",
      "new",
      "new",
      "/console?view=trades&trade=new&keep=1&keep=2#anchor"
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
      "/console?trade=A+B&keep=1&view=trades#anchor"
    ],
    [
      "/console?trade=old&keep=1#anchor",
      "trades",
      "invalid\u0001",
      null,
      "/console?keep=1&view=trades#anchor"
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
        requestedTradeId
      );

      expect(result).toEqual({ inspectedTradeId, relativeUrl: expectedUrl });
    }
  );
});
