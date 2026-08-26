import { describe, expect, it } from "vitest";

import { finiteDecimalNumber, isDecimalString } from "./decimal";

describe("shared Decimal presentation boundary", () => {
  it.each([
    ["1", 1],
    ["+1.0", 1],
    ["-0", -0],
    ["1.", 1],
    [".5", 0.5],
    ["-.5", -0.5],
    ["  +12.50  ", 12.5]
  ] as const)("accepts exact ASCII Decimal grammar %p", (value, expected) => {
    expect(isDecimalString(value)).toBe(true);
    expect(finiteDecimalNumber(value)).toBe(expected);
  });

  it.each([
    "1e2",
    "0x10",
    "",
    "   ",
    "Infinity",
    "NaN",
    "１２",
    "١٢",
    null,
    { value: "1" }
  ])("rejects non-contract value %p", (value) => {
    expect(isDecimalString(value)).toBe(false);
    expect(finiteDecimalNumber(value)).toBeNull();
  });

  it("rejects a grammar-valid magnitude whose Number conversion overflows", () => {
    const value = `1${"0".repeat(400)}`;
    expect(isDecimalString(value)).toBe(true);
    expect(finiteDecimalNumber(value)).toBeNull();
  });
});
