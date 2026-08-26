import { describe, expect, it } from "vitest";

import { en } from "./en";
import { zhTW } from "./zh-TW";

function canonicalize(value: unknown): unknown {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError("locale payload numbers must be finite");
    }
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalize((value as Record<string, unknown>)[key])])
    );
  }
  throw new TypeError("locale payload must contain plain canonical values");
}

async function digest(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(canonicalize(value)));
  const result = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(result)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function assertKeyParity(left: unknown, right: unknown, path = "root"): void {
  expect(Array.isArray(left), `${path} array shape`).toBe(Array.isArray(right));
  if (Array.isArray(left) && Array.isArray(right)) {
    expect(left).toHaveLength(right.length);
    left.forEach((value, index) => assertKeyParity(value, right[index], `${path}[${index}]`));
    return;
  }
  const leftIsObject = left !== null && typeof left === "object";
  const rightIsObject = right !== null && typeof right === "object";
  expect(leftIsObject, `${path} object shape`).toBe(rightIsObject);
  if (
    leftIsObject &&
    rightIsObject
  ) {
    const leftKeys = Object.keys(left).sort();
    const rightKeys = Object.keys(right).sort();
    if (path === "root.translation.candidates") {
      expect(leftKeys).toEqual([
        "count",
        "drawdown",
        "kicker",
        "metrics",
        "selected",
        "title"
      ]);
      expect(rightKeys).toEqual([
        "count_one",
        "count_other",
        "drawdown",
        "kicker",
        "metrics",
        "selected",
        "title"
      ]);
      for (const key of ["drawdown", "kicker", "metrics", "selected", "title"]) {
        assertKeyParity(
          (left as Record<string, unknown>)[key],
          (right as Record<string, unknown>)[key],
          `${path}.${key}`
        );
      }
      return;
    }
    expect(leftKeys, `${path} keys`).toEqual(rightKeys);
    for (const key of leftKeys) {
      assertKeyParity(
        (left as Record<string, unknown>)[key],
        (right as Record<string, unknown>)[key],
        `${path}.${key}`
      );
    }
  }
}

describe("locale resources", () => {
  it("preserves exact recursive key parity", () => {
    assertKeyParity(zhTW, en);
  });

  it("preserves the frozen complete locale payload", async () => {
    await expect(digest({ "zh-TW": zhTW, en })).resolves.toBe(
      "6e297417fb33da334c5a7483e168ecdf43b0b62f50967bf4b430e9fbea5076b6"
    );
  });

  it.each([undefined, Number.NaN, Number.POSITIVE_INFINITY, new Date(0)])(
    "rejects non-canonical payload value %p",
    (value) => {
      expect(() => canonicalize(value)).toThrow(TypeError);
    }
  );
});
