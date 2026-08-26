import { describe, expect, it } from "vitest";

import { parseUtcTimestamp } from "./utc";

describe("UTC timestamp parsing", () => {
  it.each([
    "2026-08-27T12:34:56Z",
    "2026-08-27T12:34:56.123456Z",
    "2026-08-27T12:34:56+00:00"
  ])("accepts an exact UTC timestamp %s", (value) => {
    expect(parseUtcTimestamp(value)).toBe(Date.parse(value));
  });

  it.each([
    "2026-02-30T12:34:56Z",
    "2026-08-27T12:34:56",
    "2026-08-27T12:34:56+02:00",
    "2026-8-27T12:34:56Z",
    "not-a-timestamp"
  ])("rejects a non-canonical UTC timestamp %s", (value) => {
    expect(parseUtcTimestamp(value)).toBeNull();
  });
});
