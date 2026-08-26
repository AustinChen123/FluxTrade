import { describe, expect, it } from "vitest";

import {
  buildDemoGenes,
  demoEpoch,
  demoGenes,
  demoSummaries
} from "./demo";

function canonicalize(value: unknown): unknown {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    (typeof value === "number" && Number.isFinite(value))
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype
  ) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [
          key,
          canonicalize((value as Record<string, unknown>)[key])
        ])
    );
  }
  throw new TypeError("demo canonicalizer accepts only JSON data");
}

async function sha256(value: unknown): Promise<string> {
  const bytes = new TextEncoder().encode(JSON.stringify(canonicalize(value)));
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

describe("research demo fixture", () => {
  it("preserves the complete deterministic payload digest", async () => {
    const genesByGeneration = Array.from({ length: 20 }, (_, generation) =>
      buildDemoGenes(generation)
    );

    expect(
      await sha256({
        demoEpoch,
        demoSummaries,
        genesByGeneration,
        demoGenes
      })
    ).toBe("7b8badf5ebe6e78fbd186b98cfa54e4b3ebfb7fc85f6c912ed6b155fb5b072f2");
  });

  it("rejects non-JSON classes in the canonicalizer", () => {
    expect(() => canonicalize(new Date())).toThrow(
      "demo canonicalizer accepts only JSON data"
    );
    expect(() => canonicalize(Number.NaN)).toThrow(
      "demo canonicalizer accepts only JSON data"
    );
  });
});
