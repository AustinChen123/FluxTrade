import { describe, expect, it } from "vitest";

import type { Epoch, Gene } from "../../api";
import { demoEpoch } from "./demo";
import {
  compareGenes,
  epochObjective,
  finiteNumber,
  fitnessObservationRows,
  fitnessSurfaceRows,
  numericParameterNames,
  parameterDimensions,
  selectBestGene
} from "./gaDomain";

function gene(
  id: number,
  paramPack: Record<string, unknown>,
  score = "1.5"
): Gene {
  return {
    id,
    strategy_id: "strategy",
    role: "challenger",
    param_pack: paramPack,
    score_total: score,
    score_breakdown: {},
    max_drawdown: "0.2",
    generation_index: 0,
    candidate_id: `candidate-${id}`,
    epoch_id: "epoch",
    created_at: "2026-07-28T00:00:00Z"
  };
}

describe("GA visualization transforms", () => {
  it("keeps invalid values out of numeric chart axes", () => {
    expect(finiteNumber("1.25")).toBe(1.25);
    expect(finiteNumber("")).toBeNull();
    expect(finiteNumber("not-a-number")).toBeNull();
    expect(finiteNumber(null)).toBeNull();
  });

  it("classifies numeric and categorical parameters once for every chart", () => {
    const genes = [
      gene(1, { fast: 5, slow: "20", session: "rth" }),
      gene(2, { fast: 8, slow: "34", session: "full" })
    ];

    expect(numericParameterNames(genes)).toEqual(["fast", "slow"]);
    expect(parameterDimensions(genes)).toEqual([
      { name: "fast", type: "value" },
      { name: "session", type: "category", categories: ["full", "rth"] },
      { name: "slow", type: "value" }
    ]);
  });

  it("matches backend drawdown risk ordering for signed values and score ties", () => {
    const epoch: Epoch = {
      ...demoEpoch,
      config_json: { objective: "minimize_drawdown" }
    };
    const genes = [
      { ...gene(1, { fast: 5, slow: 20 }, "9"), max_drawdown: "-0.3" },
      { ...gene(2, { fast: 5, slow: 20 }, "1"), max_drawdown: "-0.1" },
      { ...gene(3, { fast: 5, slow: 20 }, "2"), max_drawdown: "0.1" }
    ];

    expect(selectBestGene(epoch, genes)?.id).toBe(3);
    expect([...genes].sort((left, right) => compareGenes(epoch, left, right)))
      .toEqual([genes[2], genes[1], genes[0]]);
    expect(fitnessSurfaceRows(epoch, genes, "fast", "slow")).toEqual([
      [5, 20, 0.1, 3, "candidate-3"]
    ]);
  });

  it("preserves Numeric(18,8) ordering beyond JavaScript Number precision", () => {
    const genes = [
      gene(1, { fast: 5, slow: 20 }, "9999999999.99999998"),
      gene(2, { fast: 5, slow: 20 }, "9999999999.99999999")
    ];

    expect(Number(genes[0].score_total)).toBe(Number(genes[1].score_total));
    expect(selectBestGene(demoEpoch, genes)?.id).toBe(2);
    expect(fitnessSurfaceRows(demoEpoch, genes, "fast", "slow")).toEqual([
      [5, 20, 10_000_000_000, 2, "candidate-2"]
    ]);
  });

  it("keeps every valid observation while deduplicating interpolation rows", () => {
    const genes = [
      gene(1, { fast: 5, slow: 20 }, "1"),
      gene(2, { fast: 5, slow: 20 }, "2")
    ];

    expect(fitnessSurfaceRows(demoEpoch, genes, "fast", "slow")).toHaveLength(1);
    expect(
      fitnessObservationRows(demoEpoch, genes, "fast", "slow").map(
        (row) => row[3]
      )
    ).toEqual([1, 2]);
  });

  it("fails closed for an unsupported optimization objective", () => {
    const epoch = {
      ...demoEpoch,
      config_json: { objective: "maximize_magic" }
    };
    const genes = [gene(1, { fast: 5, slow: 20 }, "1")];

    expect(epochObjective(epoch)).toBeNull();
    expect(selectBestGene(epoch, genes)).toBeNull();
    expect(fitnessSurfaceRows(epoch, genes, "fast", "slow")).toEqual([]);
    expect(fitnessObservationRows(epoch, genes, "fast", "slow")).toEqual([]);
  });
});
