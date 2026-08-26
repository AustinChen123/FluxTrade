import { describe, expect, it } from "vitest";

import type { Epoch, Gene } from "../../api";
import { compareGenes, finiteNumber } from "./gaDomain";
import {
  buildResearchModel,
  displayNumber,
  displayValue,
  geneIdFromChartData
} from "./researchModel";

const epoch: Epoch = {
  id: "epoch",
  strategy_id: "strategy",
  started_at: "2026-07-28T00:00:00Z",
  finished_at: "2026-07-28T01:00:00Z",
  pop_size: 2,
  max_generations: 1,
  generations_run: 1,
  best_score: "1",
  seed: 1,
  config_json: { objective: "maximize_score" },
  status: "completed",
  eval_pair: "RITHMIC:MNQ_ROLL-PERP",
  eval_start_date: "2026-01-01",
  eval_end_date: "2026-07-28",
  eval_timeframe: "5m"
};

function gene(id: number, score: string, value: unknown = id): Gene {
  return {
    id,
    strategy_id: "strategy",
    role: "challenger",
    param_pack: { fast: value, slow: id + 1 },
    score_total: score,
    score_breakdown: {},
    max_drawdown: "0.1",
    generation_index: 0,
    candidate_id: `candidate-${id}`,
    epoch_id: epoch.id,
    created_at: "2026-07-28T01:00:00Z"
  };
}

describe("research model boundaries", () => {
  it.each([
    [1, 1],
    [-1.25, -1.25],
    ["1", 1],
    ["+1.25", 1.25],
    [" -1.25 ", -1.25],
    ["1e2", 100],
    ["0x10", 16],
    [".5", 0.5],
    ["1.", 1],
    ["", null],
    ["   ", null],
    ["Infinity", null],
    ["NaN", null],
    [null, null],
    [{ value: 1 }, null]
  ])("keeps legacy presentation classification for %j", (value, expected) => {
    expect(finiteNumber(value)).toBe(expected);
  });

  it.each(["1e2", "0x10", ".5", "1.", "", "   ", "Infinity", "NaN"])(
    "keeps %j out of exact ranking authority",
    (invalid) => {
      expect(compareGenes(epoch, gene(1, invalid), gene(2, "1"))).toBe(1);
      expect(compareGenes(epoch, gene(2, "1"), gene(1, invalid))).toBe(-1);
    }
  );

  it.each([null, { value: "1" }])(
    "fails closed when ranking receives non-string input %j",
    (invalid) => {
      const malformed = gene(1, "1");
      malformed.score_total = invalid as unknown as string;
      expect(compareGenes(epoch, malformed, gene(2, "1"))).toBe(1);
    }
  );

  it.each([
    ["1", "+1.0", 0],
    [" 1.250 ", "1.249", -1],
    ["-1.25", "-1.2", 1],
    ["9007199254740993.00000001", "9007199254740993.00000000", -1]
  ])(
    "compares exact Decimal scores %s and %s without chart-number authority",
    (left, right, expectedSign) => {
      const comparison = compareGenes(epoch, gene(1, left), gene(2, right));
      expect(comparison === 0 ? 0 : Math.sign(comparison)).toBe(expectedSign);
    }
  );

  it("builds axes, selection, objective, and ranking in one pure projection", () => {
    const genes = [gene(1, "1", "rth"), gene(2, "2", 5)];

    expect(buildResearchModel(epoch, genes, 1)).toEqual({
      dimensions: [
        { name: "fast", type: "category", categories: ["5", "rth"] },
        { name: "slow", type: "value" }
      ],
      numericParameters: ["slow"],
      selectedGene: genes[0],
      objective: "maximize_score",
      rankedGenes: [genes[1], genes[0]]
    });
  });

  it("extracts only exact numeric chart identities", () => {
    expect(geneIdFromChartData([5, 20, 1, 7, "candidate"])).toBe(7);
    expect(geneIdFromChartData({ geneId: 9 })).toBe(9);
    expect(geneIdFromChartData({ geneId: "9" })).toBeNull();
    expect(geneIdFromChartData([5, 20, 1, "7"])).toBeNull();
    expect(geneIdFromChartData(null)).toBeNull();
  });

  it("preserves display formatting", () => {
    expect(displayNumber(null, "en")).toBe("—");
    expect(displayValue("1e2", "en")).toBe("100");
  });
});
