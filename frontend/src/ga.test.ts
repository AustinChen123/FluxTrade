import { describe, expect, it } from "vitest";

import type { Epoch, Gene, GenerationSummary } from "./api";
import { demoEpoch } from "./demo";
import {
  convergenceOption,
  finiteNumber,
  numericParameterNames,
  parameterDimensions,
  scatterOption
} from "./ga";

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

  it("builds a 10k-point canvas dataset without per-point DOM state", () => {
    const genes = Array.from({ length: 10_000 }, (_, index) =>
      gene(index, { fast: index % 50, slow: index % 180 }, String(index))
    );

    const option = scatterOption(genes, "fast", "slow", null) as {
      dataset: { source: unknown[] };
      series: Array<{ large?: boolean }>;
    };

    expect(option.dataset.source).toHaveLength(10_000);
    expect(option.series[0].large).toBe(true);
  });

  it("renders persisted labels as canvas text instead of tooltip HTML", () => {
    const option = scatterOption(
      [
        {
          ...gene(1, { "<img src=x onerror=alert(1)>": 5, slow: 20 }),
          candidate_id: "<script>alert(1)</script>"
        }
      ],
      "<img src=x onerror=alert(1)>",
      "slow",
      null
    ) as {
      tooltip: {
        renderMode: string;
        formatter: (params: { value: Array<number | string> }) => string;
      };
    };

    expect(option.tooltip.renderMode).toBe("richText");
    expect(
      option.tooltip.formatter({
        value: [5, 20, 1.5, 1, "<script>alert(1)</script>"]
      })
    ).toBe(
      [
        "<script>alert(1)</script>",
        "<img src=x onerror=alert(1)>: 5",
        "slow: 20",
        "score: 1.5"
      ].join("\n")
    );
  });

  it("uses drawdown bounds for a minimize-drawdown epoch", () => {
    const epoch: Epoch = {
      ...demoEpoch,
      config_json: { objective: "minimize_drawdown" }
    };
    const summaries = [
      {
        generation_index: 0,
        candidate_count: 2,
        score_min: "1",
        score_max: "4",
        drawdown_min: "0.1",
        drawdown_max: "0.3"
      }
    ] satisfies GenerationSummary[];

    const option = convergenceOption(epoch, summaries) as {
      series: Array<{ data: unknown[] }>;
    };

    expect(option.series[0].data).toEqual([[0, 0.1]]);
    expect(option.series[1].data).toEqual([[0, 0.3]]);
  });
});
