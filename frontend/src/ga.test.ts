import { describe, expect, it } from "vitest";

import type { Epoch, Gene, GenerationSummary } from "./api";
import { demoEpoch } from "./demo";
import {
  type ChartCopy,
  compareGenes,
  convergenceOption,
  epochObjective,
  finiteNumber,
  fitnessObservationRows,
  fitnessSurfaceOption,
  fitnessSurfaceRows,
  numericParameterNames,
  parameterDimensions,
  selectedSurfaceOption,
  selectBestGene,
  surfaceRow
} from "./ga";

const copy: ChartCopy = {
  locale: "en",
  generation: "Generation",
  drawdown: "Drawdown",
  score: "Score",
  fitness: "Fitness",
  lowerDrawdown: "Lowest drawdown",
  upperDrawdown: "Highest drawdown",
  lowerScore: "Lowest score",
  upperScore: "Highest score",
  high: "High",
  low: "Low",
  selected: "Selected candidate",
  bestObserved: "Best observed at each X/Y coordinate"
};

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

function surfaceOptions(
  epoch: Epoch,
  genes: Gene[],
  xParameter: string,
  yParameter: string,
  selectedGeneId: number | null = null
) {
  const rows = fitnessSurfaceRows(epoch, genes, xParameter, yParameter);
  const selectedGene =
    genes.find((candidate) => candidate.id === selectedGeneId) ?? null;
  return {
    rows,
    base: fitnessSurfaceOption(
      epoch,
      rows,
      xParameter,
      yParameter,
      copy,
      "light"
    ),
    update: selectedSurfaceOption(
      surfaceRow(epoch, selectedGene, xParameter, yParameter)
    )
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

  it("builds 10k observed surface rows with progressive rendering", () => {
    const genes = Array.from({ length: 10_000 }, (_, index) =>
      gene(index, { fast: index, slow: index * 2 }, String(index))
    );

    const option = surfaceOptions(
      demoEpoch,
      genes,
      "fast",
      "slow"
    ).base as {
      series: Array<{
        data: unknown[];
        progressive: number;
        progressiveThreshold: number;
      }>;
    };

    expect(option.series[0].data).toHaveLength(10_000);
    expect(option.series[0].progressive).toBe(5_000);
    expect(option.series[0].progressiveThreshold).toBe(3_000);
  });

  it("renders persisted labels as canvas text instead of tooltip HTML", () => {
    const option = surfaceOptions(
      demoEpoch,
      [
        {
          ...gene(1, { "<img src=x onerror=alert(1)>": 5, slow: 20 }),
          candidate_id: "<script>alert(1)</script>"
        }
      ],
      "<img src=x onerror=alert(1)>",
      "slow"
    ).base as {
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
        "Best observed at each X/Y coordinate",
        "<img src=x onerror=alert(1)>: 5",
        "slow: 20",
        "Fitness: 1.5"
      ].join("\n")
    );
  });

  it("leaves unobserved parameter combinations empty", () => {
    const options = surfaceOptions(
      demoEpoch,
      [
        gene(1, { fast: 5, slow: 20 }, "1"),
        gene(2, { fast: 8, slow: 20 }, "2"),
        gene(3, { fast: 8, slow: 34 }, "3")
      ],
      "fast",
      "slow",
      3
    );
    const option = options.base as {
      grid: { right: number };
      visualMap: {
        orient: string;
        seriesIndex: number;
        min: number;
        max: number;
        formatter: (value: unknown) => string;
      };
      xAxis: { type: string };
      yAxis: { type: string };
      series: Array<{ type: string; data: unknown[] }>;
      media: Array<{ option: { visualMap: { itemHeight: number } } }>;
    };

    expect(option.xAxis.type).toBe("value");
    expect(option.yAxis.type).toBe("value");
    expect(option.series[0].data).toHaveLength(3);
    expect(option.series[0].type).toBe("custom");
    expect(option.series[1].data).toHaveLength(0);
    expect(
      (options.update as { series: Array<{ data: unknown[] }> }).series[0].data
    ).toHaveLength(1);
    expect(option.visualMap.orient).toBe("vertical");
    expect(option.visualMap.seriesIndex).toBe(0);
    expect(option.grid.right).toBeGreaterThanOrEqual(100);
    expect(option.media[0].option.visualMap.itemHeight).toBeLessThan(150);
  });

  it("keeps a constant fitness scale truthful and locale-formatted", () => {
    const option = surfaceOptions(
      demoEpoch,
      [gene(1, { fast: 5, slow: 20 }), gene(2, { fast: 8, slow: 34 })],
      "fast",
      "slow"
    ).base as {
      visualMap: {
        min: number;
        max: number;
        formatter: (value: unknown) => string;
      };
    };

    expect(option.visualMap.min).toBe(1.5);
    expect(option.visualMap.max).toBe(1.5);
    expect(option.visualMap.formatter(1234.5)).toBe("1,234.5");
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

    const option = convergenceOption(epoch, summaries, copy, "light") as {
      series: Array<{ data: unknown[] }>;
    };

    expect(option.series[0].data).toEqual([[0, 0.1]]);
    expect(option.series[1].data).toEqual([[0, 0.3]]);
  });

  it("formats chart values with the active locale", () => {
    const option = convergenceOption(
      demoEpoch,
      [
        {
          generation_index: 1_234,
          candidate_count: 2,
          score_min: "1234.5",
          score_max: "2345.6",
          drawdown_min: "0.1",
          drawdown_max: "0.2"
        }
      ],
      copy,
      "light"
    ) as {
      tooltip: { valueFormatter: (value: unknown) => string };
      xAxis: { axisLabel: { formatter: (value: unknown) => string } };
    };

    expect(option.tooltip.valueFormatter(1234.5)).toBe("1,234.5");
    expect(option.xAxis.axisLabel.formatter(1234)).toBe("1,234");
  });

  it("uses the best observed drawdown for duplicate surface coordinates", () => {
    const epoch: Epoch = {
      ...demoEpoch,
      config_json: { objective: "minimize_drawdown" }
    };
    const options = surfaceOptions(
      epoch,
      [
        { ...gene(1, { fast: 5, slow: 20 }, "9"), max_drawdown: "0.3" },
        { ...gene(2, { fast: 5, slow: 20 }, "1"), max_drawdown: "0.1" }
      ],
      "fast",
      "slow"
    );
    const option = options.base as {
      visualMap: { inRange: { color: string[] } };
      series: Array<{
        data: Array<[number, number, number, number, string]>;
        name: string;
      }>;
    };

    expect(option.series[0].data).toEqual([[5, 20, 0.1, 2, "candidate-2"]]);
    expect(option.series[0].name).toBe(
      "Best observed at each X/Y coordinate"
    );
    expect(option.visualMap.inRange.color[0]).toBe("#454b8c");
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
