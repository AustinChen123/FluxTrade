import { describe, expect, it } from "vitest";

import type { Epoch, Gene, GenerationSummary } from "../../api";
import {
  convergenceOption,
  fitnessSurfaceOption,
  parallelOption,
  selectedSurfaceOption,
  type ChartCopy
} from "./gaCharts";
import {
  fitnessSurfaceRows,
  parameterDimensions,
  surfaceRow
} from "./gaDomain";
import { demoEpoch } from "./demo";

const epoch: Epoch = {
  id: "epoch",
  strategy_id: "strategy",
  started_at: "2026-07-28T00:00:00Z",
  finished_at: "2026-07-28T01:00:00Z",
  pop_size: 1,
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
  score = "1.5",
  paramPack: Record<string, unknown> = { fast: id, slow: id * 2 }
): Gene {
  return {
    id,
    strategy_id: "strategy",
    role: "challenger",
    param_pack: paramPack,
    score_total: score,
    score_breakdown: {},
    max_drawdown: "0.1",
    generation_index: 0,
    candidate_id: `candidate-${id}`,
    epoch_id: "epoch",
    created_at: "2026-07-28T01:00:00Z"
  };
}

function surfaceOptions(
  currentEpoch: Epoch,
  genes: Gene[],
  xParameter: string,
  yParameter: string,
  selectedGeneId: number | null = null
) {
  const rows = fitnessSurfaceRows(
    currentEpoch,
    genes,
    xParameter,
    yParameter
  );
  const selectedGene =
    genes.find((candidate) => candidate.id === selectedGeneId) ?? null;
  return {
    rows,
    base: fitnessSurfaceOption(
      currentEpoch,
      rows,
      xParameter,
      yParameter,
      copy,
      "light"
    ),
    update: selectedSurfaceOption(
      surfaceRow(currentEpoch, selectedGene, xParameter, yParameter)
    )
  };
}

describe("GA chart projection", () => {
  it("projects convergence without changing persisted Decimal strings", () => {
    const summaries: GenerationSummary[] = [
      {
        generation_index: 0,
        candidate_count: 1,
        score_min: "1.25",
        score_max: "2.50",
        drawdown_min: "0.1",
        drawdown_max: "0.2"
      }
    ];
    const option = convergenceOption(epoch, summaries, copy, "light") as {
      series: Array<{ data: unknown[] }>;
    };

    expect(option.series.map((series) => series.data)).toEqual([
      [[0, 1.25]],
      [[0, 2.5]]
    ]);
    expect(summaries[0].score_max).toBe("2.50");
  });

  it("keeps surface observations and selection in separate series updates", () => {
    const genes = [gene(1, "1"), gene(2, "2")];
    const rows = fitnessSurfaceRows(epoch, genes, "fast", "slow");
    const option = fitnessSurfaceOption(
      epoch,
      rows,
      "fast",
      "slow",
      copy,
      "dark"
    ) as { series: Array<{ id: string; data: unknown[] }> };
    const selection = selectedSurfaceOption(rows[1]) as {
      series: Array<{ id: string; data: unknown[] }>;
    };

    expect(option.series[0]).toMatchObject({
      id: "fitness-surface",
      data: rows
    });
    expect(option.series[1]).toMatchObject({
      id: "selected-candidate",
      data: []
    });
    expect(selection.series).toEqual([
      { id: "selected-candidate", data: [rows[1]] }
    ]);
  });

  it("limits parallel presentation to twelve dimensions", () => {
    const candidate = gene(1, "1");
    candidate.param_pack = Object.fromEntries(
      Array.from({ length: 14 }, (_, index) => [`p${index}`, index])
    );
    const option = parallelOption(
      [candidate],
      parameterDimensions([candidate]),
      candidate.id,
      "light",
      "en"
    ) as { parallelAxis: unknown[]; series: Array<{ data: unknown[] }> };

    expect(option.parallelAxis).toHaveLength(12);
    expect(option.series[0].data).toHaveLength(1);
    expect(option.series[1].data).toHaveLength(1);
  });

  it("builds 10k observed surface rows with progressive rendering", () => {
    const genes = Array.from({ length: 10_000 }, (_, index) =>
      gene(index, String(index), { fast: index, slow: index * 2 })
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
          ...gene(1, "1.5", {
            "<img src=x onerror=alert(1)>": 5,
            slow: 20
          }),
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
        gene(1, "1", { fast: 5, slow: 20 }),
        gene(2, "2", { fast: 8, slow: 20 }),
        gene(3, "3", { fast: 8, slow: 34 })
      ],
      "fast",
      "slow",
      3
    );
    const option = options.base as {
      grid: { right: number };
      visualMap: { orient: string; seriesIndex: number };
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
      [
        gene(1, "1.5", { fast: 5, slow: 20 }),
        gene(2, "1.5", { fast: 8, slow: 34 })
      ],
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
    const minimizeEpoch: Epoch = {
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
    const option = convergenceOption(
      minimizeEpoch,
      summaries,
      copy,
      "light"
    ) as { series: Array<{ data: unknown[] }> };

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
    const minimizeEpoch: Epoch = {
      ...demoEpoch,
      config_json: { objective: "minimize_drawdown" }
    };
    const options = surfaceOptions(
      minimizeEpoch,
      [
        {
          ...gene(1, "9", { fast: 5, slow: 20 }),
          max_drawdown: "0.3"
        },
        {
          ...gene(2, "1", { fast: 5, slow: 20 }),
          max_drawdown: "0.1"
        }
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
});
