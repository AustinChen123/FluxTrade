import type { EChartsCoreOption } from "echarts/core";

import type { Epoch, Gene, GenerationSummary } from "./api";

export type ParameterDimension = {
  name: string;
  type: "value" | "category";
  categories?: string[];
};

const palette = {
  ink: "#182128",
  muted: "#66726f",
  grid: "#cbd5d1",
  teal: "#0e6b6f",
  coral: "#d46a4c",
  indigo: "#454b8c",
  paper: "#eef2f1"
};

export function finiteNumber(value: unknown): number | null {
  if (
    typeof value !== "number" &&
    typeof value !== "string"
  ) {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parameterDimensions(genes: Gene[]): ParameterDimension[] {
  const names = [...new Set(genes.flatMap((gene) => Object.keys(gene.param_pack)))]
    .sort();
  return names.map((name) => {
    const values = genes
      .map((gene) => gene.param_pack[name])
      .filter((value) => value !== undefined && value !== null);
    if (values.length > 0 && values.every((value) => finiteNumber(value) !== null)) {
      return { name, type: "value" };
    }
    return {
      name,
      type: "category",
      categories: [...new Set(values.map(String))].sort()
    };
  });
}

export function numericParameterNames(genes: Gene[]): string[] {
  return parameterDimensions(genes)
    .filter((dimension) => dimension.type === "value")
    .map((dimension) => dimension.name);
}

export function objectiveLabel(epoch: Epoch): string {
  const objective = epoch.config_json.objective;
  if (objective === "minimize_drawdown") {
    return "最小回撤";
  }
  if (objective === "maximize_return") {
    return "最大報酬";
  }
  return "最大評分";
}

export function convergenceOption(
  epoch: Epoch,
  summaries: GenerationSummary[]
): EChartsCoreOption {
  const minimizeDrawdown = epoch.config_json.objective === "minimize_drawdown";
  const lower = summaries.map((item) => [
    item.generation_index,
    finiteNumber(minimizeDrawdown ? item.drawdown_min : item.score_min)
  ]);
  const upper = summaries.map((item) => [
    item.generation_index,
    finiteNumber(minimizeDrawdown ? item.drawdown_max : item.score_max)
  ]);
  return {
    animation: false,
    grid: { left: 48, right: 18, top: 18, bottom: 34 },
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "value",
      minInterval: 1,
      name: "世代",
      nameTextStyle: { color: palette.muted },
      axisLabel: { color: palette.muted },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.5 } }
    },
    yAxis: {
      type: "value",
      scale: true,
      name: minimizeDrawdown ? "回撤" : "評分",
      nameTextStyle: { color: palette.muted },
      axisLabel: { color: palette.muted },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.65 } }
    },
    series: [
      {
        name: minimizeDrawdown ? "最低回撤" : "最低評分",
        type: "line",
        data: lower,
        showSymbol: false,
        lineStyle: { color: palette.grid, width: 1 },
        areaStyle: { color: "rgba(69, 75, 140, 0.08)" }
      },
      {
        name: minimizeDrawdown ? "最高回撤" : "最高評分",
        type: "line",
        data: upper,
        showSymbol: false,
        lineStyle: { color: minimizeDrawdown ? palette.coral : palette.teal, width: 2 }
      }
    ]
  };
}

export function scatterOption(
  genes: Gene[],
  xParameter: string,
  yParameter: string,
  selectedGeneId: number | null
): EChartsCoreOption {
  const rows = genes.flatMap((gene) => {
    const x = finiteNumber(gene.param_pack[xParameter]);
    const y = finiteNumber(gene.param_pack[yParameter]);
    const score = finiteNumber(gene.score_total);
    if (x === null || y === null || score === null) {
      return [];
    }
    return [[x, y, score, gene.id, gene.candidate_id]];
  });
  const selected = rows.filter((row) => row[3] === selectedGeneId);
  const scores = rows.map((row) => row[2] as number);
  const scoreMin = scores.length ? Math.min(...scores) : 0;
  const scoreMax = scores.length ? Math.max(...scores) : 1;
  return {
    animation: false,
    dataset: {
      dimensions: [xParameter, yParameter, "score", "gene_id", "candidate_id"],
      source: rows
    },
    grid: { left: 64, right: 28, top: 24, bottom: 58 },
    tooltip: {
      trigger: "item",
      renderMode: "richText",
      formatter: (params: unknown) => {
        const value = (params as { value: Array<number | string> }).value;
        return [
          String(value[4]),
          `${xParameter}: ${value[0]}`,
          `${yParameter}: ${value[1]}`,
          `score: ${value[2]}`
        ].join("\n");
      }
    },
    visualMap: {
      min: scoreMin,
      max: scoreMax === scoreMin ? scoreMin + 1 : scoreMax,
      dimension: 2,
      orient: "horizontal",
      left: "center",
      bottom: 4,
      calculable: true,
      text: ["高", "低"],
      textStyle: { color: palette.muted },
      inRange: { color: ["#c7d8d4", palette.teal, palette.indigo] }
    },
    xAxis: {
      type: "value",
      name: xParameter,
      scale: true,
      nameTextStyle: { color: palette.ink },
      axisLabel: { color: palette.muted },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.55 } }
    },
    yAxis: {
      type: "value",
      name: yParameter,
      scale: true,
      nameTextStyle: { color: palette.ink },
      axisLabel: { color: palette.muted },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.55 } }
    },
    series: [
      {
        type: "scatter",
        encode: { x: xParameter, y: yParameter, tooltip: [0, 1, 2] },
        symbolSize: 7,
        large: rows.length >= 5_000,
        largeThreshold: 5_000,
        progressive: 5_000,
        progressiveThreshold: 3_000,
        itemStyle: { opacity: 0.7 }
      },
      {
        type: "scatter",
        data: selected,
        symbolSize: 16,
        itemStyle: {
          color: palette.coral,
          borderColor: palette.paper,
          borderWidth: 3
        },
        z: 10
      }
    ]
  };
}

export function parallelOption(
  genes: Gene[],
  dimensions: ParameterDimension[],
  selectedGeneId: number | null
): EChartsCoreOption {
  const visibleDimensions = dimensions.slice(0, 12);
  const rows = genes.map((gene) => ({
    name: gene.candidate_id,
    geneId: gene.id,
    value: visibleDimensions.map((dimension) => {
      const value = gene.param_pack[dimension.name];
      return dimension.type === "value" ? finiteNumber(value) : String(value);
    })
  }));
  return {
    animation: false,
    parallel: {
      left: 54,
      right: 54,
      top: 38,
      bottom: 34,
      parallelAxisDefault: {
        nameTextStyle: { color: palette.ink },
        axisLabel: { color: palette.muted },
        axisLine: { lineStyle: { color: palette.grid } }
      }
    },
    parallelAxis: visibleDimensions.map((dimension, index) => ({
      dim: index,
      name: dimension.name,
      type: dimension.type,
      data: dimension.categories
    })),
    series: [
      {
        type: "parallel",
        data: rows,
        progressive: 2_000,
        progressiveThreshold: 1_000,
        lineStyle: {
          color: palette.indigo,
          width: 1,
          opacity: 0.08
        },
        emphasis: {
          lineStyle: { color: palette.coral, width: 2, opacity: 0.95 }
        }
      },
      {
        type: "parallel",
        data: rows.filter((row) => row.geneId === selectedGeneId),
        silent: true,
        lineStyle: { color: palette.coral, width: 3, opacity: 1 },
        z: 10
      }
    ]
  };
}
