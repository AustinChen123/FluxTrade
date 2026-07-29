import type { CustomSeriesRenderItem } from "echarts";
import type { EChartsCoreOption } from "echarts/core";

import type { Epoch, Gene, GenerationSummary } from "./api";
import type { Theme } from "./theme";

export type ParameterDimension = {
  name: string;
  type: "value" | "category";
  categories?: string[];
};

export type ChartCopy = {
  locale: string;
  generation: string;
  drawdown: string;
  score: string;
  fitness: string;
  lowerDrawdown: string;
  upperDrawdown: string;
  lowerScore: string;
  upperScore: string;
  high: string;
  low: string;
  selected: string;
  bestObserved: string;
};

export type Objective =
  | "maximize_score"
  | "maximize_return"
  | "minimize_drawdown";

type DecimalParts = {
  negative: boolean;
  integer: string;
  fraction: string;
};

const palettes = {
  light: {
    ink: "#182128",
    muted: "#66726f",
    grid: "#cbd5d1",
    teal: "#0e6b6f",
    coral: "#d46a4c",
    indigo: "#454b8c",
    paper: "#eef2f1",
    low: "#d7e3df"
  },
  dark: {
    ink: "#e5ece9",
    muted: "#94a29d",
    grid: "#3b4949",
    teal: "#49b2ae",
    coral: "#ef8a6b",
    indigo: "#8c91d9",
    paper: "#11191d",
    low: "#2e4a49"
  }
} as const;

export function finiteNumber(value: unknown): number | null {
  if (typeof value !== "number" && typeof value !== "string") {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function decimalParts(value: string, absolute = false): DecimalParts | null {
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(value.trim());
  if (!match) {
    return null;
  }
  const integer = match[2].replace(/^0+(?=\d)/, "");
  const fraction = (match[3] ?? "").replace(/0+$/, "");
  const zero = integer === "0" && fraction === "";
  return {
    negative: !absolute && match[1] === "-" && !zero,
    integer,
    fraction
  };
}

function compareMagnitude(left: DecimalParts, right: DecimalParts): number {
  if (left.integer.length !== right.integer.length) {
    return left.integer.length - right.integer.length;
  }
  const integer =
    left.integer === right.integer ? 0 : left.integer < right.integer ? -1 : 1;
  if (integer !== 0) {
    return integer;
  }
  const length = Math.max(left.fraction.length, right.fraction.length);
  const leftFraction = left.fraction.padEnd(length, "0");
  const rightFraction = right.fraction.padEnd(length, "0");
  return leftFraction === rightFraction
    ? 0
    : leftFraction < rightFraction
      ? -1
      : 1;
}

function compareDecimals(left: DecimalParts, right: DecimalParts): number {
  if (left.negative !== right.negative) {
    return left.negative ? -1 : 1;
  }
  const magnitude = compareMagnitude(left, right);
  return left.negative ? -magnitude : magnitude;
}

export function epochObjective(epoch: Epoch): Objective | null {
  const objective = epoch.config_json.objective;
  return objective === "maximize_score" ||
    objective === "maximize_return" ||
    objective === "minimize_drawdown"
    ? objective
    : null;
}

function numberFormatter(locale: string) {
  const formatter = new Intl.NumberFormat(locale, {
    maximumFractionDigits: 8
  });
  return (value: unknown) => {
    const parsed = finiteNumber(value);
    return parsed === null ? String(value) : formatter.format(parsed);
  };
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

export function convergenceOption(
  epoch: Epoch,
  summaries: GenerationSummary[],
  copy: ChartCopy,
  theme: Theme
): EChartsCoreOption {
  const palette = palettes[theme];
  const format = numberFormatter(copy.locale);
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
    grid: { left: 48, right: 18, top: 54, bottom: 34 },
    legend: {
      top: 12,
      right: 18,
      textStyle: { color: palette.muted }
    },
    tooltip: { trigger: "axis", valueFormatter: format },
    xAxis: {
      type: "value",
      minInterval: 1,
      name: copy.generation,
      nameTextStyle: { color: palette.muted },
      axisLabel: { color: palette.muted, formatter: format },
      axisLine: { lineStyle: { color: palette.grid } },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.5 } }
    },
    yAxis: {
      type: "value",
      scale: true,
      name: minimizeDrawdown ? copy.drawdown : copy.score,
      nameTextStyle: { color: palette.muted },
      axisLabel: { color: palette.muted, formatter: format },
      axisLine: { lineStyle: { color: palette.grid } },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.65 } }
    },
    series: [
      {
        name: minimizeDrawdown ? copy.lowerDrawdown : copy.lowerScore,
        type: "line",
        data: lower,
        showSymbol: false,
        lineStyle: { color: palette.grid, width: 1 },
        areaStyle: { color: `${palette.indigo}14` }
      },
      {
        name: minimizeDrawdown ? copy.upperDrawdown : copy.upperScore,
        type: "line",
        data: upper,
        showSymbol: false,
        lineStyle: {
          color: minimizeDrawdown ? palette.coral : palette.teal,
          width: 2
        }
      }
    ]
  };
}

export type SurfaceRow = [
  number,
  number,
  number,
  number,
  string
];

function objectiveDecimal(epoch: Epoch, gene: Gene): DecimalParts | null {
  const objective = epochObjective(epoch);
  if (objective === null) {
    return null;
  }
  return objective === "minimize_drawdown"
    ? decimalParts(gene.max_drawdown, true)
    : decimalParts(gene.score_total);
}

function objectiveValue(epoch: Epoch, gene: Gene): number | null {
  const objective = epochObjective(epoch);
  const raw =
    objective === "minimize_drawdown"
      ? gene.max_drawdown
      : objective === null
        ? null
        : gene.score_total;
  if (raw === null || objectiveDecimal(epoch, gene) === null) {
    return null;
  }
  const value = finiteNumber(raw);
  return value === null
    ? null
    : objective === "minimize_drawdown"
      ? Math.abs(value)
      : value;
}

export function compareGenes(epoch: Epoch, left: Gene, right: Gene): number {
  const objective = epochObjective(epoch);
  const minimize = objective === "minimize_drawdown";
  const leftValue = objectiveDecimal(epoch, left);
  const rightValue = objectiveDecimal(epoch, right);
  if (leftValue === null) {
    return rightValue === null ? 0 : 1;
  }
  if (rightValue === null) {
    return -1;
  }
  const comparison = compareDecimals(leftValue, rightValue);
  const primary = minimize ? comparison : -comparison;
  if (primary !== 0 || !minimize) {
    return primary;
  }
  const leftScore = decimalParts(left.score_total);
  const rightScore = decimalParts(right.score_total);
  if (leftScore === null) {
    return rightScore === null ? 0 : 1;
  }
  return rightScore === null ? -1 : -compareDecimals(leftScore, rightScore);
}

export function selectBestGene(epoch: Epoch, genes: Gene[]): Gene | null {
  if (epochObjective(epoch) === null) {
    return null;
  }
  return genes.reduce<Gene | null>(
    (best, candidate) =>
      best === null || compareGenes(epoch, candidate, best) < 0
        ? candidate
        : best,
    null
  );
}

export function fitnessSurfaceRows(
  epoch: Epoch,
  genes: Gene[],
  xParameter: string,
  yParameter: string
): SurfaceRow[] {
  if (epochObjective(epoch) === null) {
    return [];
  }
  const observed = new Map<string, Gene>();
  for (const gene of genes) {
    const x = finiteNumber(gene.param_pack[xParameter]);
    const y = finiteNumber(gene.param_pack[yParameter]);
    if (x === null || y === null || objectiveValue(epoch, gene) === null) {
      continue;
    }
    const key = `${x}\0${y}`;
    const current = observed.get(key);
    if (current === undefined || compareGenes(epoch, gene, current) < 0) {
      observed.set(key, gene);
    }
  }
  return [...observed.values()].flatMap((gene) => {
    const row = surfaceRow(epoch, gene, xParameter, yParameter);
    return row === null ? [] : [row];
  });
}

export function fitnessObservationRows(
  epoch: Epoch,
  genes: Gene[],
  xParameter: string,
  yParameter: string
): SurfaceRow[] {
  if (epochObjective(epoch) === null) {
    return [];
  }
  return genes.flatMap((gene) => {
    const row = surfaceRow(epoch, gene, xParameter, yParameter);
    return row === null ? [] : [row];
  });
}

export function surfaceRow(
  epoch: Epoch,
  gene: Gene | null,
  xParameter: string,
  yParameter: string
): SurfaceRow | null {
  if (gene === null) {
    return null;
  }
  const x = finiteNumber(gene.param_pack[xParameter]);
  const y = finiteNumber(gene.param_pack[yParameter]);
  const value = objectiveValue(epoch, gene);
  return x === null || y === null || value === null
    ? null
    : [x, y, value, gene.id, gene.candidate_id];
}

export function fitnessSurfaceOption(
  epoch: Epoch,
  surface: SurfaceRow[],
  xParameter: string,
  yParameter: string,
  copy: ChartCopy,
  theme: Theme
): EChartsCoreOption {
  const palette = palettes[theme];
  const minimize = epochObjective(epoch) === "minimize_drawdown";
  const metricLabel = minimize ? copy.drawdown : copy.fitness;
  const values = surface.map((item) => item[2]);
  const valueMin = values.length ? Math.min(...values) : 0;
  const valueMax = values.length ? Math.max(...values) : 1;
  const format = numberFormatter(copy.locale);
  const coordinateCount = Math.max(
    new Set(surface.map((row) => row[0])).size,
    new Set(surface.map((row) => row[1])).size,
    1
  );
  const cellSize = Math.max(2, Math.min(18, 240 / coordinateCount));
  const renderSurfaceCell: CustomSeriesRenderItem = (_params, api) => {
    const [x, y] = api.coord([api.value(0), api.value(1)]);
    return {
      type: "rect",
      shape: {
        x: x - cellSize / 2,
        y: y - cellSize / 2,
        width: cellSize,
        height: cellSize
      },
      style: { fill: api.visual("color") },
      emphasis: {
        style: {
          stroke: palette.ink,
          lineWidth: 1
        }
      }
    };
  };
  const renderSelectedCandidate: CustomSeriesRenderItem = (_params, api) => {
    const [x, y] = api.coord([api.value(0), api.value(1)]);
    return {
      type: "circle",
      shape: { cx: x, cy: y, r: 7.5 },
      style: {
        fill: palette.coral,
        stroke: palette.paper,
        lineWidth: 3
      }
    };
  };
  return {
    animation: false,
    grid: { left: 64, right: 108, top: 24, bottom: 58 },
    tooltip: {
      trigger: "item",
      renderMode: "richText",
      formatter: (params: unknown) => {
        const { value, seriesIndex } = params as {
          value: SurfaceRow;
          seriesIndex?: number;
        };
        return [
          String(value[4]),
          seriesIndex === 1 ? copy.selected : copy.bestObserved,
          `${xParameter}: ${format(value[0])}`,
          `${yParameter}: ${format(value[1])}`,
          `${metricLabel}: ${format(value[2])}`
        ].join("\n");
      }
    },
    visualMap: {
      min: valueMin,
      max: valueMax,
      formatter: format,
      dimension: 2,
      seriesIndex: 0,
      orient: "vertical",
      right: 12,
      top: "middle",
      itemWidth: 12,
      itemHeight: 190,
      calculable: true,
      text: [copy.high, copy.low],
      textGap: 8,
      textStyle: { color: palette.muted },
      inRange: {
        color: minimize
          ? [palette.indigo, palette.teal, palette.low]
          : [palette.low, palette.teal, palette.indigo]
      }
    },
    xAxis: {
      type: "value",
      scale: true,
      name: xParameter,
      nameTextStyle: { color: palette.ink },
      axisLabel: { color: palette.muted, hideOverlap: true, formatter: format },
      axisLine: { lineStyle: { color: palette.grid } },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.45 } }
    },
    yAxis: {
      type: "value",
      scale: true,
      name: yParameter,
      nameTextStyle: { color: palette.ink },
      axisLabel: { color: palette.muted, hideOverlap: true, formatter: format },
      axisLine: { lineStyle: { color: palette.grid } },
      splitLine: { lineStyle: { color: palette.grid, opacity: 0.45 } }
    },
    series: [
      {
        id: "fitness-surface",
        name: copy.bestObserved,
        type: "custom",
        coordinateSystem: "cartesian2d",
        renderItem: renderSurfaceCell,
        data: surface,
        encode: { x: 0, y: 1, value: 2 },
        progressive: 5_000,
        progressiveThreshold: 3_000
      },
      {
        id: "selected-candidate",
        name: copy.selected,
        type: "custom",
        coordinateSystem: "cartesian2d",
        renderItem: renderSelectedCandidate,
        data: [],
        encode: { x: 0, y: 1 },
        z: 10
      }
    ],
    media: [
      {
        query: { maxWidth: 600 },
        option: {
          grid: { left: 54, right: 74, top: 18, bottom: 54 },
          visualMap: {
            right: 2,
            itemWidth: 10,
            itemHeight: 112,
            textGap: 3,
            textStyle: { color: palette.muted, fontSize: 10 }
          }
        }
      }
    ]
  };
}

export function selectedSurfaceOption(
  selected: SurfaceRow | null
): EChartsCoreOption {
  return {
    series: [
      {
        id: "selected-candidate",
        data: selected === null ? [] : [selected]
      }
    ]
  };
}

export function parallelOption(
  genes: Gene[],
  dimensions: ParameterDimension[],
  selectedGeneId: number | null,
  theme: Theme,
  locale: string
): EChartsCoreOption {
  const palette = palettes[theme];
  const format = numberFormatter(locale);
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
      data: dimension.categories,
      axisLabel:
        dimension.type === "value"
          ? { color: palette.muted, formatter: format }
          : { color: palette.muted }
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
