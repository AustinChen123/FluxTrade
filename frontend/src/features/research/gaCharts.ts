import type { CustomSeriesRenderItem } from "echarts";
import type { EChartsCoreOption } from "echarts/core";

import type { Theme } from "../../shared/theme";
import {
  epochObjective,
  finiteNumber,
  type Epoch,
  type Gene,
  type GenerationSummary,
  type ParameterDimension,
  type SurfaceRow
} from "./gaDomain";

export type ResearchChartOption = EChartsCoreOption;

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

function numberFormatter(locale: string) {
  const formatter = new Intl.NumberFormat(locale, {
    maximumFractionDigits: 8
  });
  return (value: unknown) => {
    const parsed = finiteNumber(value);
    return parsed === null ? String(value) : formatter.format(parsed);
  };
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
