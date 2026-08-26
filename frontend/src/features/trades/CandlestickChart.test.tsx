// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

const chartModules = vi.hoisted(() => ({
  CandlestickSeries: { id: "candlestick" },
  DataZoomComponent: { id: "data-zoom" },
  GridComponent: { id: "grid" },
  LegendComponent: { id: "legend" },
  ScatterChart: { id: "scatter" },
  TooltipComponent: { id: "tooltip" },
  use: vi.fn()
}));
const renderCore = vi.hoisted(() => vi.fn());

vi.mock("echarts/core", () => ({ use: chartModules.use }));
vi.mock("echarts/components", () => ({
  DataZoomComponent: chartModules.DataZoomComponent,
  GridComponent: chartModules.GridComponent,
  LegendComponent: chartModules.LegendComponent,
  TooltipComponent: chartModules.TooltipComponent
}));
vi.mock("echarts/charts", () => ({
  CandlestickChart: chartModules.CandlestickSeries,
  ScatterChart: chartModules.ScatterChart
}));
vi.mock("../../shared/charts/EChartCore", () => ({
  EChartCore: (props: { ariaLabel: string }) => {
    renderCore(props);
    return <div role="img" aria-label={props.ariaLabel} />;
  }
}));

import { CandlestickChart } from "./CandlestickChart";

describe("CandlestickChart", () => {
  afterEach(() => {
    cleanup();
  });

  it("registers only the trade chart modules and forwards chart props", () => {
    const option = { series: [{ id: "candles" }] };
    const updateOption = { series: [{ id: "selected-trade" }] };
    const onDataClick = vi.fn();
    render(
      <CandlestickChart
        option={option}
        updateOption={updateOption}
        className="trade-chart"
        ariaLabel="trade candles"
        onDataClick={onDataClick}
      />
    );

    expect(chartModules.use).toHaveBeenCalledOnce();
    expect(chartModules.use).toHaveBeenCalledWith([
      chartModules.CandlestickSeries,
      chartModules.DataZoomComponent,
      chartModules.GridComponent,
      chartModules.LegendComponent,
      chartModules.ScatterChart,
      chartModules.TooltipComponent
    ]);
    expect(renderCore).toHaveBeenCalledWith({
      option,
      updateOption,
      className: "trade-chart",
      ariaLabel: "trade candles",
      onDataClick
    });
    expect(screen.getByRole("img", { name: "trade candles" })).toBeTruthy();
  });
});
