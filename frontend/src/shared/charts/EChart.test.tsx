// @vitest-environment jsdom

import { cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const echarts = vi.hoisted(() => {
  const chart = {
    dispose: vi.fn(),
    on: vi.fn(),
    resize: vi.fn(),
    setOption: vi.fn()
  };
  return {
    chart,
    getInstanceByDom: vi.fn(() => chart),
    init: vi.fn(() => chart),
    use: vi.fn()
  };
});

vi.mock("echarts/core", () => echarts);
vi.mock("echarts/components", () => ({
  GridComponent: {},
  LegendComponent: {},
  ParallelComponent: {},
  TooltipComponent: {},
  VisualMapComponent: {}
}));
vi.mock("echarts/charts", () => ({
  CustomChart: {},
  LineChart: {},
  ParallelChart: {}
}));
vi.mock("echarts/renderers", () => ({ CanvasRenderer: {} }));

import { EChart } from "./EChart";

class ResizeObserverMock {
  observe = vi.fn();
  disconnect = vi.fn();
}

describe("EChart", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("ResizeObserver", ResizeObserverMock);
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("merges selection updates without resetting the base surface", async () => {
    const base = { series: [{ id: "fitness-surface", data: [[1, 2, 3]] }] };
    const firstSelection = {
      series: [{ id: "selected-candidate", data: [[1, 2, 3]] }]
    };
    const secondSelection = {
      series: [{ id: "selected-candidate", data: [[4, 5, 6]] }]
    };
    const view = render(
      <EChart
        option={base}
        updateOption={firstSelection}
        ariaLabel="surface"
      />
    );

    await waitFor(() => expect(echarts.chart.setOption).toHaveBeenCalledTimes(2));
    expect(echarts.chart.setOption).toHaveBeenNthCalledWith(1, base, {
      notMerge: true,
      lazyUpdate: true
    });
    expect(echarts.chart.setOption).toHaveBeenNthCalledWith(2, firstSelection, {
      lazyUpdate: true
    });

    view.rerender(
      <EChart
        option={base}
        updateOption={secondSelection}
        ariaLabel="surface"
      />
    );

    await waitFor(() => expect(echarts.chart.setOption).toHaveBeenCalledTimes(3));
    expect(echarts.chart.setOption).toHaveBeenLastCalledWith(secondSelection, {
      lazyUpdate: true
    });
  });

  it("forwards chart data clicks and disposes the instance", async () => {
    const onDataClick = vi.fn();
    const view = render(
      <EChart option={{}} ariaLabel="chart" onDataClick={onDataClick} />
    );
    await waitFor(() => expect(echarts.chart.on).toHaveBeenCalled());
    const click = echarts.chart.on.mock.calls.find(
      ([event]) => event === "click"
    )?.[1] as (event: { dataIndex: number; data: unknown }) => void;

    click({ dataIndex: 2, data: [5, 20, 1, 9] });
    expect(onDataClick).toHaveBeenCalledWith([5, 20, 1, 9], 2);

    view.unmount();
    expect(echarts.chart.dispose).toHaveBeenCalledOnce();
  });
});
