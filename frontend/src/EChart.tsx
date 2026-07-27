import { useEffect, useRef } from "react";
import {
  DataZoomComponent,
  DatasetComponent,
  GridComponent,
  LegendComponent,
  ParallelComponent,
  TooltipComponent,
  VisualMapComponent
} from "echarts/components";
import { LineChart, ParallelChart, ScatterChart } from "echarts/charts";
import { CanvasRenderer } from "echarts/renderers";
import * as echarts from "echarts/core";
import type { EChartsCoreOption } from "echarts/core";

echarts.use([
  CanvasRenderer,
  DataZoomComponent,
  DatasetComponent,
  GridComponent,
  LegendComponent,
  LineChart,
  ParallelChart,
  ParallelComponent,
  ScatterChart,
  TooltipComponent,
  VisualMapComponent
]);

type Props = {
  option: EChartsCoreOption;
  className?: string;
  ariaLabel: string;
  onDataClick?: (data: unknown, dataIndex: number) => void;
};

export function EChart({
  option,
  className,
  ariaLabel,
  onDataClick
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const clickRef = useRef(onDataClick);
  clickRef.current = onDataClick;

  useEffect(() => {
    if (!containerRef.current) {
      return;
    }
    const chart = echarts.init(containerRef.current, undefined, {
      renderer: "canvas"
    });
    chart.on("click", (event) => {
      if (typeof event.dataIndex === "number") {
        clickRef.current?.(event.data, event.dataIndex);
      }
    });
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(containerRef.current);
    return () => {
      observer.disconnect();
      chart.dispose();
    };
  }, []);

  useEffect(() => {
    const chart = containerRef.current
      ? echarts.getInstanceByDom(containerRef.current)
      : undefined;
    chart?.setOption(option, { notMerge: true, lazyUpdate: true });
  }, [option]);

  return (
    <div
      ref={containerRef}
      className={className}
      role="img"
      aria-label={ariaLabel}
    />
  );
}
