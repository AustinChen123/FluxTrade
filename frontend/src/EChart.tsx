import { useEffect, useRef } from "react";
import {
  GridComponent,
  LegendComponent,
  ParallelComponent,
  TooltipComponent,
  VisualMapComponent
} from "echarts/components";
import {
  CustomChart,
  LineChart,
  ParallelChart
} from "echarts/charts";
import { CanvasRenderer } from "echarts/renderers";
import * as echarts from "echarts/core";
import type { EChartsCoreOption } from "echarts/core";

echarts.use([
  CanvasRenderer,
  GridComponent,
  LegendComponent,
  CustomChart,
  LineChart,
  ParallelChart,
  ParallelComponent,
  TooltipComponent,
  VisualMapComponent
]);

type Props = {
  option: EChartsCoreOption;
  updateOption?: EChartsCoreOption;
  className?: string;
  ariaLabel: string;
  onDataClick?: (data: unknown, dataIndex: number) => void;
};

export function EChart({
  option,
  updateOption,
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

  useEffect(() => {
    const chart = containerRef.current
      ? echarts.getInstanceByDom(containerRef.current)
      : undefined;
    if (updateOption) {
      chart?.setOption(updateOption, { lazyUpdate: true });
    }
  }, [option, updateOption]);

  return (
    <div
      ref={containerRef}
      className={className}
      role="img"
      aria-label={ariaLabel}
    />
  );
}
