import {
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent
} from "echarts/components";
import { CandlestickChart as CandlestickSeries, ScatterChart } from "echarts/charts";
import * as echarts from "echarts/core";

import { EChartCore, type EChartProps } from "../../EChartCore";

echarts.use([
  CandlestickSeries,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  ScatterChart,
  TooltipComponent
]);

export function CandlestickChart(props: EChartProps) {
  return <EChartCore {...props} />;
}
