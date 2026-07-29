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
import * as echarts from "echarts/core";
import { EChartCore, type EChartProps } from "./EChartCore";

echarts.use([
  GridComponent,
  LegendComponent,
  CustomChart,
  LineChart,
  ParallelChart,
  ParallelComponent,
  TooltipComponent,
  VisualMapComponent
]);

export function EChart(props: EChartProps) {
  return <EChartCore {...props} />;
}
