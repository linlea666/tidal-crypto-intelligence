import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import {
  BarChart,
  LineChart,
  HeatmapChart,
  CandlestickChart,
} from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  VisualMapComponent,
  DataZoomComponent,
  LegendComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
echarts.use([
  BarChart,
  LineChart,
  HeatmapChart,
  CandlestickChart,
  GridComponent,
  TooltipComponent,
  VisualMapComponent,
  DataZoomComponent,
  LegendComponent,
  CanvasRenderer,
]);
export function Chart({
  option,
  height = 280,
  label,
}: {
  option: Record<string, unknown>;
  height?: number;
  label: string;
}) {
  const node = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!node.current) return;
    const axis = {
      axisLabel: { color: "#a0aeaa" },
      axisLine: { lineStyle: { color: "#43504d" } },
    };
    const chart = echarts.init(
      node.current,
      {
        categoryAxis: axis,
        valueAxis: axis,
        timeAxis: axis,
        logAxis: axis,
        legend: { textStyle: { color: "#a0aeaa" } },
      },
      { renderer: "canvas" },
    );
    chart.setOption({
      animation: false,
      textStyle: {
        fontFamily: "Inter, PingFang SC, sans-serif",
        color: "#a0aeaa",
      },
      ...option,
    });
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(node.current);
    return () => {
      observer.disconnect();
      chart.dispose();
    };
  }, [option]);
  return (
    <div
      ref={node}
      role="img"
      aria-label={label}
      style={{ height, width: "100%" }}
    />
  );
}
