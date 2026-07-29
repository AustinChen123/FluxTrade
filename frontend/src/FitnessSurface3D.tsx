import { useEffect, useId, useRef, useState } from "react";

import type { SurfaceRow } from "./ga";
import type { Theme } from "./theme";

type Props = {
  surfaceRows: SurfaceRow[];
  observations: SurfaceRow[];
  selected: SurfaceRow | null;
  xParameter: string;
  yParameter: string;
  metricLabel: string;
  observationLabel: string;
  hint: string;
  locale: string;
  theme: Theme;
  ariaLabel: string;
  onDataClick?: (data: unknown) => void;
};

type NormalizedPoint = {
  x: number;
  y: number;
  z: number;
  row: SurfaceRow;
};

type ProjectedPoint = NormalizedPoint & {
  screenX: number;
  screenY: number;
};

type HoverPoint = {
  x: number;
  row: SurfaceRow;
};

type Extents = {
  x: [number, number];
  y: [number, number];
  z: [number, number];
};

const MESH_COLUMNS = 24;
const MESH_ROWS = 18;

function extent(values: number[]): [number, number] {
  if (!values.length) {
    return [0, 1];
  }
  let min = values[0];
  let max = values[0];
  for (const value of values) {
    min = Math.min(min, value);
    max = Math.max(max, value);
  }
  return [min, max];
}

function normalize(value: number, [min, max]: [number, number]): number {
  return max === min ? 0.5 : (value - min) / (max - min);
}

function rowExtents(rows: SurfaceRow[]): Extents {
  return {
    x: extent(rows.map((row) => row[0])),
    y: extent(rows.map((row) => row[1])),
    z: extent(rows.map((row) => row[2]))
  };
}

function normalizeRow(row: SurfaceRow, extents: Extents): NormalizedPoint {
  return {
    x: normalize(row[0], extents.x),
    y: normalize(row[1], extents.y),
    z: normalize(row[2], extents.z),
    row
  };
}

function interpolate(
  points: NormalizedPoint[],
  x: number,
  y: number
): number {
  let weighted = 0;
  let weights = 0;
  for (const point of points) {
    const distance = (point.x - x) ** 2 + (point.y - y) ** 2;
    if (distance < 1e-8) {
      return point.z;
    }
    const weight = 1 / distance;
    weighted += point.z * weight;
    weights += weight;
  }
  return weights === 0 ? 0.5 : weighted / weights;
}

function project(
  point: Pick<NormalizedPoint, "x" | "y" | "z">,
  width: number,
  height: number,
  yaw: number,
  pitch: number,
  zoom: number
) {
  const x = (point.x - 0.5) * 2;
  const y = (point.y - 0.5) * 2;
  const z = (point.z - 0.5) * 1.25;
  const yawX = x * Math.cos(yaw) - y * Math.sin(yaw);
  const yawY = x * Math.sin(yaw) + y * Math.cos(yaw);
  const pitchY = yawY * Math.cos(pitch) + z * Math.sin(pitch);
  const depth = yawY * Math.sin(pitch) - z * Math.cos(pitch);
  const perspective = 2.8 / (3.25 - depth);
  const scale =
    Math.min(width, height) * (width < 480 ? 0.44 : 0.52) * zoom;
  return {
    x: width / 2 + yawX * scale * perspective,
    y: height * 0.5 - pitchY * scale * perspective,
    depth
  };
}

function colorChannel(color: string, offset: number): number {
  return Number.parseInt(color.slice(offset, offset + 2), 16);
}

function mixColor(low: string, high: string, amount: number): string {
  const t = Math.max(0, Math.min(1, amount));
  const channel = (offset: number) =>
    Math.round(
      colorChannel(low, offset) * (1 - t) +
        colorChannel(high, offset) * t
    );
  return `rgb(${channel(1)}, ${channel(3)}, ${channel(5)})`;
}

export function FitnessSurface3D({
  surfaceRows,
  observations,
  selected,
  xParameter,
  yParameter,
  metricLabel,
  observationLabel,
  hint,
  locale,
  theme,
  ariaLabel,
  onDataClick
}: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const hintId = useId();
  const clickRef = useRef(onDataClick);
  const selectedRef = useRef(selected);
  const metricLabelRef = useRef(metricLabel);
  const drawRef = useRef<() => void>(() => undefined);
  const [hover, setHover] = useState<HoverPoint | null>(null);
  clickRef.current = onDataClick;
  selectedRef.current = selected;
  metricLabelRef.current = metricLabel;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return;
    }
    const context = canvas.getContext("2d");
    if (!context) {
      return;
    }
    setHover(null);
    const extents = rowExtents(observations);
    const surfacePoints = surfaceRows.map((row) => normalizeRow(row, extents));
    const observedPoints = observations.map((row) =>
      normalizeRow(row, extents)
    );
    // ponytail: fixed mesh keeps interpolation bounded; raise only after profiling.
    const mesh = Array.from(
      { length: MESH_COLUMNS * MESH_ROWS },
      (_, index) => {
        const x = (index % MESH_COLUMNS) / (MESH_COLUMNS - 1);
        const y = Math.floor(index / MESH_COLUMNS) / (MESH_ROWS - 1);
        return { x, y, z: interpolate(surfacePoints, x, y) };
      }
    );
    const faces = Array.from(
      { length: (MESH_COLUMNS - 1) * (MESH_ROWS - 1) * 2 },
      (_, index) => {
        const cell = Math.floor(index / 2);
        const column = cell % (MESH_COLUMNS - 1);
        const row = Math.floor(cell / (MESH_COLUMNS - 1));
        const topLeft = row * MESH_COLUMNS + column;
        return index % 2 === 0
          ? [topLeft, topLeft + 1, topLeft + MESH_COLUMNS]
          : [
              topLeft + 1,
              topLeft + MESH_COLUMNS + 1,
              topLeft + MESH_COLUMNS
            ];
      }
    );
    const rotation = { yaw: -0.72, pitch: 0.9, zoom: 1 };
    let projectedPoints: ProjectedPoint[] = [];
    let dragging = false;
    let moved = false;
    let activePointIndex = -1;
    let lastX = 0;
    let lastY = 0;

    const draw = () => {
      const bounds = canvas.getBoundingClientRect();
      const width = bounds.width;
      const height = bounds.height;
      const deviceScale = Math.min(window.devicePixelRatio || 1, 2);
      const pixelWidth = Math.round(width * deviceScale);
      const pixelHeight = Math.round(height * deviceScale);
      if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
        canvas.width = pixelWidth;
        canvas.height = pixelHeight;
      }
      context.setTransform(deviceScale, 0, 0, deviceScale, 0, 0);
      context.clearRect(0, 0, width, height);
      if (!surfacePoints.length || !observedPoints.length) {
        return;
      }
      const styles = getComputedStyle(canvas);
      const low = styles.getPropertyValue("--teal").trim() || "#0e6b6f";
      const high = styles.getPropertyValue("--indigo").trim() || "#454b8c";
      const grid = styles.getPropertyValue("--grid").trim() || "#cbd5d1";
      const ink = styles.getPropertyValue("--ink").trim() || "#182128";
      const coral = styles.getPropertyValue("--coral").trim() || "#d46a4c";
      const paper = styles.getPropertyValue("--paper").trim() || "#eef2f1";
      const projectedMesh = mesh.map((point) => ({
        ...point,
        ...project(
          point,
          width,
          height,
          rotation.yaw,
          rotation.pitch,
          rotation.zoom
        )
      }));
      const orderedFaces = faces
        .map((face) => ({
          face,
          depth:
            face.reduce((sum, vertex) => sum + projectedMesh[vertex].depth, 0) /
            3
        }))
        .sort((left, right) => left.depth - right.depth);
      context.lineWidth = 0.55;
      for (const { face } of orderedFaces) {
        const vertices = face.map((index) => projectedMesh[index]);
        const fitness =
          vertices.reduce((sum, vertex) => sum + vertex.z, 0) / vertices.length;
        context.beginPath();
        context.moveTo(vertices[0].x, vertices[0].y);
        context.lineTo(vertices[1].x, vertices[1].y);
        context.lineTo(vertices[2].x, vertices[2].y);
        context.closePath();
        context.fillStyle = mixColor(low, high, fitness);
        context.globalAlpha = 0.84;
        context.fill();
        context.strokeStyle = grid;
        context.globalAlpha = 0.32;
        context.stroke();
      }
      context.globalAlpha = 1;
      projectedPoints = observedPoints.map((point) => {
        const projected = project(
          point,
          width,
          height,
          rotation.yaw,
          rotation.pitch,
          rotation.zoom
        );
        return {
          ...point,
          screenX: projected.x,
          screenY: projected.y
        };
      });
      const selectedPoint = selectedRef.current
        ? normalizeRow(selectedRef.current, extents)
        : null;
      if (selectedPoint) {
        const surfacePoint = project(
          {
            x: selectedPoint.x,
            y: selectedPoint.y,
            z: interpolate(surfacePoints, selectedPoint.x, selectedPoint.y)
          },
          width,
          height,
          rotation.yaw,
          rotation.pitch,
          rotation.zoom
        );
        const marker = project(
          selectedPoint,
          width,
          height,
          rotation.yaw,
          rotation.pitch,
          rotation.zoom
        );
        context.beginPath();
        context.moveTo(surfacePoint.x, surfacePoint.y);
        context.lineTo(marker.x, marker.y);
        context.strokeStyle = coral;
        context.lineWidth = 1;
        context.globalAlpha = 0.55;
        context.setLineDash([3, 3]);
        context.stroke();
        context.setLineDash([]);
        context.globalAlpha = 1;
        context.beginPath();
        context.arc(marker.x, marker.y, 7.5, 0, Math.PI * 2);
        context.fillStyle = coral;
        context.fill();
        context.lineWidth = 3;
        context.strokeStyle = paper;
        context.stroke();
      }
      context.fillStyle = ink;
      context.font = '11px "SFMono-Regular", "Roboto Mono", monospace';
      context.fillText(xParameter, width - 78, height - 24);
      context.fillText(yParameter, 18, height - 24);
      context.fillText(metricLabelRef.current, 18, 24);
    };

    const hitPoint = (event: PointerEvent): ProjectedPoint | null => {
      const bounds = canvas.getBoundingClientRect();
      const x = event.clientX - bounds.left;
      const y = event.clientY - bounds.top;
      let nearest: ProjectedPoint | null = null;
      let nearestDistance = 12 ** 2;
      for (const point of projectedPoints) {
        const distance =
          (point.screenX - x) ** 2 + (point.screenY - y) ** 2;
        if (distance <= nearestDistance) {
          nearest = point;
          nearestDistance = distance;
        }
      }
      return nearest;
    };
    const showPoint = (point: ProjectedPoint | null) => {
      if (!point) {
        setHover(null);
        return;
      }
      const bounds = canvas.getBoundingClientRect();
      setHover({
        x: point.screenX - bounds.width / 2,
        row: point.row
      });
    };
    const pointerDown = (event: PointerEvent) => {
      dragging = true;
      moved = false;
      lastX = event.clientX;
      lastY = event.clientY;
      canvas.focus();
      canvas.setPointerCapture(event.pointerId);
      canvas.style.cursor = "grabbing";
    };
    const pointerMove = (event: PointerEvent) => {
      if (dragging) {
        const dx = event.clientX - lastX;
        const dy = event.clientY - lastY;
        moved ||= Math.abs(dx) + Math.abs(dy) > 2;
        if (moved) {
          activePointIndex = -1;
        }
        rotation.yaw += dx * 0.008;
        rotation.pitch = Math.max(
          0.2,
          Math.min(1.35, rotation.pitch + dy * 0.006)
        );
        lastX = event.clientX;
        lastY = event.clientY;
        setHover(null);
        draw();
        return;
      }
      const point = hitPoint(event);
      canvas.style.cursor = point ? "pointer" : "grab";
      activePointIndex = point ? projectedPoints.indexOf(point) : -1;
      showPoint(point);
    };
    const pointerUp = (event: PointerEvent) => {
      dragging = false;
      if (canvas.hasPointerCapture(event.pointerId)) {
        canvas.releasePointerCapture(event.pointerId);
      }
      canvas.style.cursor = "grab";
    };
    const pointerLeave = () => {
      activePointIndex = -1;
      if (!dragging) {
        setHover(null);
        canvas.style.cursor = "grab";
      }
    };
    const blur = () => {
      activePointIndex = -1;
      setHover(null);
    };
    const keyDown = (event: KeyboardEvent) => {
      if (!projectedPoints.length) {
        return;
      }
      const previous =
        event.key === "ArrowLeft" || event.key === "ArrowUp";
      const next =
        event.key === "ArrowRight" || event.key === "ArrowDown";
      if (previous || next || event.key === "Home" || event.key === "End") {
        event.preventDefault();
        if (event.key === "Home") {
          activePointIndex = 0;
        } else if (event.key === "End") {
          activePointIndex = projectedPoints.length - 1;
        } else {
          const direction = previous ? -1 : 1;
          activePointIndex =
            activePointIndex === -1
              ? previous
                ? projectedPoints.length - 1
                : 0
              : (activePointIndex + direction + projectedPoints.length) %
                projectedPoints.length;
        }
        showPoint(projectedPoints[activePointIndex]);
        return;
      }
      if (
        activePointIndex >= 0 &&
        (event.key === "Enter" || event.key === " ")
      ) {
        event.preventDefault();
        clickRef.current?.(projectedPoints[activePointIndex].row);
      } else if (event.key === "Escape") {
        activePointIndex = -1;
        setHover(null);
      }
    };
    const click = (event: PointerEvent) => {
      if (!moved) {
        const point = hitPoint(event);
        if (point) {
          clickRef.current?.(point.row);
        }
      }
    };
    const wheel = (event: WheelEvent) => {
      event.preventDefault();
      rotation.zoom = Math.max(
        0.7,
        Math.min(1.5, rotation.zoom - event.deltaY * 0.001)
      );
      draw();
    };
    const observer = new ResizeObserver(draw);
    observer.observe(canvas);
    canvas.addEventListener("pointerdown", pointerDown);
    canvas.addEventListener("pointermove", pointerMove);
    canvas.addEventListener("pointerup", pointerUp);
    canvas.addEventListener("pointercancel", pointerUp);
    canvas.addEventListener("pointerleave", pointerLeave);
    canvas.addEventListener("blur", blur);
    canvas.addEventListener("keydown", keyDown);
    canvas.addEventListener("click", click);
    canvas.addEventListener("wheel", wheel, { passive: false });
    drawRef.current = draw;
    draw();
    return () => {
      drawRef.current = () => undefined;
      observer.disconnect();
      canvas.removeEventListener("pointerdown", pointerDown);
      canvas.removeEventListener("pointermove", pointerMove);
      canvas.removeEventListener("pointerup", pointerUp);
      canvas.removeEventListener("pointercancel", pointerUp);
      canvas.removeEventListener("pointerleave", pointerLeave);
      canvas.removeEventListener("blur", blur);
      canvas.removeEventListener("keydown", keyDown);
      canvas.removeEventListener("click", click);
      canvas.removeEventListener("wheel", wheel);
    };
  }, [observations, surfaceRows, xParameter, yParameter]);

  useEffect(() => {
    drawRef.current();
  }, [metricLabel, selected, theme]);

  const formatter = new Intl.NumberFormat(locale, {
    maximumFractionDigits: 8
  });

  return (
    <div className="chart-3d-shell">
      <canvas
        ref={canvasRef}
        className="chart chart-topology chart-3d"
        role="application"
        tabIndex={0}
        aria-label={ariaLabel}
        aria-describedby={hintId}
      />
      <p id={hintId} className="surface-hint">{hint}</p>
      {hover && (
        <div
          className="surface-tooltip"
          role="status"
          aria-live="polite"
          style={
            hover.x <= 0
              ? { right: "12px", top: "12px" }
              : { left: "12px", top: "12px" }
          }
        >
          <strong>{hover.row[4]}</strong>
          <span>{observationLabel}</span>
          <span>{xParameter}: {formatter.format(hover.row[0])}</span>
          <span>{yParameter}: {formatter.format(hover.row[1])}</span>
          <span>{metricLabel}: {formatter.format(hover.row[2])}</span>
        </div>
      )}
    </div>
  );
}
