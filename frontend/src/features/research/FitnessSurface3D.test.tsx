// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { FitnessSurface3D } from "./FitnessSurface3D";
import type { SurfaceRow } from "./gaDomain";

const context = {
  arc: vi.fn(),
  beginPath: vi.fn(),
  clearRect: vi.fn(),
  closePath: vi.fn(),
  fill: vi.fn(),
  fillText: vi.fn(),
  lineTo: vi.fn(),
  moveTo: vi.fn(),
  setLineDash: vi.fn(),
  setTransform: vi.fn(),
  stroke: vi.fn(),
  fillStyle: "",
  font: "",
  globalAlpha: 1,
  lineWidth: 1,
  strokeStyle: ""
};

const resizeDisconnect = vi.fn();

class ResizeObserverMock {
  observe = vi.fn();
  disconnect = resizeDisconnect;
}

const best: SurfaceRow = [5, 20, 2, 2, "candidate-best"];
const other: SurfaceRow = [5, 20, 1, 1, "candidate-other"];

describe("FitnessSurface3D", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal("ResizeObserver", ResizeObserverMock);
    Object.defineProperty(HTMLCanvasElement.prototype, "getContext", {
      configurable: true,
      value: vi.fn(() => context)
    });
    Object.defineProperty(HTMLCanvasElement.prototype, "getBoundingClientRect", {
      configurable: true,
      value: vi.fn(() => ({
        bottom: 350,
        height: 350,
        left: 0,
        right: 600,
        top: 0,
        width: 600,
        x: 0,
        y: 0,
        toJSON: () => ({})
      }))
    });
    Object.defineProperty(HTMLCanvasElement.prototype, "setPointerCapture", {
      configurable: true,
      value: vi.fn()
    });
    Object.defineProperty(HTMLCanvasElement.prototype, "hasPointerCapture", {
      configurable: true,
      value: vi.fn(() => false)
    });
    Object.defineProperty(HTMLCanvasElement.prototype, "releasePointerCapture", {
      configurable: true,
      value: vi.fn()
    });
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("keeps non-best observations keyboard-accessible and selectable", () => {
    const onDataClick = vi.fn();
    render(
      <FitnessSurface3D
        surfaceRows={[best]}
        observations={[best, other]}
        selected={other}
        xParameter="fast"
        yParameter="slow"
        metricLabel="Fitness"
        observationLabel="Observed candidate"
        hint="Use arrow keys and Enter"
        locale="en"
        theme="light"
        ariaLabel="Interactive fitness surface"
        onDataClick={onDataClick}
      />
    );
    const canvas = screen.getByRole("application", {
      name: "Interactive fitness surface"
    });

    fireEvent.keyDown(canvas, { key: "ArrowRight" });
    expect(screen.getByRole("status").textContent).toContain("candidate-best");
    fireEvent.keyDown(canvas, { key: "ArrowRight" });
    const status = screen.getByRole("status");
    expect(status.textContent).toContain("candidate-other");
    expect(status.style.right).toBe("12px");
    fireEvent.keyDown(canvas, { key: "Enter" });

    expect(onDataClick).toHaveBeenCalledWith(other);
    const [x, y] = context.arc.mock.calls.at(-1) as [number, number];
    expect(x).toBeGreaterThanOrEqual(0);
    expect(x).toBeLessThanOrEqual(600);
    expect(y).toBeGreaterThanOrEqual(0);
    expect(y).toBeLessThanOrEqual(350);

    onDataClick.mockClear();
    fireEvent.pointerMove(canvas, { clientX: x, clientY: y });
    fireEvent.click(canvas, { clientX: x, clientY: y });
    expect(onDataClick).toHaveBeenCalledWith(other);

    onDataClick.mockClear();
    fireEvent.pointerLeave(canvas);
    expect(screen.queryByRole("status")).toBeNull();
    fireEvent.keyDown(canvas, { key: "Enter" });
    expect(onDataClick).not.toHaveBeenCalled();

    fireEvent.pointerDown(canvas, { clientX: x, clientY: y, pointerId: 1 });
    fireEvent.pointerMove(canvas, {
      clientX: x + 20,
      clientY: y + 10,
      pointerId: 1
    });
    fireEvent.pointerUp(canvas, {
      clientX: x + 20,
      clientY: y + 10,
      pointerId: 1
    });
    fireEvent.click(canvas, { clientX: x + 20, clientY: y + 10 });
    expect(onDataClick).not.toHaveBeenCalled();
    fireEvent.keyDown(canvas, { key: "Enter" });
    expect(onDataClick).not.toHaveBeenCalled();
  });

  it("wraps reverse keyboard navigation through all observations", () => {
    const view = render(
      <FitnessSurface3D
        surfaceRows={[best]}
        observations={[best, other]}
        selected={null}
        xParameter="fast"
        yParameter="slow"
        metricLabel="Fitness"
        observationLabel="Observed candidate"
        hint="Use arrow keys and Enter"
        locale="en"
        theme="dark"
        ariaLabel="Interactive fitness surface"
      />
    );

    fireEvent.keyDown(
      screen.getByRole("application", { name: "Interactive fitness surface" }),
      { key: "ArrowLeft" }
    );
    expect(screen.getByRole("status").textContent).toContain("candidate-other");

    view.unmount();
    expect(resizeDisconnect).toHaveBeenCalledOnce();
  });

  it("clears the active observation when the plotted axes change", () => {
    const surfaceRows = [best];
    const observations = [best, other];
    const view = render(
      <FitnessSurface3D
        surfaceRows={surfaceRows}
        observations={observations}
        selected={null}
        xParameter="fast"
        yParameter="slow"
        metricLabel="Fitness"
        observationLabel="Observed candidate"
        hint="Use arrow keys and Enter"
        locale="en"
        theme="light"
        ariaLabel="Interactive fitness surface"
      />
    );
    const canvas = screen.getByRole("application", {
      name: "Interactive fitness surface"
    });

    fireEvent.keyDown(canvas, { key: "ArrowRight" });
    expect(screen.getByRole("status").textContent).toContain("candidate-best");

    view.rerender(
      <FitnessSurface3D
        surfaceRows={surfaceRows}
        observations={observations}
        selected={null}
        xParameter="slow"
        yParameter="fast"
        metricLabel="Fitness"
        observationLabel="Observed candidate"
        hint="Use arrow keys and Enter"
        locale="en"
        theme="light"
        ariaLabel="Interactive fitness surface"
      />
    );

    expect(screen.queryByRole("status")).toBeNull();
  });

  it("redraws a translated metric label without rebuilding the mesh", () => {
    const surfaceRows = [best];
    const observations = [best, other];
    const view = render(
      <FitnessSurface3D
        surfaceRows={surfaceRows}
        observations={observations}
        selected={null}
        xParameter="fast"
        yParameter="slow"
        metricLabel="Fitness"
        observationLabel="Observed candidate"
        hint="Use arrow keys and Enter"
        locale="en"
        theme="light"
        ariaLabel="Interactive fitness surface"
      />
    );
    context.fillText.mockClear();

    view.rerender(
      <FitnessSurface3D
        surfaceRows={surfaceRows}
        observations={observations}
        selected={null}
        xParameter="fast"
        yParameter="slow"
        metricLabel="適應度"
        observationLabel="觀測候選"
        hint="使用方向鍵與 Enter"
        locale="zh-TW"
        theme="light"
        ariaLabel="互動式適應度曲面"
      />
    );

    expect(resizeDisconnect).not.toHaveBeenCalled();
    expect(context.fillText).toHaveBeenCalledWith("適應度", 18, 24);
  });
});
