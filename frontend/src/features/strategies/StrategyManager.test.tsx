// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError, type StrategyState, type StrategyStatus } from "../../api";
import i18n from "../../i18n";
import { StrategyManager } from "./StrategyManager";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadStrategyStates: vi.fn(),
  sendStrategyCommand: vi.fn()
}));

vi.mock("../../api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api")>()),
  ...api
}));

function strategy(
  strategyId: string,
  status: StrategyStatus,
  error: string | null = null
): StrategyState {
  const availableCommands = {
    DISCOVERED: ["START"],
    READY: ["START"],
    WARNING: ["START"],
    ACTIVE: ["STOP"],
    STOPPED: ["RESUME"],
    ERROR: ["FORCE_RECOVER"]
  } as const;
  return {
    strategy_id: strategyId,
    status,
    config: {},
    performance: {},
    last_heartbeat: 1_753_680_000_000,
    uptime_start: 1_753_676_400_000,
    last_error_message: error,
    entered_error_at: null,
    recovered_at: null,
    stopped_at: null,
    version: 3,
    available_commands: [...availableCommands[status]]
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("strategy management", () => {
  beforeEach(async () => {
    vi.resetAllMocks();
    window.sessionStorage.clear();
    await i18n.changeLanguage("zh-TW");
    api.ensureBrowserSession.mockResolvedValue({
      actor: "operator@example.com",
      capabilities: [],
      csrf_token: "csrf-token",
      expires_at: "2026-07-29T12:00:00Z",
      step_up_expires_at: "2026-07-29T11:00:00Z"
    });
    api.loadStrategyStates.mockResolvedValue([
      strategy("active-strategy", "ACTIVE"),
      strategy("stopped-strategy", "STOPPED"),
      strategy("error-strategy", "ERROR", "feed disconnected"),
      strategy("ready-strategy", "READY")
    ]);
    api.sendStrategyCommand.mockResolvedValue(undefined);
    vi.spyOn(window, "confirm").mockReturnValue(true);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("renders authoritative status and only implemented state actions", async () => {
    render(<StrategyManager />);

    expect(await screen.findByText("active-strategy")).toBeTruthy();
    expect(screen.getByText("feed disconnected")).toBeTruthy();
    expect(screen.getByRole("button", { name: "停止" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "恢復" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "強制恢復" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "啟動" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "重新載入" })).toBeNull();
    expect(screen.getByLabelText("策略狀態摘要").textContent).toContain("4");
  });

  it("renders only commands supplied by the authoritative state contract", async () => {
    api.loadStrategyStates.mockResolvedValue([
      {
        ...strategy("active-strategy", "ACTIVE"),
        available_commands: []
      }
    ]);
    render(<StrategyManager />);

    expect(await screen.findByText("active-strategy")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "停止" })).toBeNull();
  });

  it("does not send a command when confirmation is declined", async () => {
    vi.mocked(window.confirm).mockReturnValue(false);
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(api.sendStrategyCommand).not.toHaveBeenCalled();
  });

  it("serializes rapid command interactions before a second confirmation", async () => {
    const request = deferred();
    api.sendStrategyCommand.mockReturnValue(request.promise);
    render(<StrategyManager />);
    const stop = await screen.findByRole("button", { name: "停止" });

    fireEvent.click(stop);
    fireEvent.click(stop);

    expect(window.confirm).toHaveBeenCalledTimes(1);
    expect(api.sendStrategyCommand).toHaveBeenCalledTimes(1);
    request.resolve();
    await waitFor(() => expect(api.loadStrategyStates).toHaveBeenCalledTimes(2));
  });

  it("sends one CSRF-protected command, locks actions, and refreshes", async () => {
    const request = deferred();
    api.sendStrategyCommand.mockReturnValue(request.promise);
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(window.confirm).toHaveBeenCalledWith(
      "確定要對 active-strategy 執行「停止」？"
    );
    expect(api.sendStrategyCommand).toHaveBeenCalledWith(
      "active-strategy",
      "STOP",
      3,
      expect.any(String),
      "csrf-token"
    );
    expect(
      (screen.getByRole("button", { name: "送出中" }) as HTMLButtonElement)
        .disabled
    ).toBe(true);
    expect(
      (screen.getByRole("button", { name: "恢復" }) as HTMLButtonElement)
        .disabled
    ).toBe(true);

    request.resolve();
    expect(
      await screen.findByText("已接受 active-strategy 的「停止」命令。")
    ).toBeTruthy();
    await waitFor(() => expect(api.loadStrategyStates).toHaveBeenCalledTimes(2));
    expect(
      (screen.getByRole("button", {
        name: "等待狀態更新"
      }) as HTMLButtonElement).disabled
    ).toBe(true);

    fireEvent.click(screen.getByRole("button", { name: "重新整理狀態" }));
    await waitFor(() => expect(api.loadStrategyStates).toHaveBeenCalledTimes(3));
    expect(
      (screen.getByRole("button", {
        name: "等待狀態更新"
      }) as HTMLButtonElement).disabled
    ).toBe(true);

    api.loadStrategyStates.mockResolvedValue([
      { ...strategy("active-strategy", "STOPPED"), version: 4 },
      strategy("stopped-strategy", "STOPPED"),
      strategy("error-strategy", "ERROR", "feed disconnected"),
      strategy("ready-strategy", "READY")
    ]);
    fireEvent.click(screen.getByRole("button", { name: "重新整理狀態" }));

    await waitFor(() => expect(api.loadStrategyStates).toHaveBeenCalledTimes(4));
    expect(screen.queryByText("等待狀態更新")).toBeNull();
    expect(screen.getAllByRole("button", { name: "恢復" })).toHaveLength(2);
  });

  it("explains missing step-up access without discarding the snapshot", async () => {
    api.sendStrategyCommand.mockRejectedValue(
      new ApiError("step_up_required", 403)
    );
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "強制恢復" }));

    expect(
      await screen.findByText(
        "這項命令需要短效升權。請取得 step-up 權限後重新建立工作階段。"
      )
    ).toBeTruthy();
    expect(screen.getByText("策略命令未送出")).toBeTruthy();
    expect(screen.getByText("error-strategy")).toBeTruthy();
    expect(
      (screen.getByRole("button", {
        name: "強制恢復"
      }) as HTMLButtonElement).disabled
    ).toBe(false);
  });

  it("fails closed when the command outcome is ambiguous", async () => {
    api.sendStrategyCommand.mockRejectedValue(new TypeError("Failed to fetch"));
    const firstRender = render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(await screen.findByText("策略命令結果不明")).toBeTruthy();
    expect(
      screen.getByText(
        "控制面可能已接受命令；在權威狀態更新前不會重新開放操作。"
      )
    ).toBeTruthy();
    expect(
      (screen.getByRole("button", {
        name: "等待狀態更新"
      }) as HTMLButtonElement).disabled
    ).toBe(true);

    fireEvent.click(screen.getByRole("button", { name: "重新整理狀態" }));
    await waitFor(() => expect(api.loadStrategyStates).toHaveBeenCalledTimes(2));
    expect(screen.getByRole("button", { name: "等待狀態更新" })).toBeTruthy();

    firstRender.unmount();
    render(<StrategyManager />);
    expect(
      await screen.findByRole("button", { name: "等待狀態更新" })
    ).toBeTruthy();
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(3);
    expect(api.loadStrategyStates).toHaveBeenCalledTimes(3);
  });

  it("treats a server-side timeout as an ambiguous command outcome", async () => {
    api.sendStrategyCommand.mockRejectedValue(
      new ApiError("gateway_timeout", 504)
    );
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(await screen.findByText("策略命令結果不明")).toBeTruthy();
    expect(screen.getByRole("button", { name: "等待狀態更新" })).toBeTruthy();
  });

  it("unlocks after the control plane confirms there is no engine listener", async () => {
    api.sendStrategyCommand.mockRejectedValue(
      new ApiError("strategy_engine_listener_unavailable", 503)
    );
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(await screen.findByText("策略命令未送出")).toBeTruthy();
    expect(
      (screen.getByRole("button", { name: "停止" }) as HTMLButtonElement)
        .disabled
    ).toBe(false);
    expect(screen.queryByText("等待狀態更新")).toBeNull();
  });

  it("keeps an uncertain lock when a snapshot temporarily omits the strategy", async () => {
    api.sendStrategyCommand.mockRejectedValue(new TypeError("Failed to fetch"));
    render(<StrategyManager />);
    fireEvent.click(await screen.findByRole("button", { name: "停止" }));
    await screen.findByText("策略命令結果不明");

    api.loadStrategyStates.mockResolvedValue([
      strategy("stopped-strategy", "STOPPED")
    ]);
    fireEvent.click(screen.getByRole("button", { name: "重新整理狀態" }));
    await waitFor(() => expect(screen.queryByText("active-strategy")).toBeNull());

    api.loadStrategyStates.mockResolvedValue([
      strategy("active-strategy", "ACTIVE")
    ]);
    fireEvent.click(screen.getByRole("button", { name: "重新整理狀態" }));

    expect(
      await screen.findByRole("button", { name: "等待狀態更新" })
    ).toBeTruthy();
  });

  it("keeps an accepted command distinct from a failed state refresh", async () => {
    api.loadStrategyStates
      .mockResolvedValueOnce([strategy("active-strategy", "ACTIVE")])
      .mockRejectedValueOnce(new ApiError("strategy_state_query_unavailable", 503));
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(
      await screen.findByText("已接受 active-strategy 的「停止」命令。")
    ).toBeTruthy();
    expect(screen.getByText("命令已接受，但狀態尚未更新")).toBeTruthy();
    expect(screen.queryByText("策略命令未送出")).toBeNull();
    expect(screen.getByText("active-strategy")).toBeTruthy();
    expect(screen.getByRole("button", { name: "等待狀態更新" })).toBeTruthy();
  });

  it("keeps an accepted lock when the refreshed snapshot omits its strategy", async () => {
    api.loadStrategyStates
      .mockResolvedValueOnce([strategy("active-strategy", "ACTIVE")])
      .mockResolvedValueOnce([]);
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(
      await screen.findByText("已接受 active-strategy 的「停止」命令。")
    ).toBeTruthy();
    await waitFor(() => expect(api.loadStrategyStates).toHaveBeenCalledTimes(2));
    expect(screen.queryByText("active-strategy")).toBeNull();
    expect(
      window.sessionStorage.getItem("fluxtrade.strategy.awaiting")
    ).toBe('[["active-strategy",{"status":"ACTIVE","version":3}]]');
  });

  it("unlocks the next action after the authoritative state changes", async () => {
    api.loadStrategyStates
      .mockResolvedValueOnce([strategy("active-strategy", "ACTIVE")])
      .mockResolvedValueOnce([
        { ...strategy("active-strategy", "STOPPED"), version: 4 }
      ]);
    render(<StrategyManager />);

    fireEvent.click(await screen.findByRole("button", { name: "停止" }));

    expect(await screen.findByRole("button", { name: "恢復" })).toBeTruthy();
    expect(screen.queryByText("等待狀態更新")).toBeNull();
  });

  it("shows a directional empty state", async () => {
    api.loadStrategyStates.mockResolvedValue([]);
    render(<StrategyManager />);

    expect(await screen.findByText("尚無策略狀態")).toBeTruthy();
    expect(screen.getByLabelText("策略狀態摘要").textContent).toContain("0");
  });

  it("performs one fresh owner setup after active-only unmount and re-entry", async () => {
    window.sessionStorage.setItem(
      "fluxtrade.strategy.awaiting",
      '[["active-strategy",{"status":"ACTIVE","version":3}]]'
    );
    const first = render(<StrategyManager />);
    expect(
      await screen.findByRole("button", { name: "等待狀態更新" })
    ).toBeTruthy();
    first.unmount();

    render(<StrategyManager />);

    expect(
      await screen.findByRole("button", { name: "等待狀態更新" })
    ).toBeTruthy();
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(2);
    expect(api.loadStrategyStates).toHaveBeenCalledTimes(2);
  });
});
