// @vitest-environment jsdom

import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError, type StrategyState } from "../../api";
import i18n from "../../shared/i18n";
import { AWAITING_STORAGE_KEY } from "./strategyCommandState";
import { useStrategyManager } from "./useStrategyManager";

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
  status: StrategyState["status"] = "ACTIVE",
  version = 3
): StrategyState {
  return {
    strategy_id: "active-strategy",
    status,
    config: {},
    performance: {},
    last_heartbeat: null,
    uptime_start: null,
    last_error_message: null,
    entered_error_at: null,
    recovered_at: null,
    stopped_at: null,
    version,
    available_commands: status === "ACTIVE" ? ["STOP"] : ["RESUME"]
  };
}

describe("useStrategyManager", () => {
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
    api.loadStrategyStates.mockResolvedValue([strategy()]);
    api.sendStrategyCommand.mockResolvedValue(undefined);
    vi.spyOn(window, "confirm").mockReturnValue(true);
    vi.spyOn(window.crypto, "randomUUID").mockReturnValue(
      "00000000-0000-4000-8000-000000000001"
    );
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("restores the exact persisted lock before one owner setup refresh", async () => {
    window.sessionStorage.setItem(
      AWAITING_STORAGE_KEY,
      '[["active-strategy",{"status":"ACTIVE","version":3}]]'
    );
    const { result } = renderHook(() => useStrategyManager(i18n.t));

    expect(result.current.awaitingStrategies.has("active-strategy")).toBe(true);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(1);
    expect(api.loadStrategyStates).toHaveBeenCalledTimes(1);
    expect(result.current.awaitingStrategies.has("active-strategy")).toBe(true);
  });

  it("ignores storage read failure and still performs the authoritative load", async () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new DOMException("blocked");
    });
    const { result } = renderHook(() => useStrategyManager(i18n.t));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect([...result.current.awaitingStrategies]).toEqual([]);
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(1);
    expect(api.loadStrategyStates).toHaveBeenCalledTimes(1);
  });

  it("keeps the authoritative load usable when empty-storage removal fails", async () => {
    vi.spyOn(Storage.prototype, "removeItem").mockImplementation(() => {
      throw new DOMException("blocked");
    });
    const { result } = renderHook(() => useStrategyManager(i18n.t));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.strategies).toEqual([strategy()]);
    expect([...result.current.awaitingStrategies]).toEqual([]);
  });

  it.each([
    [new ApiError("step_up_required", 403), { type: "step_up" }],
    [new ApiError("unauthorized", 401), { type: "unauthorized" }],
    [new ApiError("session_unavailable", 503), { type: "service", status: 503 }],
    [new TypeError("Failed to fetch"), { type: "generic" }]
  ] as const)(
    "classifies browser-session setup failure %s without loading states",
    async (reason, detail) => {
      api.ensureBrowserSession.mockRejectedValue(reason);
      const { result } = renderHook(() => useStrategyManager(i18n.t));

      await waitFor(() => expect(result.current.loading).toBe(false));
      expect(result.current.error).toEqual({ kind: "load", detail });
      expect(api.loadStrategyStates).not.toHaveBeenCalled();
      expect(result.current.strategies).toEqual([]);
    }
  );

  it("retains an ambiguous lock in memory when storage writes fail", async () => {
    api.sendStrategyCommand.mockRejectedValue(new TypeError("Failed to fetch"));
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("quota");
    });

    await act(async () => {
      await result.current.submit(strategy(), "STOP");
    });

    expect(result.current.error?.kind).toBe("unknown");
    expect(result.current.awaitingStrategies.has("active-strategy")).toBe(true);
    expect(api.sendStrategyCommand).toHaveBeenCalledWith(
      "active-strategy",
      "STOP",
      3,
      "00000000-0000-4000-8000-000000000001",
      "csrf-token"
    );
  });

  it("fails before send and durable lock when UUID creation fails", async () => {
    vi.mocked(window.crypto.randomUUID).mockImplementation(() => {
      throw new Error("uuid unavailable");
    });
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.submit(strategy(), "STOP");
    });

    expect(api.sendStrategyCommand).not.toHaveBeenCalled();
    expect(result.current.pendingStrategyId).toBeNull();
    expect([...result.current.awaitingStrategies]).toEqual([]);
    expect(result.current.error).toEqual({
      kind: "command",
      detail: { type: "generic" }
    });
  });

  it.each([
    [new ApiError("bad_request", 400), false, "command"],
    [new ApiError("request_timeout", 408), true, "unknown"],
    [new ApiError("server_failure", 503), true, "unknown"],
    [new TypeError("Failed to fetch"), true, "unknown"]
  ] as const)(
    "applies the command failure disposition for %s",
    async (reason, locked, errorKind) => {
      api.sendStrategyCommand.mockRejectedValue(reason);
      const { result } = renderHook(() => useStrategyManager(i18n.t));
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.submit(strategy(), "STOP");
      });

      expect(result.current.awaitingStrategies.has("active-strategy")).toBe(
        locked
      );
      expect(result.current.error?.kind).toBe(errorKind);
      expect(result.current.pendingStrategyId).toBeNull();
    }
  );
});
