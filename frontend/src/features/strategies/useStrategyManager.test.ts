// @vitest-environment jsdom

import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError, type KillSwitchStatus, type StrategyState } from "../../api";
import i18n from "../../shared/i18n";
import { AWAITING_STORAGE_KEY } from "./strategyCommandState";
import { useStrategyManager } from "./useStrategyManager";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadKillSwitchStatus: vi.fn(),
  clearKillSwitch: vi.fn(),
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
      permissions: { can_mutate: true, can_step_up: true },
      csrf_token: "csrf-token",
      expires_at: "2026-07-29T12:00:00Z",
      step_up_expires_at: "2026-07-29T11:00:00Z"
    });
    api.loadKillSwitchStatus.mockResolvedValue({ state: "OK", redis_state: "OK", durable_state: "OK", listener_available: true });
    api.clearKillSwitch.mockResolvedValue(undefined);
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
    vi.unstubAllGlobals();
  });

  it.each([
    [false, false, "STOP"], [false, true, "RESUME"], [true, false, "RESUME"]
  ] as const)("rejects direct submit without permission %s/%s/%s", async (can_mutate, can_step_up, command) => {
    api.ensureBrowserSession.mockResolvedValue({ permissions: { can_mutate, can_step_up } });
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await act(() => result.current.submit(strategy(command === "STOP" ? "ACTIVE" : "STOPPED"), command));
    expect(window.confirm).not.toHaveBeenCalled();
    expect(api.sendStrategyCommand).not.toHaveBeenCalled();
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

  it("requires known lockdown and confirmation, guards duplicate clears, and reports accepted-but-still-locked", async () => {
    api.loadKillSwitchStatus
      .mockResolvedValueOnce({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true })
      .mockResolvedValueOnce({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true });
    let resolveClear!: () => void;
    api.clearKillSwitch.mockImplementation(() => new Promise<void>((resolve) => { resolveClear = resolve; }));
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(result.current.killSwitchStatus?.state).toBe("LOCKDOWN"));
    vi.mocked(window.confirm).mockReturnValueOnce(false);
    await act(() => result.current.unlockKillSwitch());
    expect(api.clearKillSwitch).not.toHaveBeenCalled();
    vi.mocked(window.confirm).mockReturnValue(true);
    let first!: Promise<void>;
    act(() => { first = result.current.unlockKillSwitch(); });
    await waitFor(() => expect(result.current.killSwitchPending).toBe(true));
    await act(() => result.current.unlockKillSwitch());
    expect(api.clearKillSwitch).toHaveBeenCalledTimes(1);
    await act(async () => { resolveClear(); await first; });
    expect(result.current.notice).toBe(i18n.t("strategies.killSwitchAcceptedAwaiting"));
    expect(api.loadKillSwitchStatus).toHaveBeenCalledTimes(2);
  });

  it("leaves a failed gate read unavailable without breaking the strategy load", async () => {
    api.loadKillSwitchStatus.mockRejectedValue(new Error("network"));
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.strategies).toHaveLength(1);
    expect(result.current.killSwitchReadError).toBe(true);
    expect(result.current.killSwitchStatus).toBeNull();
  });

  it.each([
    [{ state: "OK", listener_available: true }, true, true],
    [{ state: "LOCKDOWN", listener_available: false }, true, true],
    [{ state: "LOCKDOWN", listener_available: true }, false, true],
    [{ state: "LOCKDOWN", listener_available: true }, true, false]
  ] as const)("keeps direct unlock unavailable for status/listener/session gate %j", async (status, canMutate, canStepUp) => {
    api.loadKillSwitchStatus.mockResolvedValue({ ...status, redis_state: null, durable_state: null });
    api.ensureBrowserSession.mockResolvedValue({ permissions: { can_mutate: canMutate, can_step_up: canStepUp } });
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(result.current.killSwitchStatus?.state).toBe(status.state));
    await act(() => result.current.unlockKillSwitch());
    expect(window.confirm).not.toHaveBeenCalled();
    expect(api.clearKillSwitch).not.toHaveBeenCalled();
  });

  it("reports observed OK only after the follow-up read", async () => {
    api.loadKillSwitchStatus
      .mockResolvedValueOnce({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true })
      .mockResolvedValueOnce({ state: "OK", redis_state: "OK", durable_state: "OK", listener_available: true });
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(result.current.killSwitchStatus?.state).toBe("LOCKDOWN"));
    await act(() => result.current.unlockKillSwitch());
    expect(api.clearKillSwitch).toHaveBeenCalledTimes(1);
    expect(api.loadKillSwitchStatus).toHaveBeenCalledTimes(2);
    expect(result.current.notice).toBe(i18n.t("strategies.killSwitchObserved"));
  });

  it("single-flights manual status refreshes and blocks clear while the read is pending", async () => {
    api.loadKillSwitchStatus.mockResolvedValueOnce({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true });
    const deferredStatus = (() => {
      let resolve!: (value: KillSwitchStatus) => void;
      const promise = new Promise<KillSwitchStatus>((done) => { resolve = done; });
      return { promise, resolve };
    })();
    api.loadKillSwitchStatus.mockReturnValueOnce(deferredStatus.promise);
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(result.current.killSwitchStatus?.state).toBe("LOCKDOWN"));
    let first!: Promise<unknown>;
    let second!: Promise<unknown>;
    act(() => {
      first = result.current.refreshKillSwitch();
      second = result.current.refreshKillSwitch();
    });
    expect(first).toBe(second);
    expect(api.loadKillSwitchStatus).toHaveBeenCalledTimes(2);
    expect(result.current.killSwitchReadPending).toBe(true);
    await act(() => result.current.unlockKillSwitch());
    expect(api.clearKillSwitch).not.toHaveBeenCalled();
    await act(async () => {
      deferredStatus.resolve({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true });
      await first;
    });
    expect(result.current.killSwitchReadPending).toBe(false);
  });

  it.each([
    [new ApiError("redis_publish_failed", 503), "strategies.killSwitchUnknown"],
    [new ApiError("kill_switch_no_listener", 503), "strategies.killSwitchRejected"]
  ] as const)("distinguishes ambiguous clear outcome from definite backend rejection", async (failure, messageKey) => {
    api.loadKillSwitchStatus.mockResolvedValue({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true });
    api.clearKillSwitch.mockRejectedValue(failure);
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(result.current.killSwitchStatus?.state).toBe("LOCKDOWN"));
    await act(() => result.current.unlockKillSwitch());
    expect(result.current.notice).toBe(i18n.t(messageKey));
  });

  it.each([
    [200, { status: "accepted" }, "strategies.killSwitchUnknown"],
    [408, { error: "request_timeout" }, "strategies.killSwitchUnknown"],
    [403, { error: "operator_capability_required" }, "strategies.killSwitchRejected"]
  ] as const)("classifies actual clear API HTTP %i outcomes causally", async (status, body, messageKey) => {
    api.loadKillSwitchStatus.mockResolvedValue({ state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true });
    const actualApi = await vi.importActual<typeof import("../../api")>("../../api");
    api.clearKillSwitch.mockImplementation(actualApi.clearKillSwitch);
    const fetch = vi.fn().mockResolvedValue({
      ok: status >= 200 && status < 300, status, statusText: "",
      json: async () => body
    });
    vi.stubGlobal("fetch", fetch);
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(result.current.killSwitchStatus?.state).toBe("LOCKDOWN"));
    await act(() => result.current.unlockKillSwitch());
    expect(result.current.notice).toBe(i18n.t(messageKey));
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][0]).toBe("/ops/kill-switch/clear");
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
    [new ApiError("unauthorized", 401), false, "command"],
    [new ApiError("forbidden", 403), false, "command"],
    [new ApiError("stale_version", 409), false, "command"],
    [new ApiError("strategy_engine_listener_unavailable", 503), false, "command"],
    [new Error("invalid_response"), true, "unknown"],
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


it("blocks direct submit after an accepted command's snapshot refresh fails", async () => {
  vi.resetAllMocks();
  window.sessionStorage.clear();
  api.ensureBrowserSession.mockResolvedValue({ permissions: { can_mutate: true, can_step_up: true } });
  api.loadStrategyStates.mockResolvedValueOnce([strategy()]).mockRejectedValueOnce(new Error("invalid_response"));
  api.sendStrategyCommand.mockResolvedValue(undefined);
  const confirm = vi.spyOn(window, "confirm").mockReturnValue(true);
  try {
    const { result } = renderHook(() => useStrategyManager(i18n.t));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await act(() => result.current.submit(strategy(), "STOP"));
    await act(() => result.current.submit({ ...strategy(), strategy_id: "other" }, "STOP"));
    expect(result.current.readOnly).toBe(true);
    expect(result.current.awaitingStrategies.has("active-strategy")).toBe(true);
    expect(confirm).toHaveBeenCalledTimes(1);
    expect(api.sendStrategyCommand).toHaveBeenCalledTimes(1);
  } finally {
    cleanup();
    vi.restoreAllMocks();
  }
});
