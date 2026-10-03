import { useCallback, useEffect, useRef, useState } from "react";
import type { useTranslation } from "react-i18next";

import {
  ApiError,
  clearKillSwitch,
  ensureBrowserSession,
  loadKillSwitchStatus,
  loadStrategyStates,
  sendStrategyCommand,
  type BrowserSession,
  type KillSwitchStatus,
  type StrategyCommand,
  type StrategyState
} from "../../api";
import {
  AWAITING_STORAGE_KEY,
  isDefiniteCommandRejection,
  parseAwaitingStrategies,
  serializeAwaitingStrategies,
  transitionStrategyCommandState,
  type AwaitingStrategies,
  type StrategyErrorKind,
  type StrategyManagerError
} from "./strategyCommandState";

type Translate = ReturnType<typeof useTranslation>["t"];

function classifyError(
  reason: unknown,
  kind: StrategyErrorKind
): StrategyManagerError {
  if (kind === "unknown") {
    return { kind, detail: { type: "unknown" } };
  }
  if (reason instanceof ApiError) {
    if (reason.message === "step_up_required") {
      return { kind, detail: { type: "step_up" } };
    }
    if (reason.status === 401 || reason.status === 403) {
      return { kind, detail: { type: "unauthorized" } };
    }
    return { kind, detail: { type: "service", status: reason.status } };
  }
  return { kind, detail: { type: "generic" } };
}

function loadAwaitingStrategies(): AwaitingStrategies {
  try {
    return parseAwaitingStrategies(
      window.sessionStorage.getItem(AWAITING_STORAGE_KEY)
    );
  } catch {
    return new Map();
  }
}

function saveAwaitingStrategies(strategies: AwaitingStrategies): void {
  try {
    const serialized = serializeAwaitingStrategies(strategies);
    if (serialized === null) {
      window.sessionStorage.removeItem(AWAITING_STORAGE_KEY);
    } else {
      window.sessionStorage.setItem(AWAITING_STORAGE_KEY, serialized);
    }
  } catch {
    // Browser storage is optional; the in-memory lock remains fail-closed.
  }
}

function permittedCommands(strategy: StrategyState, session: BrowserSession | null) {
  if (session?.permissions?.can_mutate !== true) return [];
  return strategy.available_commands.filter((command) =>
    command === "STOP" ||
    (["START", "RESUME", "FORCE_RECOVER"].includes(command) &&
      session.permissions.can_step_up === true)
  );
}

function commandLabel(command: StrategyCommand, t: Translate): string {
  return t(`strategies.command.${command}`);
}

export function useStrategyManager(t: Translate) {
  const [session, setSession] = useState<BrowserSession | null>(null);
  const [strategies, setStrategies] = useState<StrategyState[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<StrategyManagerError | null>(null);
  const [notice, setNotice] = useState("");
  const [pendingStrategyId, setPendingStrategyId] = useState<string | null>(null);
  const [awaitingStrategies, setAwaitingStrategies] =
    useState<AwaitingStrategies>(loadAwaitingStrategies);
  const [killSwitchStatus, setKillSwitchStatus] = useState<KillSwitchStatus | null>(null);
  const [killSwitchReadError, setKillSwitchReadError] = useState(false);
  const [killSwitchPending, setKillSwitchPending] = useState(false);
  const [killSwitchReadPending, setKillSwitchReadPending] = useState(false);
  const killSwitchPendingRef = useRef(false);
  const killSwitchReadPendingRef = useRef(false);
  const killSwitchReadPromiseRef = useRef<Promise<KillSwitchStatus | null> | null>(null);

  const readKillSwitchStatus = useCallback(() => {
    if (killSwitchReadPromiseRef.current !== null) {
      return killSwitchReadPromiseRef.current;
    }
    killSwitchReadPendingRef.current = true;
    setKillSwitchReadPending(true);
    const request = (async (): Promise<KillSwitchStatus | null> => {
      try {
        const status = await loadKillSwitchStatus();
        setKillSwitchStatus(status);
        setKillSwitchReadError(false);
        return status;
      } catch {
        setKillSwitchStatus(null);
        setKillSwitchReadError(true);
        return null;
      } finally {
        killSwitchReadPendingRef.current = false;
        killSwitchReadPromiseRef.current = null;
        setKillSwitchReadPending(false);
      }
    })();
    killSwitchReadPromiseRef.current = request;
    return request;
  }, []);

  const refreshKillSwitch = useCallback(() => {
    if (killSwitchPendingRef.current) return Promise.resolve(null);
    return readKillSwitchStatus();
  }, [readKillSwitchStatus]);

  const applyStrategyStates = useCallback((items: StrategyState[]) => {
    setStrategies(items);
    setAwaitingStrategies((current) =>
      transitionStrategyCommandState(current, {
        type: "authoritative_snapshot",
        strategies: items
      })
    );
  }, []);

  const refresh = useCallback(async () => {
    setLoading(true);
    setSession(null);
    setError(null);
    try {
      const browserSession = await ensureBrowserSession();
      setKillSwitchStatus(null);
      await refreshKillSwitch();
      const items = await loadStrategyStates();
      applyStrategyStates(items);
      setSession(browserSession);
    } catch (reason) {
      setError(classifyError(reason, "load"));
    } finally {
      setLoading(false);
    }
  }, [applyStrategyStates, refreshKillSwitch]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    saveAwaitingStrategies(awaitingStrategies);
  }, [awaitingStrategies]);

  const submit = useCallback(
    async (strategy: StrategyState, command: StrategyCommand) => {
      if (killSwitchPendingRef.current || !permittedCommands(strategy, session).includes(command)) return;
      if (
        !window.confirm(
          t("strategies.confirm", {
            command: commandLabel(command, t),
            strategyId: strategy.strategy_id
          })
        )
      ) {
        return;
      }
      setPendingStrategyId(strategy.strategy_id);
      setError(null);
      setNotice("");
      let idempotencyKey: string;
      try {
        idempotencyKey = window.crypto.randomUUID();
      } catch (reason) {
        setError(classifyError(reason, "command"));
        setPendingStrategyId(null);
        return;
      }
      const awaiting = transitionStrategyCommandState(awaitingStrategies, {
        type: "command_started",
        strategy
      });
      setAwaitingStrategies(awaiting);
      saveAwaitingStrategies(awaiting);
      try {
        await sendStrategyCommand(
          strategy.strategy_id,
          command,
          strategy.version,
          idempotencyKey,
          session?.csrf_token
        );
        setNotice(
          t("strategies.accepted", {
            command: commandLabel(command, t),
            strategyId: strategy.strategy_id
          })
        );
        try {
          const updatedStrategies = await loadStrategyStates();
          applyStrategyStates(updatedStrategies);
        } catch (reason) {
          setSession(null);
          setError(classifyError(reason, "refresh"));
        }
      } catch (reason) {
        const commandFailure =
          reason instanceof ApiError
            ? {
                type: "api" as const,
                status: reason.status,
                message: reason.message
              }
            : { type: "unknown" as const };
        const definiteRejection =
          isDefiniteCommandRejection(commandFailure);
        if (definiteRejection) {
          const unlocked = transitionStrategyCommandState(awaiting, {
            type: "definite_rejection",
            strategyId: strategy.strategy_id
          });
          setAwaitingStrategies(unlocked);
          saveAwaitingStrategies(unlocked);
        }
        setError(
          classifyError(reason, definiteRejection ? "command" : "unknown")
        );
      } finally {
        setPendingStrategyId(null);
      }
    },
    [applyStrategyStates, awaitingStrategies, session, t]
  );

  const unlockKillSwitch = useCallback(async () => {
    if (killSwitchPendingRef.current || killSwitchReadPendingRef.current || loading || pendingStrategyId !== null ||
        killSwitchStatus?.state !== "LOCKDOWN" || !killSwitchStatus.listener_available ||
        session?.permissions?.can_mutate !== true || session.permissions.can_step_up !== true) return;
    const confirmed = window.confirm(t("strategies.killSwitchConfirm"));
    if (!confirmed) return;
    killSwitchPendingRef.current = true;
    setKillSwitchPending(true);
    setNotice("");
    try {
      await clearKillSwitch(session.csrf_token);
      setNotice(t("strategies.killSwitchAccepted"));
      const observed = await readKillSwitchStatus();
      setNotice(observed?.state === "OK"
        ? t("strategies.killSwitchObserved")
        : t("strategies.killSwitchAcceptedAwaiting"));
    } catch (reason) {
      const definite = reason instanceof ApiError &&
        ([400, 401, 403, 404, 405, 409, 422].includes(reason.status) ||
          (reason.status === 503 && ["kill_switch_no_listener", "redis_unavailable"].includes(reason.message)));
      setNotice(definite ? t("strategies.killSwitchRejected") : t("strategies.killSwitchUnknown"));
    } finally {
      killSwitchPendingRef.current = false;
      setKillSwitchPending(false);
    }
  }, [killSwitchStatus, loading, pendingStrategyId, readKillSwitchStatus, session, t]);

  return {
    strategies: strategies.map((strategy) => ({
      ...strategy, available_commands: permittedCommands(strategy, session)
    })),
    readOnly: session?.permissions?.can_mutate !== true,
    stepUpRequired: session?.permissions?.can_mutate === true &&
      session?.permissions?.can_step_up !== true,
    loading,
    error,
    notice,
    pendingStrategyId,
    awaitingStrategies,
    refresh,
    submit,
    killSwitchStatus,
    killSwitchReadError,
    killSwitchPending,
    killSwitchReadPending,
    refreshKillSwitch,
    unlockKillSwitch
  };
}
