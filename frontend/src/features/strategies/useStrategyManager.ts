import { useCallback, useEffect, useState } from "react";
import type { useTranslation } from "react-i18next";

import {
  ApiError,
  ensureBrowserSession,
  loadStrategyStates,
  sendStrategyCommand,
  type BrowserSession,
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
    setError(null);
    try {
      const browserSession = await ensureBrowserSession();
      setSession(browserSession);
      applyStrategyStates(await loadStrategyStates());
    } catch (reason) {
      setError(classifyError(reason, "load"));
    } finally {
      setLoading(false);
    }
  }, [applyStrategyStates]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    saveAwaitingStrategies(awaitingStrategies);
  }, [awaitingStrategies]);

  const submit = useCallback(
    async (strategy: StrategyState, command: StrategyCommand) => {
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
    [applyStrategyStates, awaitingStrategies, session?.csrf_token, t]
  );

  return {
    strategies,
    loading,
    error,
    notice,
    pendingStrategyId,
    awaitingStrategies,
    refresh,
    submit
  };
}
