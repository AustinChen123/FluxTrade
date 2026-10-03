import { useMemo } from "react";
import type { useTranslation } from "react-i18next";

import type { Locale } from "../../shared/i18n";
import {
  formatPresentationTimestamp,
  PRESENTATION_TIME_ZONE
} from "../../shared/time/presentation";
import type {
  AwaitingStrategies,
  StrategyCommandName,
  StrategyErrorDetail,
  StrategyManagerError,
  StrategyRecord
} from "./strategyCommandState";
type KillSwitchStatus = {
  state: "LOCKDOWN" | "OK" | "UNKNOWN";
  listener_available: boolean;
};

type Translate = ReturnType<typeof useTranslation>["t"];

export interface StrategyManagerViewProps {
  strategies: StrategyRecord[];
  readOnly?: boolean;
  stepUpRequired?: boolean;
  loading: boolean;
  error: StrategyManagerError | null;
  notice: string;
  pendingStrategyId: string | null;
  awaitingStrategies: AwaitingStrategies;
  locale: Locale;
  t: Translate;
  refresh: () => Promise<void>;
  submit: (
    strategy: StrategyRecord,
    command: StrategyCommandName
  ) => Promise<void>;
  killSwitchStatus?: KillSwitchStatus | null;
  killSwitchReadError?: boolean;
  killSwitchPending?: boolean;
  killSwitchReadPending?: boolean;
  refreshKillSwitch?: () => Promise<unknown>;
  unlockKillSwitch?: () => Promise<void>;
}

function commandLabel(command: StrategyCommandName, t: Translate): string {
  return t(`strategies.command.${command}`);
}

function errorMessage(detail: StrategyErrorDetail, t: Translate): string {
  switch (detail.type) {
    case "unknown":
      return t("strategies.unknownErrorBody");
    case "step_up":
      return t("strategies.stepUpRequired");
    case "unauthorized":
      return t("strategies.unauthorized");
    case "service":
      return t("strategies.serviceError", { status: detail.status });
    case "generic":
      return t("strategies.errorBody");
  }
}

export function StrategyManagerView({
  strategies,
  readOnly = false,
  stepUpRequired = false,
  loading,
  error,
  notice,
  pendingStrategyId,
  awaitingStrategies,
  locale,
  t,
  refresh,
  submit
  ,killSwitchStatus = null, killSwitchReadError = false, killSwitchPending = false, killSwitchReadPending = false,
  refreshKillSwitch = async () => undefined, unlockKillSwitch = async () => undefined
}: StrategyManagerViewProps) {
  const counts = useMemo(() => {
    const totals: Partial<Record<StrategyRecord["status"], number>> = {};
    for (const strategy of strategies) {
      totals[strategy.status] = (totals[strategy.status] ?? 0) + 1;
    }
    return totals;
  }, [strategies]);
  const dateFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        dateStyle: "medium",
        timeStyle: "medium",
        timeZone: PRESENTATION_TIME_ZONE
      }),
    [locale]
  );

  return (
    <section
      className="strategy-console"
      aria-labelledby="strategy-title"
      aria-busy={loading || pendingStrategyId !== null}
    >
      <div className="strategy-intro">
        <div>
          <p className="panel-kicker">{t("strategies.kicker")}</p>
          <h2 id="strategy-title">{t("strategies.title")}</h2>
          <p>{t("strategies.body")}</p>
        </div>
        <button
          type="button"
          onClick={() => void refresh()}
          disabled={loading || pendingStrategyId !== null}
        >
          {t("strategies.refresh")}
        </button>
      </div>

      {error !== null && (
        <div className="error-panel strategy-feedback" role="alert">
          <div>
            <strong>{t(`strategies.${error.kind}ErrorTitle`)}</strong>
            <p>{errorMessage(error.detail, t)}</p>
          </div>
        </div>
      )}
      {!loading && error === null && (readOnly || stepUpRequired) && (
        <p role="status">{t(readOnly ? "strategies.readOnly" : "strategies.stepUpRequired")}</p>
      )}
      {notice && (
        <p className="strategy-notice" role="status">
          {notice}
        </p>
      )}

      <section className="strategy-safety" aria-labelledby="strategy-safety-title">
        <div>
          <h3 id="strategy-safety-title">
            <svg aria-hidden="true" viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M12 3 3 7v5c0 5 9 9 9 9s9-4 9-9V7Z" />
              <path d="M12 8v5m0 3v1" />
            </svg>
            {t("strategies.killSwitchTitle")}
          </h3>
          <p>{t("strategies.killSwitchStatus", { state: killSwitchReadError ? t("strategies.killSwitchUnavailable") : (killSwitchStatus?.state ?? t("strategies.killSwitchLoading")) })}</p>
          <p id="strategy-recovery-unavailable">{t(killSwitchPending || killSwitchReadPending || pendingStrategyId !== null ? "strategies.killSwitchPending" : killSwitchReadError ? "strategies.killSwitchReadError" : loading || killSwitchStatus === null ? "strategies.killSwitchLoadingReason" : killSwitchStatus.state === "UNKNOWN" ? "strategies.killSwitchUnknownStatus" : killSwitchStatus.state === "OK" ? "strategies.killSwitchGateOnly" : !killSwitchStatus.listener_available ? "strategies.killSwitchNoListener" : readOnly || stepUpRequired ? "strategies.killSwitchPermission" : "strategies.killSwitchRecoveryWarning")}</p>
        </div>
        <button type="button" onClick={() => void refreshKillSwitch()} disabled={loading || killSwitchPending || killSwitchReadPending}>{t("strategies.killSwitchRefresh")}</button>
        <button type="button" onClick={() => void unlockKillSwitch()} disabled={
          loading || killSwitchPending || killSwitchReadPending || pendingStrategyId !== null || killSwitchReadError ||
          killSwitchStatus?.state !== "LOCKDOWN" || !killSwitchStatus.listener_available || readOnly || stepUpRequired
        } aria-describedby="strategy-recovery-unavailable">
          {t("strategies.unlockLockdown")}
        </button>
      </section>
      {strategies.length > 0 && (loading || error?.kind === "load" || error?.kind === "refresh") && (
        <p className="strategy-control-note">{t("strategies.lastKnownState")}</p>
      )}

      {(strategies.length > 0 || (!loading && error === null)) && (
        <>
          <dl className="strategy-summary" aria-label={t("strategies.summary")}>
            <div>
              <dt>{t("strategies.total")}</dt>
              <dd>{strategies.length.toLocaleString(locale)}</dd>
            </div>
            <div>
              <dt>{t("strategies.status.ACTIVE")}</dt>
              <dd>{(counts.ACTIVE ?? 0).toLocaleString(locale)}</dd>
            </div>
            <div>
              <dt>{t("strategies.status.WARNING")}</dt>
              <dd>{(counts.WARNING ?? 0).toLocaleString(locale)}</dd>
            </div>
            <div>
              <dt>{t("strategies.status.ERROR")}</dt>
              <dd>{(counts.ERROR ?? 0).toLocaleString(locale)}</dd>
            </div>
          </dl>

          {strategies.length === 0 ? (
            <div className="empty-panel">
              <strong>{t("strategies.emptyTitle")}</strong>
              <p>{t("strategies.emptyBody")}</p>
            </div>
          ) : (
            <ul className="strategy-list" aria-label={t("strategies.list")}>
              {strategies.map((strategy) => {
                const pending = pendingStrategyId === strategy.strategy_id;
                const awaitingState = awaitingStrategies.has(
                  strategy.strategy_id
                );
                return (
                  <li key={strategy.strategy_id}>
                    <div className="strategy-card-header">
                      <div className="strategy-identity">
                        <span
                          className={`strategy-status status-${strategy.status.toLowerCase()}`}
                          aria-hidden="true"
                        />
                        <div>
                          <strong>{strategy.strategy_id}</strong>
                          <span>{t(`strategies.status.${strategy.status}`)}</span>
                        </div>
                      </div>
                      <div className="strategy-action">
                        {strategy.last_error_message && (
                          <p title={strategy.last_error_message}>
                            {strategy.last_error_message}
                          </p>
                        )}
                        {strategy.available_commands.map((command) => (
                          <button
                            key={command}
                            type="button"
                            className={command === "STOP" ? "danger-action" : ""}
                            disabled={pendingStrategyId !== null || awaitingState || killSwitchPending}
                            onClick={() => void submit(strategy, command)}
                          >
                            {pending
                              ? t("strategies.pending")
                              : awaitingState
                                ? t("strategies.awaitingState")
                                : commandLabel(command, t)}
                          </button>
                        ))}
                      </div>
                    </div>
                    <p className="strategy-control-note">{t("strategies.stopExplanation")}</p>
                    {(pendingStrategyId !== null || awaitingState) && (
                      <p className="strategy-control-note">
                        {t(pendingStrategyId !== null ? "strategies.pendingExplanation" : "strategies.awaitingExplanation")}
                      </p>
                    )}
                    <dl>
                      <div>
                        <dt>{t("strategies.heartbeat")}</dt>
                        <dd>
                          {formatPresentationTimestamp(
                            strategy.last_heartbeat,
                            dateFormatter
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>{t("strategies.uptime")}</dt>
                        <dd>
                          {formatPresentationTimestamp(
                            strategy.uptime_start,
                            dateFormatter
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>{t("strategies.version")}</dt>
                        <dd>{strategy.version.toLocaleString(locale)}</dd>
                      </div>
                    </dl>

                  </li>
                );
              })}
            </ul>
          )}
        </>
      )}

      {loading && (
        <div className="loading-indicator" aria-live="polite">
          <span />
          {t("strategies.loading")}
        </div>
      )}
    </section>
  );
}
