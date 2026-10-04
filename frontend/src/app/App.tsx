import {
  lazy,
  Suspense,
  useEffect,
  useState
} from "react";
import { useTranslation } from "react-i18next";

import {
  ResearchRoute,
  type ResearchSlots
} from "../features/research/ResearchRoute";
import type { Locale } from "../shared/i18n";
import {
  applyTheme,
  initialTheme,
  saveTheme,
  type Theme
} from "../shared/theme";
import {
  parseDemoMode,
  parseNavigation,
  serializeNavigation,
  type View
} from "./navigation";

const StrategyManager = lazy(() =>
  import("../features/strategies/StrategyManager").then((module) => ({
    default: module.StrategyManager
  }))
);
const TradeChartView = lazy(() =>
  import("../features/trades/TradeChartView").then((module) => ({
    default: module.TradeChartView
  }))
);
const BacktestResultsView = lazy(() =>
  import("../features/results/BacktestResultsView").then((module) => ({
    default: module.BacktestResultsView
  }))
);

export function App() {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const demoMode = parseDemoMode(
    new URL(window.location.href),
    import.meta.env.DEV
  );
  const [initialNavigation] = useState(() =>
    parseNavigation(window.location.search)
  );
  const [view, setView] = useState<View>(initialNavigation.view);
  const [selectedResultId, setSelectedResultId] = useState<string | null>(
    initialNavigation.selectedResultId
  );
  const [researchActivated, setResearchActivated] = useState(
    view === "research"
  );
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [inspectedTradeId, setInspectedTradeId] = useState<string | null>(
    initialNavigation.inspectedTradeId
  );

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  const chooseView = (
    nextView: View,
    requestedTradeId: string | null = null,
    requestedResultId: string | null = selectedResultId
  ) => {
    if (nextView === "research") {
      setResearchActivated(true);
    }
    const navigation = serializeNavigation(
      new URL(window.location.href),
      nextView,
      requestedTradeId,
      requestedResultId
    );
    setSelectedResultId(navigation.selectedResultId);
    setInspectedTradeId(navigation.inspectedTradeId);
    window.history.replaceState(null, "", navigation.relativeUrl);
    setView(nextView);
  };

  const renderShell = ({ toolbar, content }: ResearchSlots) => (
    <main className="console-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t("app.eyebrow")}</p>
          <h1>
            {t(
              view === "research"
                ? "app.title"
                : view === "results"
                  ? "results.title"
                  : view === "strategies"
                    ? "strategies.title"
                    : "trades.title"
            )}
          </h1>
        </div>
        <div className="toolbar">
          {toolbar}
          <label className="language-control" htmlFor="language">
            {t("controls.language")}
            <select
              id="language"
              value={locale}
              onChange={(event) =>
                void i18n.changeLanguage(event.target.value as Locale)
              }
            >
              <option value="zh-TW">繁體中文</option>
              <option value="en">English</option>
            </select>
          </label>
          <button
            type="button"
            className="theme-control"
            aria-label={t(
              theme === "dark" ? "controls.light" : "controls.dark"
            )}
            title={t(theme === "dark" ? "controls.light" : "controls.dark")}
            onClick={() => {
              const next = theme === "dark" ? "light" : "dark";
              applyTheme(next);
              saveTheme(next);
              setTheme(next);
            }}
          >
            <svg className="theme-control-icon" aria-hidden="true" viewBox="0 0 24 24" fill="none">
              {theme === "dark" ? (
                <><circle cx="12" cy="12" r="3.5" /><path d="M12 2v2m0 16v2M4.93 4.93l1.42 1.42m11.3 11.3 1.42 1.42M2 12h2m16 0h2M4.93 19.07l1.42-1.42m11.3-11.3 1.42-1.42" /></>
              ) : <path d="M20.2 15.5A8.5 8.5 0 0 1 8.5 3.8 8.6 8.6 0 1 0 20.2 15.5Z" />}
            </svg>
            <small>{t("controls.theme")}</small>
          </button>
        </div>
      </header>

      <nav className="console-nav" aria-label={t("navigation.aria")}>
        <button
          type="button"
          aria-current={view === "research" ? "page" : undefined}
          onClick={() => chooseView("research")}
        >
          {t("navigation.research")}
        </button>
        <button
          type="button"
          aria-current={view === "results" ? "page" : undefined}
          onClick={() => chooseView("results")}
        >
          {t("navigation.results")}
        </button>
        <button
          type="button"
          aria-current={view === "strategies" ? "page" : undefined}
          onClick={() => chooseView("strategies")}
        >
          {t("navigation.strategies")}
        </button>
        <button
          type="button"
          aria-current={view === "trades" ? "page" : undefined}
          onClick={() => chooseView("trades")}
        >
          {t("navigation.trades")}
        </button>
      </nav>

      {view === "strategies" && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              {t("strategies.loading")}
            </div>
          }
        >
          <StrategyManager />
        </Suspense>
      )}

      {(view === "research" || view === "results" || view === "trades") &&
        demoMode && <p className="demo-notice">{t("demo")}</p>}

      {view === "results" && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              {t("results.loading")}
            </div>
          }
        >
          <BacktestResultsView
            demoMode={demoMode}
            theme={theme}
            onInspectTrade={(tradeId, resultId) => {
              chooseView("trades", tradeId, resultId);
            }}
          />
        </Suspense>
      )}

      {view === "trades" && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              {t("trades.loading")}
            </div>
          }
        >
          <TradeChartView
            demoMode={demoMode}
            theme={theme}
            initialTradeId={inspectedTradeId}
            onSelectTrade={(tradeId) => chooseView("trades", tradeId)}
          />
        </Suspense>
      )}

      {content}
    </main>
  );

  return researchActivated ? (
    <ResearchRoute
      visible={view === "research"}
      demoMode={demoMode}
      theme={theme}
    >
      {renderShell}
    </ResearchRoute>
  ) : (
    renderShell({ toolbar: null, content: null })
  );
}
