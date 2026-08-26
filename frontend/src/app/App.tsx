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
import type { Locale } from "../i18n";
import {
  applyTheme,
  initialTheme,
  saveTheme,
  type Theme
} from "../theme";
import {
  parseDemoMode,
  parseNavigation,
  serializeNavigation,
  type View
} from "./navigation";

const StrategyManager = lazy(() =>
  import("../StrategyManager").then((module) => ({
    default: module.StrategyManager
  }))
);
const TradeChartView = lazy(() =>
  import("../TradeChartView").then((module) => ({
    default: module.TradeChartView
  }))
);
const BacktestResultsView = lazy(() =>
  import("../BacktestResultsView").then((module) => ({
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

  const chooseView = (nextView: View, requestedTradeId: string | null = null) => {
    if (nextView === "research") {
      setResearchActivated(true);
    }
    const navigation = serializeNavigation(
      new URL(window.location.href),
      nextView,
      requestedTradeId
    );
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
            <span aria-hidden="true">{theme === "dark" ? "☀" : "☾"}</span>
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
            onInspectTrade={(tradeId) => {
              chooseView("trades", tradeId);
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
