import i18n from "i18next";
import { initReactI18next } from "react-i18next";

export type Locale = "zh-TW" | "en";

const STORAGE_KEY = "fluxtrade.locale";

const resources = {
  "zh-TW": {
    translation: {
      app: {
        eyebrow: "FluxTrade／研究控制台",
        title: "參數地形"
      },
      controls: {
        epoch: "演化批次",
        language: "語言",
        theme: "主題",
        light: "切換至日間主題",
        dark: "切換至夜間主題"
      },
      navigation: {
        aria: "控制台頁面",
        research: "參數研究",
        results: "回測績效",
        strategies: "策略管理",
        trades: "進出場檢視"
      },
      results: {
        kicker: "回測稽核",
        title: "回測績效",
        body: "沿著權益與風險路徑核對單次回測，並下鑽至每一筆已平倉交易。",
        unavailableTitle: "尚未連接正式回測結果",
        unavailableBody: "目前控制面尚未提供 browser-safe results contract。正式模式不會讀取 result JSON、本機報告路徑或展示資料。",
        loadErrorTitle: "回測結果未載入",
        loadErrorBody: "確認結果服務與工作階段後再試一次。",
        loading: "載入回測績效",
        strategy: "策略",
        instrument: "商品",
        timeframe: "週期",
        period: "回測期間",
        metricsAria: "回測績效摘要",
        netPnl: "淨 PnL",
        maxDrawdown: "最大回撤",
        sharpe: "Sharpe",
        sortino: "Sortino",
        calmar: "Calmar",
        tradeCount: "已平倉交易",
        equity: "權益",
        drawdown: "回撤",
        pathKicker: "風險路徑",
        pathTitle: "權益與回撤",
        pathHint: "同一時間軸 · 回撤向下",
        ariaChart: "回測權益曲線與同步回撤圖",
        noEquity: "這次結果沒有可顯示的權益與回撤樣本。",
        invalidEquity: "權益或回撤樣本格式無效，已停止繪製圖表。",
        monthlyKicker: "週期回報",
        monthlyTitle: "月報酬",
        noMonthly: "這次結果沒有月報酬資料。",
        distributionKicker: "交易形狀",
        distributionTitle: "PnL 分布",
        distributionUnit: "每筆淨 PnL · {{currency}}",
        noDistribution: "這次結果沒有交易分布資料。",
        invalidDistribution: "交易分布資料格式無效，已停止繪製分布。",
        tradesKicker: "稽核清冊",
        tradesTitle: "交易明細",
        tradeId: "交易 ID",
        action: "操作",
        inspect: "查看 K 線",
        noTrades: "這次回測沒有已平倉交易。",
        invalidTradePage: "交易頁資料不完整，已停止顯示清冊。",
        tradeProgress: "已載入 {{loaded}}／{{total}} 筆",
        loadMoreTrades: "載入更多交易",
        loadingMoreTrades: "載入交易中",
        retryTrades: "重試載入交易",
        paginationUnavailable: "尚未連接後續交易頁面。"
      },
      strategies: {
        kicker: "運行控制",
        title: "策略管理",
        body: "查看權威運行狀態，並對單一策略送出受控生命週期命令。",
        refresh: "重新整理狀態",
        loading: "讀取策略狀態",
        loadErrorTitle: "策略狀態未載入",
        commandErrorTitle: "策略命令未送出",
        refreshErrorTitle: "命令已接受，但狀態尚未更新",
        unknownErrorTitle: "策略命令結果不明",
        errorBody: "控制面目前無法完成要求。確認工作階段與服務狀態後再試一次。",
        unknownErrorBody: "控制面可能已接受命令；在權威狀態更新前不會重新開放操作。",
        unauthorized: "目前工作階段沒有管理策略的權限。請從受信任的 Tailscale 入口重新開啟。",
        stepUpRequired: "這項命令需要短效升權。請取得 step-up 權限後重新建立工作階段。",
        serviceError: "策略控制服務回覆 {{status}}。請確認引擎 listener 與控制面狀態。",
        summary: "策略狀態摘要",
        total: "策略總數",
        list: "策略運行清冊",
        heartbeat: "最近 heartbeat",
        uptime: "啟動時間",
        version: "狀態版本",
        emptyTitle: "尚無策略狀態",
        emptyBody: "策略引擎註冊第一個實例後，運行狀態會出現在這裡。",
        pending: "送出中",
        awaitingState: "等待狀態更新",
        confirm: "確定要對 {{strategyId}} 執行「{{command}}」？",
        accepted: "已接受 {{strategyId}} 的「{{command}}」命令。",
        status: {
          DISCOVERED: "已發現",
          READY: "準備就緒",
          WARNING: "警告",
          ACTIVE: "運行中",
          STOPPED: "已停止",
          ERROR: "錯誤"
        },
        command: {
          START: "啟動",
          STOP: "停止",
          RESUME: "恢復",
          FORCE_RECOVER: "強制恢復"
        }
      },
      trades: {
        kicker: "交易上下文",
        title: "K 線與進出場",
        body: "在同一段價格走勢中核對策略的實際進場、出場與交易結果。",
        unavailableTitle: "尚未連接正式 K 線資料",
        unavailableBody: "目前控制面尚未提供 browser-safe candles 與 trades contract。請使用開發展示模式驗證介面，正式模式不會顯示假資料。",
        emptyTitle: "這段結果沒有可顯示的 K 線",
        emptyBody: "確認結果綁定的資料集與時間範圍後再試一次。",
        dataQuality: "已略過 {{candles}} 根無效 K 線與 {{markers}} 個無法精確對齊的交易標記。",
        strategy: "策略",
        instrument: "商品",
        timeframe: "週期",
        tradeCount: "交易數",
        chartKicker: "價格路徑",
        chartTitle: "進出場時間軸",
        chartHint: "拖曳或滾輪縮放；點擊標記查看交易",
        ledgerKicker: "交易清冊",
        ledgerTitle: "已平倉交易",
        noTrades: "這段 K 線沒有已平倉交易。",
        selected: "已選交易",
        selectPrompt: "點擊圖表標記或交易列以查看明細。",
        side: "方向",
        quantity: "數量",
        entry: "進場",
        exit: "出場",
        fee: "費用",
        pnl: "PnL",
        price: "價格",
        longEntry: "L 進",
        longExit: "L 出",
        shortEntry: "S 進",
        shortExit: "S 出",
        loading: "載入 K 線圖表",
        ariaChart: "顯示策略進出場標記的互動式 K 線圖"
      },
      demo: "本機展示資料 · 正式建置不包含此模式",
      error: {
        title: "研究資料未載入",
        unauthorized: "目前工作階段沒有讀取研究結果的權限。請從受信任的 Tailscale 入口重新開啟。",
        service: "資料服務回覆 {{status}}：{{message}}",
        fallback: "無法讀取 GA 資料",
        unexpected: "無法讀取 GA 資料：{{message}}",
        retry: "重新讀取"
      },
      empty: {
        title: "尚無演化批次",
        body: "完成一次參數搜尋後，候選地形會出現在這裡。"
      },
      summary: {
        aria: "演化批次摘要",
        objective: "目標",
        generation: "世代",
        population: "族群",
        bestScore: "最佳評分"
      },
      evolution: {
        kicker: "演化軌跡",
        title: "收斂軌跡",
        generation: "顯示世代"
      },
      surface: {
        kicker: "適應度地形",
        title: "候選地形",
        view: "地形顯示模式",
        view2d: "2D 色階",
        view3d: "3D 地形",
        interpolated: "插值曲面 · 拖曳旋轉、滾輪縮放；游標停留／點擊或方向鍵＋Enter 可選擇實際觀測候選",
        unavailable: "這個批次需要至少兩個數值參數才能繪製候選地形。",
        unsupportedObjective: "無法辨識這個批次的最佳化目標，已停止繪製候選地形。"
      },
      candidates: {
        kicker: "候選清冊",
        title: "候選比較",
        count: "{{formattedCount}} 筆",
        drawdown: "回撤",
        selected: "已選候選",
        metrics: "評估指標"
      },
      parallel: {
        kicker: "多軸指紋",
        title: "參數指紋",
        axes: "最多顯示 12 軸",
        unavailable: "此世代沒有可繪製的參數。"
      },
      loading: {
        charts: "載入圖表引擎",
        snapshot: "讀取研究快照"
      },
      objective: {
        minimizeDrawdown: "最小回撤",
        maximizeReturn: "最大報酬",
        maximizeScore: "最大評分",
        unsupported: "不支援的最佳化目標"
      },
      chart: {
        generation: "世代",
        drawdown: "回撤",
        score: "評分",
        fitness: "適應度",
        lowerDrawdown: "最低回撤",
        upperDrawdown: "最高回撤",
        lowerScore: "最低評分",
        upperScore: "最高評分",
        high: "高",
        low: "低",
        selected: "已選候選",
        observed: "觀測候選",
        bestObserved: "每個 X/Y 座標的最佳觀測值"
      },
      aria: {
        convergence: "各世代評分或回撤的上下界收斂圖",
        surface: "兩個策略參數與候選適應度的觀測地形圖",
        surface3d: "兩個策略參數與候選適應度的互動式三維插值地形圖",
        parallel: "候選基因的多參數平行座標圖"
      }
    }
  },
  en: {
    translation: {
      app: {
        eyebrow: "FluxTrade / research console",
        title: "Parameter terrain"
      },
      controls: {
        epoch: "Evolution run",
        language: "Language",
        theme: "Theme",
        light: "Switch to light theme",
        dark: "Switch to dark theme"
      },
      navigation: {
        aria: "Console pages",
        research: "Parameter research",
        results: "Backtest performance",
        strategies: "Strategy management",
        trades: "Trade inspection"
      },
      results: {
        kicker: "Backtest audit",
        title: "Backtest performance",
        body: "Audit one run along its equity and risk path, then drill into every closed trade.",
        unavailableTitle: "Production backtest results are not connected",
        unavailableBody: "The control plane does not yet expose a browser-safe results contract. Production never reads result JSON, local report paths, or demo data.",
        loadErrorTitle: "Backtest results not loaded",
        loadErrorBody: "Check the results service and session, then try again.",
        loading: "Loading backtest performance",
        strategy: "Strategy",
        instrument: "Instrument",
        timeframe: "Timeframe",
        period: "Backtest period",
        metricsAria: "Backtest performance summary",
        netPnl: "Net PnL",
        maxDrawdown: "Max drawdown",
        sharpe: "Sharpe",
        sortino: "Sortino",
        calmar: "Calmar",
        tradeCount: "Closed trades",
        equity: "Equity",
        drawdown: "Drawdown",
        pathKicker: "Risk path",
        pathTitle: "Equity and drawdown",
        pathHint: "Shared timeline · drawdown points down",
        ariaChart: "Backtest equity curve with synchronized drawdown",
        noEquity: "This result has no chartable equity or drawdown samples.",
        invalidEquity: "Equity or drawdown samples are invalid, so the chart was not rendered.",
        monthlyKicker: "Periodic return",
        monthlyTitle: "Monthly returns",
        noMonthly: "This result has no monthly return data.",
        distributionKicker: "Trade shape",
        distributionTitle: "PnL distribution",
        distributionUnit: "Net PnL per trade · {{currency}}",
        noDistribution: "This result has no trade distribution data.",
        invalidDistribution: "Trade distribution data is invalid, so the distribution was not rendered.",
        tradesKicker: "Audit ledger",
        tradesTitle: "Trade details",
        tradeId: "Trade ID",
        action: "Action",
        inspect: "Inspect candles",
        noTrades: "This backtest has no closed trades.",
        invalidTradePage: "The trade page is incomplete, so the ledger was not rendered.",
        tradeProgress: "{{loaded}} of {{total}} trades loaded",
        loadMoreTrades: "Load more trades",
        loadingMoreTrades: "Loading trades",
        retryTrades: "Retry loading trades",
        paginationUnavailable: "Additional trade pages are not connected."
      },
      strategies: {
        kicker: "Runtime control",
        title: "Strategy management",
        body: "Inspect authoritative runtime state and send a controlled lifecycle command to one strategy.",
        refresh: "Refresh state",
        loading: "Loading strategy state",
        loadErrorTitle: "Strategy state not loaded",
        commandErrorTitle: "Strategy command not sent",
        refreshErrorTitle: "Command accepted, but state not refreshed",
        unknownErrorTitle: "Strategy command outcome unknown",
        errorBody: "The control plane could not complete the request. Check the session and service state, then try again.",
        unknownErrorBody: "The control plane may have accepted the command. Actions stay locked until authoritative state changes.",
        unauthorized: "This session cannot manage strategies. Reopen the console through the trusted Tailscale entry point.",
        stepUpRequired: "This command requires short-lived step-up access. Obtain step-up access and create a new session.",
        serviceError: "Strategy control returned {{status}}. Check the engine listener and control-plane state.",
        summary: "Strategy status summary",
        total: "Total strategies",
        list: "Strategy runtime roster",
        heartbeat: "Last heartbeat",
        uptime: "Started",
        version: "State version",
        emptyTitle: "No strategy state yet",
        emptyBody: "Runtime state will appear after the strategy engine registers its first instance.",
        pending: "Sending",
        awaitingState: "Awaiting state update",
        confirm: "Run “{{command}}” for {{strategyId}}?",
        accepted: "Accepted “{{command}}” for {{strategyId}}.",
        status: {
          DISCOVERED: "Discovered",
          READY: "Ready",
          WARNING: "Warning",
          ACTIVE: "Active",
          STOPPED: "Stopped",
          ERROR: "Error"
        },
        command: {
          START: "Start",
          STOP: "Stop",
          RESUME: "Resume",
          FORCE_RECOVER: "Force recover"
        }
      },
      trades: {
        kicker: "Trade context",
        title: "Candles and executions",
        body: "Inspect strategy entries, exits, and outcomes against the same price path.",
        unavailableTitle: "Production candle data is not connected",
        unavailableBody: "The control plane does not yet expose browser-safe candles and trades. Use development demo mode to verify the interface; production never displays fabricated data.",
        emptyTitle: "No chartable candles in this result",
        emptyBody: "Check the result's dataset and time range, then try again.",
        dataQuality: "Skipped {{candles}} invalid candles and {{markers}} trade markers without an exact candle match.",
        strategy: "Strategy",
        instrument: "Instrument",
        timeframe: "Timeframe",
        tradeCount: "Trades",
        chartKicker: "Price path",
        chartTitle: "Execution timeline",
        chartHint: "Drag or scroll to zoom; select a marker to inspect the trade",
        ledgerKicker: "Trade ledger",
        ledgerTitle: "Closed trades",
        noTrades: "There are no closed trades in this candle range.",
        selected: "Selected trade",
        selectPrompt: "Select a chart marker or trade row to inspect its details.",
        side: "Side",
        quantity: "Quantity",
        entry: "Entry",
        exit: "Exit",
        fee: "Fee",
        pnl: "PnL",
        price: "Price",
        longEntry: "L entry",
        longExit: "L exit",
        shortEntry: "S entry",
        shortExit: "S exit",
        loading: "Loading candlestick chart",
        ariaChart: "Interactive candlestick chart with strategy entry and exit markers"
      },
      demo: "Local demo data · unavailable in production builds",
      error: {
        title: "Research data not loaded",
        unauthorized: "This session cannot read research results. Reopen the console through the trusted Tailscale entry point.",
        service: "Data service returned {{status}}: {{message}}",
        fallback: "Unable to load GA data",
        unexpected: "Unable to load GA data: {{message}}",
        retry: "Reload"
      },
      empty: {
        title: "No evolution runs yet",
        body: "Candidate terrain will appear after a parameter search completes."
      },
      summary: {
        aria: "Evolution run summary",
        objective: "Objective",
        generation: "Generation",
        population: "Population",
        bestScore: "Best score"
      },
      evolution: {
        kicker: "Evolution trace",
        title: "Convergence",
        generation: "Generation"
      },
      surface: {
        kicker: "Fitness surface",
        title: "Candidate terrain",
        view: "Terrain view",
        view2d: "2D color",
        view3d: "3D terrain",
        interpolated: "Interpolated surface · drag to rotate, scroll to zoom; select observed candidates by hover/click or arrow keys + Enter",
        unavailable: "This run needs at least two numeric parameters to render candidate terrain.",
        unsupportedObjective: "This run has an unsupported optimization objective, so candidate terrain is unavailable."
      },
      candidates: {
        kicker: "Candidate ledger",
        title: "Candidate comparison",
        count_one: "{{formattedCount}} candidate",
        count_other: "{{formattedCount}} candidates",
        drawdown: "DD",
        selected: "Selected candidate",
        metrics: "Evaluation metrics"
      },
      parallel: {
        kicker: "Multi-axis fingerprint",
        title: "Parameter fingerprint",
        axes: "Up to 12 axes",
        unavailable: "This generation has no chartable parameters."
      },
      loading: {
        charts: "Loading chart engine",
        snapshot: "Loading research snapshot"
      },
      objective: {
        minimizeDrawdown: "Minimize drawdown",
        maximizeReturn: "Maximize return",
        maximizeScore: "Maximize score",
        unsupported: "Unsupported optimization objective"
      },
      chart: {
        generation: "Generation",
        drawdown: "Drawdown",
        score: "Score",
        fitness: "Fitness",
        lowerDrawdown: "Lowest drawdown",
        upperDrawdown: "Highest drawdown",
        lowerScore: "Lowest score",
        upperScore: "Highest score",
        high: "High",
        low: "Low",
        selected: "Selected candidate",
        observed: "Observed candidate",
        bestObserved: "Best observed at each X/Y coordinate"
      },
      aria: {
        convergence: "Convergence chart showing generation score or drawdown bounds",
        surface: "Observed fitness surface for two strategy parameters",
        surface3d: "Interactive three-dimensional interpolated fitness surface for two strategy parameters",
        parallel: "Parallel coordinates for candidate parameters"
      }
    }
  }
} as const;

export function resolveLocale(
  stored: string | null,
  languages: readonly string[]
): Locale {
  if (stored === "zh-TW" || stored === "en") {
    return stored;
  }
  for (const language of languages) {
    const normalized = language.toLowerCase();
    if (normalized === "zh" || normalized.startsWith("zh-")) {
      return "zh-TW";
    }
    if (normalized === "en" || normalized.startsWith("en-")) {
      return "en";
    }
  }
  return "zh-TW";
}

function detectedLocale(): Locale {
  let stored: string | null = null;
  try {
    stored = window.localStorage.getItem(STORAGE_KEY);
  } catch {
    // Storage may be unavailable in hardened browser contexts.
  }
  const languages =
    typeof navigator === "undefined"
      ? []
      : [...navigator.languages, navigator.language];
  return resolveLocale(stored, languages);
}

void i18n.use(initReactI18next).init({
  resources,
  lng: detectedLocale(),
  fallbackLng: "zh-TW",
  supportedLngs: ["zh-TW", "en"],
  interpolation: { escapeValue: false }
});

i18n.on("languageChanged", (language) => {
  const locale: Locale = language === "en" ? "en" : "zh-TW";
  document.documentElement.lang = locale === "en" ? "en" : "zh-Hant";
  try {
    window.localStorage.setItem(STORAGE_KEY, locale);
  } catch {
    // The active language still applies for the current page.
  }
});

document.documentElement.lang =
  i18n.language === "en" ? "en" : "zh-Hant";

export default i18n;
