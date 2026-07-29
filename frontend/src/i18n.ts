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
