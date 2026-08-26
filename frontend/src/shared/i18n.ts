import i18n from "i18next";
import { initReactI18next } from "react-i18next";

import { en } from "./locales/en";
import { zhTW } from "./locales/zh-TW";

export type Locale = "zh-TW" | "en";

const STORAGE_KEY = "fluxtrade.locale";

const resources = { "zh-TW": zhTW, en } as const;

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
