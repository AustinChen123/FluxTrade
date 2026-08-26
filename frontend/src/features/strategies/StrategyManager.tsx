import { useTranslation } from "react-i18next";

import type { Locale } from "../../shared/i18n";
import { StrategyManagerView } from "./StrategyManagerView";
import { useStrategyManager } from "./useStrategyManager";

export function StrategyManager() {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const manager = useStrategyManager(t);

  return <StrategyManagerView {...manager} locale={locale} t={t} />;
}
