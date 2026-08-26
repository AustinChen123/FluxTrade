// @vitest-environment jsdom

import { describe, expect, it } from "vitest";

import i18n from "./i18n";

describe("locale bootstrap", () => {
  it("applies language, document metadata, and the exact storage key", async () => {
    const storage = new Map<string, string>();
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: {
        getItem: (key: string) => storage.get(key) ?? null,
        setItem: (key: string, value: string) => storage.set(key, value)
      }
    });
    await i18n.changeLanguage("en");
    expect(document.documentElement.lang).toBe("en");
    expect(window.localStorage.getItem("fluxtrade.locale")).toBe("en");
    await i18n.changeLanguage("zh-TW");
    expect(document.documentElement.lang).toBe("zh-Hant");
  });
});
