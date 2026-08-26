// @vitest-environment jsdom

import { beforeEach, describe, expect, it, vi } from "vitest";

import { applyTheme, initialTheme, saveTheme } from "./theme";

const storage = new Map<string, string>();

describe("theme bootstrap", () => {
  beforeEach(() => {
    storage.clear();
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: {
        getItem: (key: string) => storage.get(key) ?? null,
        setItem: (key: string, value: string) => storage.set(key, value)
      }
    });
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: vi.fn(() => ({ matches: false }) as MediaQueryList)
    });
    delete document.documentElement.dataset.theme;
    document.head.innerHTML = '<meta name="theme-color" content="">';
  });

  it.each(["light", "dark"] as const)("restores saved %s theme", (theme) => {
    window.localStorage.setItem("fluxtrade.theme", theme);
    expect(initialTheme()).toBe(theme);
  });

  it("falls back to the browser preference", () => {
    vi.spyOn(window, "matchMedia").mockReturnValue({
      matches: true
    } as MediaQueryList);
    expect(initialTheme()).toBe("dark");
  });

  it("applies and persists the selected theme", () => {
    applyTheme("dark");
    saveTheme("dark");
    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(
      document.querySelector('meta[name="theme-color"]')?.getAttribute("content")
    ).toBe("#11191d");
    expect(window.localStorage.getItem("fluxtrade.theme")).toBe("dark");
  });

  it("continues in memory when storage is denied", () => {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: {
        getItem: () => {
          throw new Error("denied");
        },
        setItem: () => {
          throw new Error("denied");
        }
      }
    });
    vi.spyOn(window, "matchMedia").mockReturnValue({
      matches: false
    } as MediaQueryList);
    expect(initialTheme()).toBe("light");
    expect(() => saveTheme("light")).not.toThrow();
  });
});
