export type View = "research" | "results" | "strategies" | "trades";

export interface NavigationState {
  readonly view: View;
  readonly inspectedTradeId: string | null;
}

export interface SerializedNavigation {
  readonly inspectedTradeId: string | null;
  readonly relativeUrl: string;
}

function validTradeId(value: string | null): value is string {
  return (
    value !== null &&
    value.length > 0 &&
    value.length <= 256 &&
    !/[\u0000-\u001F\u007F]/.test(value)
  );
}

export function parseNavigation(search: string): NavigationState {
  const parameters = new URLSearchParams(search);
  const requestedView = parameters.get("view");
  const view: View =
    requestedView === "results" ||
    requestedView === "strategies" ||
    requestedView === "trades"
      ? requestedView
      : "research";
  const requestedTradeId = parameters.get("trade");

  return {
    view,
    inspectedTradeId:
      view === "trades" && validTradeId(requestedTradeId)
        ? requestedTradeId
        : null
  };
}

export function parseDemoMode(currentUrl: URL, dev: boolean): boolean {
  return dev === true && currentUrl.searchParams.get("demo") === "1";
}

export function serializeNavigation(
  currentUrl: URL,
  view: View,
  requestedTradeId: string | null
): SerializedNavigation {
  const inspectedTradeId =
    view === "trades" && validTradeId(requestedTradeId)
      ? requestedTradeId
      : null;

  if (view === "research") {
    currentUrl.searchParams.delete("view");
  } else {
    currentUrl.searchParams.set("view", view);
  }
  if (inspectedTradeId === null) {
    currentUrl.searchParams.delete("trade");
  } else {
    currentUrl.searchParams.set("trade", inspectedTradeId);
  }

  return {
    inspectedTradeId,
    relativeUrl: `${currentUrl.pathname}${currentUrl.search}${currentUrl.hash}`
  };
}
