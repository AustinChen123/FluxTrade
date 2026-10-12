import { expect, test } from "@playwright/test";

type ExpectedPages = {
  orders: Record<string, unknown>;
  trades: Record<string, unknown>;
};

function integrationEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Spider order browser fixture is missing ${name}`);
  return value;
}

test("real read-only StrategyManager inspects scoped local orders and executions", async ({
  page,
  context
}) => {
  const origin = integrationEnv("SPIDER_ORDERS_REAL_ORIGIN");
  const login = integrationEnv("SPIDER_ORDERS_REAL_LOGIN");
  const expected = JSON.parse(integrationEnv("SPIDER_ORDERS_REAL_EXPECTED_JSON")) as ExpectedPages;
  const expectedOrders = expected.orders;
  const expectedTrades = expected.trades;
  const strategyId = (expectedOrders.scope as { strategy_id: string }).strategy_id;
  const order = (expectedOrders.orders as Array<Record<string, unknown>>)[0];
  const localOrderId = order.local_order_id as string;
  const version = expectedOrders.view_version as string;

  await context.setExtraHTTPHeaders({
    "Tailscale-User-Login": login,
    "Tailscale-App-Capabilities": "{}"
  });
  const spiderRequests: string[] = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.startsWith("/api/v1/spider/orders")) spiderRequests.push(request.url());
  });

  await page.goto(origin);
  await page.getByRole("button", { name: /strategy management|策略管理/i }).click();
  await expect(page.getByText(strategyId, { exact: true })).toBeVisible();
  const safety = page.locator(".strategy-safety");
  await expect(safety.locator("p").filter({ hasText: /LOCKDOWN/ })).toBeVisible();
  const card = page.locator(".strategy-list > li").filter({ hasText: strategyId });
  await expect(card.locator(".strategy-action button")).toHaveCount(0);
  const open = card.getByRole("button", { name: /local execution records|檢視本地執行紀錄/i });
  await open.focus();
  const ordersResponsePromise = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/v1/spider/orders" && response.status() === 200
  );
  await page.keyboard.press("Enter");
  const ordersResponse = await ordersResponsePromise;
  const ordersDto = await ordersResponse.json() as Record<string, unknown>;
  expect(ordersDto.schema_version).toBe("spider_local_orders_v1");
  expect(ordersDto.scope).toEqual(expectedOrders.scope);
  expect(ordersDto.filters).toEqual(expectedOrders.filters);
  expect(ordersDto.view_version).toBe(version);
  expect(ordersDto.orders).toEqual(expectedOrders.orders);
  await expect(page.getByText("SQL_LOCAL_FACTS", { exact: false })).toBeVisible();
  await expect(page.getByText("NOT_ASSERTED", { exact: false })).toBeVisible();
  await expect(page.getByText(localOrderId, { exact: true })).toBeVisible();
  await expect(page.getByText(version, { exact: false })).toBeVisible();
  const ordersTable = page.locator(".spider-orders tbody tr");
  const orderRow = ordersTable.filter({ has: page.getByRole("rowheader", { name: localOrderId }) });
  await expect(orderRow.locator("td").nth(5)).toHaveText(order.price as string);
  const localizedMissingId = /^(?:Not recorded|未記錄)$/;
  for (const expectedOrder of expectedOrders.orders as Array<Record<string, unknown>>) {
    const row = ordersTable.filter({ has: page.getByRole("rowheader", { name: expectedOrder.local_order_id as string }) });
    const providerIdCell = row.locator("td").nth(0);
    if (typeof expectedOrder.provider_order_id === "string") {
      await expect(providerIdCell).toHaveText(expectedOrder.provider_order_id);
    } else {
      expect(expectedOrder.provider_order_id).toBeNull();
      await expect(providerIdCell).toHaveText(localizedMissingId);
    }
  }

  const inspect = page.getByRole("button", { name: new RegExp(localOrderId) });
  const inspectBox = await inspect.boundingBox();
  expect(inspectBox?.height).toBeGreaterThanOrEqual(44);
  await inspect.focus();
  const tradesResponsePromise = page.waitForResponse((response) =>
    new URL(response.url()).pathname === `/api/v1/spider/orders/${encodeURIComponent(localOrderId)}/trades` &&
    response.status() === 200
  );
  await page.keyboard.press("Enter");
  const tradesResponse = await tradesResponsePromise;
  const tradesDto = await tradesResponse.json() as Record<string, unknown>;
  expect(tradesDto.schema_version).toBe("spider_local_trades_v1");
  expect(tradesDto.scope).toEqual(expectedTrades.scope);
  expect(tradesDto.filters).toEqual(expectedTrades.filters);
  expect(tradesDto.local_order_id).toBe(localOrderId);
  expect(tradesDto.view_version).toBe(version);
  expect(tradesDto.trades).toEqual(expectedTrades.trades);
  const tradeQuery = new URL(tradesResponse.url()).searchParams;
  expect(tradeQuery.get("view_version")).toBe(version);
  expect(tradeQuery.has("local_order_id")).toBe(false);
  const firstTrade = (expectedTrades.trades as Array<Record<string, unknown>>)[0];
  const tradesTable = page.locator(".spider-trades tbody tr");
  const tradeRow = tradesTable.filter({ has: page.getByRole("rowheader", { name: firstTrade.local_trade_id as string }) });
  await expect(tradeRow.locator("td").nth(5)).toContainText(firstTrade.fee as string);
  for (const expectedTrade of expectedTrades.trades as Array<Record<string, unknown>>) {
    const row = tradesTable.filter({ has: page.getByRole("rowheader", { name: expectedTrade.local_trade_id as string }) });
    const providerIdCell = row.locator("td").nth(0);
    if (typeof expectedTrade.provider_trade_id === "string") {
      await expect(providerIdCell).toHaveText(expectedTrade.provider_trade_id);
    } else {
      expect(expectedTrade.provider_trade_id).toBeNull();
      await expect(providerIdCell).toHaveText(localizedMissingId);
    }
  }
  await expect(page.getByText("NOT_ASSERTED", { exact: false })).toBeVisible();

  await page.evaluate(() => new Promise((resolve) => window.setTimeout(resolve, 800)));
  expect(spiderRequests).toHaveLength(2);
  expect(spiderRequests.every((url) => new URL(url).searchParams.get("strategy_id") === strategyId)).toBe(true);

  const panel = page.locator(".spider-orders");
  const mobileGeometry = await panel.evaluate((element) => ({
    clientWidth: element.clientWidth,
    scrollWidth: element.scrollWidth,
    viewportWidth: document.documentElement.clientWidth,
    documentWidth: document.documentElement.scrollWidth,
    tableClientWidth: element.querySelector<HTMLElement>(".spider-table-scroll")?.clientWidth ?? 0,
    tableScrollWidth: element.querySelector<HTMLElement>(".spider-table-scroll")?.scrollWidth ?? 0
  }));
  expect(mobileGeometry.documentWidth).toBeLessThanOrEqual(mobileGeometry.viewportWidth);
  expect(mobileGeometry.tableScrollWidth).toBeGreaterThan(mobileGeometry.tableClientWidth);
  expect(mobileGeometry.scrollWidth).toBeLessThanOrEqual(mobileGeometry.clientWidth);
});
