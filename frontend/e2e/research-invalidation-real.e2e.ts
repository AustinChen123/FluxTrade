import { expect, test } from "@playwright/test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";

type ContinueSignal = { epochId: string };

test("reconnect refreshes committed research data and preserves selection", async ({
  page,
  context
}) => {
  const origin = process.env.RESEARCH_REAL_ORIGIN;
  const cookie = process.env.RESEARCH_REAL_COOKIE;
  const readyPath = process.env.RESEARCH_REAL_READY_FILE;
  const continuePath = process.env.RESEARCH_REAL_CONTINUE_FILE;
  const initialEpochId = process.env.RESEARCH_REAL_INITIAL_EPOCH;
  if (!origin || !cookie || !readyPath || !continuePath || !initialEpochId) {
    throw new Error("real research integration fixture is incomplete");
  }
  const [name, value] = cookie.split("=", 2);
  if (!name || !value) throw new Error("browser session cookie is malformed");
  await context.addCookies([{
    name,
    value,
    url: origin,
    secure: true,
    httpOnly: true,
    sameSite: "Strict"
  }]);

  let streamResponses = 0;
  let epochReads = 0;
  page.on("response", (response) => {
    if (response.url().endsWith("/api/v1/events")) streamResponses += 1;
  });
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/evolution-epochs") epochReads += 1;
  });
  const stream = page.waitForResponse((response) =>
    response.url().endsWith("/api/v1/events") && response.status() === 200
  );
  await page.goto(origin);
  await page.getByRole("button", { name: "Parameter research" }).click();
  await expect(page.locator("#epoch")).toHaveValue(initialEpochId);
  await expect(page.locator(".ranking-list button.is-selected")).toHaveCount(1);
  await stream;

  const selectedCandidate = (
    await page.locator(".ranking-list button.is-selected strong").innerText()
  ).trim();
  const readsBeforeDisconnect = epochReads;
  writeFileSync(readyPath, JSON.stringify({ selectedCandidate }));

  await expect.poll(() => existsSync(continuePath), {
    timeout: 75_000,
    intervals: [50, 100, 250]
  }).toBe(true);
  const signal = JSON.parse(readFileSync(continuePath, "utf8")) as ContinueSignal;

  await expect.poll(() => streamResponses, { timeout: 15_000 }).toBeGreaterThan(1);
  await expect.poll(() => epochReads, { timeout: 15_000 }).toBeGreaterThan(
    readsBeforeDisconnect
  );
  await expect(
    page.locator('#epoch option[value="' + signal.epochId + '"]')
  ).toHaveCount(1);
  await expect(page.locator("#epoch")).toHaveValue(initialEpochId);
  await expect(page.locator(".ranking-list button.is-selected")).toHaveCount(1);
  await expect(
    page.locator(".ranking-list button.is-selected strong")
  ).toHaveText(selectedCandidate);
});
