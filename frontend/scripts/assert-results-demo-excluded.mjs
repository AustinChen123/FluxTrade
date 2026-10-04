import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const frontendRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const assetsDirectory = path.join(frontendRoot, "dist", "assets");
const resultsChunks = readdirSync(assetsDirectory).filter((name) =>
  /^BacktestResultsView-[^/]+\.js$/u.test(name)
);

if (resultsChunks.length !== 1) {
  throw new Error(
    `Expected one Results entry chunk, found ${resultsChunks.length}`
  );
}

const chunk = readFileSync(path.join(assetsDirectory, resultsChunks[0]), "utf8");
const demoTradeConstruction = [
  "trade-000184",
  "19851.25",
  "19852.25",
  "2026-07-28T15:30:00.000Z",
  "0.88"
];
const leakedSentinels = demoTradeConstruction.filter((sentinel) =>
  chunk.includes(sentinel)
);

if (leakedSentinels.length > 0) {
  throw new Error(
    `Results entry contains demo trade construction: ${leakedSentinels.join(", ")}`
  );
}

console.log(`Verified demo trade construction is absent from ${resultsChunks[0]}`);

const tradesChunks = readdirSync(assetsDirectory).filter((name) =>
  /^TradeChartView-[^/]+\.js$/u.test(name)
);

if (tradesChunks.length !== 1) {
  throw new Error(
    `Expected one Trades entry chunk, found ${tradesChunks.length}`
  );
}

const tradesChunk = readFileSync(path.join(assetsDirectory, tradesChunks[0]), "utf8");
const demoTradesConstruction = [
  "trade-000184",
  "19851.25",
  "19852.25",
  "2026-07-28T15:30:00.000Z",
  "0.88"
];
const leakedTradesSentinels = demoTradesConstruction.filter((sentinel) =>
  tradesChunk.includes(sentinel)
);

if (leakedTradesSentinels.length > 0) {
  throw new Error(
    `Trades entry contains demo trade construction: ${leakedTradesSentinels.join(", ")}`
  );
}

console.log(`Verified demo trade construction is absent from ${tradesChunks[0]}`);
