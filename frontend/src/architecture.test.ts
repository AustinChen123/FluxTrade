import fs from "node:fs";
import path from "node:path";
import {
  NodeFlags,
  SyntaxKind,
  type Expression,
  type FunctionDeclaration,
  type ImportDeclaration,
  type Node,
  type SourceFile,
  type TypeNode
} from "typescript/unstable/ast";
import {
  isArrayBindingPattern,
  isBindingElement,
  isCallExpression,
  isExportDeclaration,
  isFunctionDeclaration,
  isIdentifier,
  isImportDeclaration,
  isImportTypeNode,
  isJsxAttribute,
  isJsxExpression,
  isJsxOpeningElement,
  isJsxSelfClosingElement,
  isLiteralTypeNode,
  isMetaProperty,
  isNamedImports,
  isNamedExports,
  isNoSubstitutionTemplateLiteral,
  isPropertyAccessExpression,
  isStringLiteral,
  isVariableDeclaration,
  isVariableDeclarationList
} from "typescript/unstable/ast";
import {
  API,
  type Checker,
  type NodeHandle
} from "typescript/unstable/sync";
import { afterAll, describe, expect, it } from "vitest";

type ImportKind = "type-only" | "value";

interface ImportEdge {
  readonly importer: string;
  readonly kind: ImportKind;
  readonly resolved: string;
  readonly specifier: string;
}

interface PolicyGlobalUse {
  readonly access: string;
  readonly importer: string;
}

type ArchitectureRole =
  | "main"
  | "api"
  | "vite-env"
  | "app-navigation"
  | "app-shell"
  | "research-io"
  | "strategy-io"
  | "feature-owner"
  | "feature-presentation"
  | "feature-model"
  | "feature-chart"
  | "feature-demo"
  | "shared-chart"
  | "shared-pure"
  | "shared-theme"
  | "shared-i18n"
  | "source-test"
  | "architecture-test"
  | "locale-resource-test"
  | "vite-config"
  | "playwright-config"
  | "toolchain-test"
  | "e2e-fixture"
  | "e2e-test";

const frontendRoot = process.cwd();
const sourceRoot = path.resolve(process.cwd(), "src");
const e2eRoot = path.resolve(frontendRoot, "e2e");
const configPath = path.resolve(process.cwd(), "tsconfig.json");
const compilerApi = new API();
const compilerSnapshot = compilerApi.updateSnapshot({
  openProjects: [configPath]
});
const compilerProject = compilerSnapshot
  .getProjects()
  .find((project) => path.resolve(project.configFileName) === configPath);
if (!compilerProject) {
  throw new Error("frontend architecture project is unavailable");
}
afterAll(() => {
  compilerSnapshot.dispose();
  compilerApi.close();
});
const executableExtensions = new Set([
  ".cjs",
  ".cts",
  ".js",
  ".jsx",
  ".mjs",
  ".mts",
  ".ts",
  ".tsx"
]);
let compilerFixtureSequence = 0;

const expectedInventory = [
  "api.test.ts",
  "api.ts",
  "app/App.test.tsx",
  "app/App.tsx",
  "app/navigation.test.ts",
  "app/navigation.ts",
  "architecture.test.ts",
  "features/research/FitnessSurface3D.test.tsx",
  "features/research/FitnessSurface3D.tsx",
  "features/research/ResearchPage.test.tsx",
  "features/research/ResearchPage.tsx",
  "features/research/ResearchRoute.test.tsx",
  "features/research/ResearchRoute.tsx",
  "features/research/demo.test.ts",
  "features/research/demo.ts",
  "features/research/gaCharts.test.ts",
  "features/research/gaCharts.ts",
  "features/research/gaDomain.test.ts",
  "features/research/gaDomain.ts",
  "features/research/researchModel.test.ts",
  "features/research/researchModel.ts",
  "features/research/useResearchWorkspace.test.ts",
  "features/research/useResearchWorkspace.ts",
  "features/results/BacktestResultsView.test.tsx",
  "features/results/BacktestResultsView.tsx",
  "features/results/demo.test.ts",
  "features/results/demo.ts",
  "features/results/resultsCharts.test.ts",
  "features/results/resultsCharts.ts",
  "features/results/resultsModel.test.ts",
  "features/results/resultsModel.ts",
  "features/results/useTradePagination.test.ts",
  "features/results/useTradePagination.ts",
  "features/strategies/StrategyManager.test.tsx",
  "features/strategies/StrategyManager.tsx",
  "features/strategies/StrategyManagerView.test.tsx",
  "features/strategies/StrategyManagerView.tsx",
  "features/strategies/strategyCommandState.test.ts",
  "features/strategies/strategyCommandState.ts",
  "features/strategies/useStrategyManager.test.ts",
  "features/strategies/useStrategyManager.ts",
  "features/trades/CandlestickChart.test.tsx",
  "features/trades/CandlestickChart.tsx",
  "features/trades/TradeChartView.test.tsx",
  "features/trades/TradeChartView.tsx",
  "features/trades/demo.test.ts",
  "features/trades/demo.ts",
  "features/trades/tradeCharts.test.ts",
  "features/trades/tradeCharts.ts",
  "features/trades/tradeModel.test.ts",
  "features/trades/tradeModel.ts",
  "main.tsx",
  "shared/charts/EChart.test.tsx",
  "shared/charts/EChart.tsx",
  "shared/charts/EChartCore.tsx",
  "shared/format/decimal.test.ts",
  "shared/format/decimal.ts",
  "shared/i18n.test.ts",
  "shared/i18n.ts",
  "shared/locales/en.ts",
  "shared/locales/resources.test.ts",
  "shared/locales/zh-TW.ts",
  "shared/theme.test.ts",
  "shared/theme.ts",
  "shared/time/presentation.test.ts",
  "shared/time/presentation.ts",
  "shared/time/utc.test.ts",
  "shared/time/utc.ts",
  "shared/trading/closedTrade.ts",
  "styles/index.css",
  "styles/research.css",
  "styles/responsive.css",
  "styles/results.css",
  "styles/shell.css",
  "styles/strategies.css",
  "styles/tokens.css",
  "styles/trades.css",
  "vite-env.d.ts"
] as const;
const expectedInventorySet = new Set<string>(expectedInventory);

const expectedE2eInventory = [
  "e2e/console-smoke.e2e.ts",
  "e2e/fixtures.ts"
] as const;

const expectedRootExecutableInventory = [
  "playwright.config.ts",
  "toolchain-contract.test.js",
  "vite.config.ts"
] as const;

const expectedOutsideImportLedger = [
  "e2e/console-smoke.e2e.ts|./fixtures|value|e2e/fixtures.ts",
  "e2e/console-smoke.e2e.ts|@playwright/test|value|@playwright/test",
  "playwright.config.ts|@playwright/test|value|@playwright/test",
  "toolchain-contract.test.js|./e2e/fixtures.ts|value|e2e/fixtures.ts",
  "toolchain-contract.test.js|./playwright.config.ts|value|playwright.config.ts",
  "toolchain-contract.test.js|node:child_process|value|node:child_process",
  "toolchain-contract.test.js|node:fs|value|node:fs",
  "toolchain-contract.test.js|node:path|value|node:path",
  "toolchain-contract.test.js|vitest|value|vitest",
  "vite.config.ts|@vitejs/plugin-react|value|@vitejs/plugin-react",
  "vite.config.ts|vite|value|vite"
].sort();

const expectedBareImportLedger = [
  "api.test.ts|vitest|value",
  "app/App.test.tsx|@testing-library/react|value",
  "app/App.test.tsx|react|value",
  "app/App.test.tsx|react|value",
  "app/App.test.tsx|react|value",
  "app/App.test.tsx|react|value",
  "app/App.test.tsx|vitest|value",
  "app/App.tsx|react-i18next|value",
  "app/App.tsx|react|value",
  "app/navigation.test.ts|vitest|value",
  "architecture.test.ts|node:fs|value",
  "architecture.test.ts|node:path|value",
  "architecture.test.ts|typescript/unstable/ast|value",
  "architecture.test.ts|typescript/unstable/ast|value",
  "architecture.test.ts|typescript/unstable/sync|value",
  "architecture.test.ts|vitest|value",
  "features/research/demo.test.ts|vitest|value",
  "features/research/FitnessSurface3D.test.tsx|@testing-library/react|value",
  "features/research/FitnessSurface3D.test.tsx|vitest|value",
  "features/research/FitnessSurface3D.tsx|react|value",
  "features/research/gaCharts.test.ts|vitest|value",
  "features/research/gaCharts.ts|echarts/core|type-only",
  "features/research/gaCharts.ts|echarts|type-only",
  "features/research/gaDomain.test.ts|vitest|value",
  "features/research/researchModel.test.ts|vitest|value",
  "features/research/ResearchPage.test.tsx|@testing-library/react|value",
  "features/research/ResearchPage.test.tsx|vitest|value",
  "features/research/ResearchPage.tsx|react-i18next|value",
  "features/research/ResearchPage.tsx|react|value",
  "features/research/ResearchRoute.test.tsx|@testing-library/react|value",
  "features/research/ResearchRoute.test.tsx|vitest|value",
  "features/research/ResearchRoute.tsx|react-i18next|value",
  "features/research/ResearchRoute.tsx|react|value",
  "features/research/useResearchWorkspace.test.ts|@testing-library/react|value",
  "features/research/useResearchWorkspace.test.ts|vitest|value",
  "features/research/useResearchWorkspace.ts|react|value",
  "features/results/BacktestResultsView.test.tsx|@testing-library/react|value",
  "features/results/BacktestResultsView.test.tsx|vitest|value",
  "features/results/BacktestResultsView.tsx|react-i18next|value",
  "features/results/BacktestResultsView.tsx|react|value",
  "features/results/demo.test.ts|vitest|value",
  "features/results/resultsCharts.test.ts|vitest|value",
  "features/results/resultsCharts.ts|echarts/core|type-only",
  "features/results/resultsModel.test.ts|vitest|value",
  "features/results/useTradePagination.test.ts|@testing-library/react|value",
  "features/results/useTradePagination.test.ts|vitest|value",
  "features/results/useTradePagination.ts|react|value",
  "features/strategies/strategyCommandState.test.ts|vitest|value",
  "features/strategies/StrategyManager.test.tsx|@testing-library/react|value",
  "features/strategies/StrategyManager.test.tsx|vitest|value",
  "features/strategies/StrategyManager.tsx|react-i18next|value",
  "features/strategies/StrategyManagerView.test.tsx|@testing-library/react|value",
  "features/strategies/StrategyManagerView.test.tsx|vitest|value",
  "features/strategies/StrategyManagerView.tsx|react-i18next|type-only",
  "features/strategies/StrategyManagerView.tsx|react|value",
  "features/strategies/useStrategyManager.test.ts|@testing-library/react|value",
  "features/strategies/useStrategyManager.test.ts|vitest|value",
  "features/strategies/useStrategyManager.ts|react-i18next|type-only",
  "features/strategies/useStrategyManager.ts|react|value",
  "features/trades/CandlestickChart.test.tsx|@testing-library/react|value",
  "features/trades/CandlestickChart.test.tsx|vitest|value",
  "features/trades/CandlestickChart.tsx|echarts/charts|value",
  "features/trades/CandlestickChart.tsx|echarts/components|value",
  "features/trades/CandlestickChart.tsx|echarts/core|value",
  "features/trades/demo.test.ts|vitest|value",
  "features/trades/tradeCharts.test.ts|vitest|value",
  "features/trades/tradeCharts.ts|echarts/core|type-only",
  "features/trades/TradeChartView.test.tsx|@testing-library/react|value",
  "features/trades/TradeChartView.test.tsx|vitest|value",
  "features/trades/TradeChartView.tsx|react-i18next|value",
  "features/trades/TradeChartView.tsx|react|value",
  "features/trades/tradeModel.test.ts|vitest|value",
  "main.tsx|react-dom/client|value",
  "main.tsx|react|value",
  "shared/charts/EChart.test.tsx|@testing-library/react|value",
  "shared/charts/EChart.test.tsx|vitest|value",
  "shared/charts/EChart.tsx|echarts/charts|value",
  "shared/charts/EChart.tsx|echarts/components|value",
  "shared/charts/EChart.tsx|echarts/core|value",
  "shared/charts/EChartCore.tsx|echarts/core|type-only",
  "shared/charts/EChartCore.tsx|echarts/core|value",
  "shared/charts/EChartCore.tsx|echarts/renderers|value",
  "shared/charts/EChartCore.tsx|react|value",
  "shared/format/decimal.test.ts|vitest|value",
  "shared/i18n.test.ts|vitest|value",
  "shared/i18n.ts|i18next|value",
  "shared/i18n.ts|react-i18next|value",
  "shared/locales/resources.test.ts|vitest|value",
  "shared/theme.test.ts|vitest|value",
  "shared/time/presentation.test.ts|vitest|value",
  "shared/time/utc.test.ts|vitest|value"
] as const;

const expectedRelativeImportLedger = [
  "api.test.ts|./api|value|api.ts",
  "app/App.test.tsx|../api|type-only|api.ts",
  "app/App.test.tsx|../api|type-only|api.ts",
  "app/App.test.tsx|../shared/i18n|value|shared/i18n.ts",
  "app/App.test.tsx|./App|value|app/App.tsx",
  "app/App.tsx|../features/research/ResearchRoute|value|features/research/ResearchRoute.tsx",
  "app/App.tsx|../features/results/BacktestResultsView|value|features/results/BacktestResultsView.tsx",
  "app/App.tsx|../features/strategies/StrategyManager|value|features/strategies/StrategyManager.tsx",
  "app/App.tsx|../features/trades/TradeChartView|value|features/trades/TradeChartView.tsx",
  "app/App.tsx|../shared/i18n|type-only|shared/i18n.ts",
  "app/App.tsx|../shared/theme|value|shared/theme.ts",
  "app/App.tsx|./navigation|value|app/navigation.ts",
  "app/navigation.test.ts|./navigation|value|app/navigation.ts",
  "features/research/demo.test.ts|./demo|value|features/research/demo.ts",
  "features/research/demo.ts|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/FitnessSurface3D.test.tsx|./FitnessSurface3D|value|features/research/FitnessSurface3D.tsx",
  "features/research/FitnessSurface3D.test.tsx|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/FitnessSurface3D.tsx|../../shared/theme|type-only|shared/theme.ts",
  "features/research/FitnessSurface3D.tsx|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/gaCharts.test.ts|../../api|type-only|api.ts",
  "features/research/gaCharts.test.ts|./demo|value|features/research/demo.ts",
  "features/research/gaCharts.test.ts|./gaCharts|value|features/research/gaCharts.ts",
  "features/research/gaCharts.test.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/gaCharts.ts|../../shared/theme|type-only|shared/theme.ts",
  "features/research/gaCharts.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/gaDomain.test.ts|../../api|type-only|api.ts",
  "features/research/gaDomain.test.ts|./demo|value|features/research/demo.ts",
  "features/research/gaDomain.test.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/gaDomain.ts|../../api|type-only|api.ts",
  "features/research/researchModel.test.ts|../../api|type-only|api.ts",
  "features/research/researchModel.test.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/researchModel.test.ts|./researchModel|value|features/research/researchModel.ts",
  "features/research/researchModel.ts|../../api|type-only|api.ts",
  "features/research/researchModel.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/ResearchPage.test.tsx|../../api|type-only|api.ts",
  "features/research/ResearchPage.test.tsx|../../shared/i18n|value|shared/i18n.ts",
  "features/research/ResearchPage.test.tsx|./researchModel|value|features/research/researchModel.ts",
  "features/research/ResearchPage.test.tsx|./ResearchPage|value|features/research/ResearchPage.tsx",
  "features/research/ResearchPage.test.tsx|./useResearchWorkspace|type-only|features/research/useResearchWorkspace.ts",
  "features/research/ResearchPage.tsx|../../shared/charts/EChart|value|shared/charts/EChart.tsx",
  "features/research/ResearchPage.tsx|../../shared/theme|type-only|shared/theme.ts",
  "features/research/ResearchPage.tsx|./FitnessSurface3D|value|features/research/FitnessSurface3D.tsx",
  "features/research/ResearchPage.tsx|./gaCharts|type-only|features/research/gaCharts.ts",
  "features/research/ResearchPage.tsx|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/ResearchPage.tsx|./researchModel|value|features/research/researchModel.ts",
  "features/research/ResearchPage.tsx|./useResearchWorkspace|type-only|features/research/useResearchWorkspace.ts",
  "features/research/ResearchRoute.test.tsx|../../api|type-only|api.ts",
  "features/research/ResearchRoute.test.tsx|../../api|type-only|api.ts",
  "features/research/ResearchRoute.test.tsx|../../shared/i18n|value|shared/i18n.ts",
  "features/research/ResearchRoute.test.tsx|./ResearchRoute|value|features/research/ResearchRoute.tsx",
  "features/research/ResearchRoute.tsx|../../shared/i18n|type-only|shared/i18n.ts",
  "features/research/ResearchRoute.tsx|../../shared/theme|type-only|shared/theme.ts",
  "features/research/ResearchRoute.tsx|./gaCharts|value|features/research/gaCharts.ts",
  "features/research/ResearchRoute.tsx|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/ResearchRoute.tsx|./ResearchPage|value|features/research/ResearchPage.tsx",
  "features/research/ResearchRoute.tsx|./useResearchWorkspace|value|features/research/useResearchWorkspace.ts",
  "features/research/useResearchWorkspace.test.ts|../../api|type-only|api.ts",
  "features/research/useResearchWorkspace.test.ts|../../api|value|api.ts",
  "features/research/useResearchWorkspace.test.ts|./useResearchWorkspace|value|features/research/useResearchWorkspace.ts",
  "features/research/useResearchWorkspace.ts|../../api|value|api.ts",
  "features/research/useResearchWorkspace.ts|./demo|value|features/research/demo.ts",
  "features/research/useResearchWorkspace.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/useResearchWorkspace.ts|./researchModel|value|features/research/researchModel.ts",
  "features/results/BacktestResultsView.test.tsx|../../shared/i18n|value|shared/i18n.ts",
  "features/results/BacktestResultsView.test.tsx|./BacktestResultsView|value|features/results/BacktestResultsView.tsx",
  "features/results/BacktestResultsView.test.tsx|./demo|value|features/results/demo.ts",
  "features/results/BacktestResultsView.test.tsx|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/BacktestResultsView.tsx|../../shared/charts/EChart|value|shared/charts/EChart.tsx",
  "features/results/BacktestResultsView.tsx|../../shared/format/decimal|value|shared/format/decimal.ts",
  "features/results/BacktestResultsView.tsx|../../shared/i18n|type-only|shared/i18n.ts",
  "features/results/BacktestResultsView.tsx|../../shared/theme|type-only|shared/theme.ts",
  "features/results/BacktestResultsView.tsx|../../shared/time/utc|value|shared/time/utc.ts",
  "features/results/BacktestResultsView.tsx|./demo|value|features/results/demo.ts",
  "features/results/BacktestResultsView.tsx|./resultsCharts|value|features/results/resultsCharts.ts",
  "features/results/BacktestResultsView.tsx|./resultsModel|value|features/results/resultsModel.ts",
  "features/results/BacktestResultsView.tsx|./useTradePagination|value|features/results/useTradePagination.ts",
  "features/results/demo.test.ts|./demo|value|features/results/demo.ts",
  "features/results/demo.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/results/demo.ts|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/resultsCharts.test.ts|./demo|value|features/results/demo.ts",
  "features/results/resultsCharts.test.ts|./resultsCharts|value|features/results/resultsCharts.ts",
  "features/results/resultsCharts.ts|../../shared/format/decimal|value|shared/format/decimal.ts",
  "features/results/resultsCharts.ts|../../shared/i18n|type-only|shared/i18n.ts",
  "features/results/resultsCharts.ts|../../shared/theme|type-only|shared/theme.ts",
  "features/results/resultsCharts.ts|../../shared/time/utc|value|shared/time/utc.ts",
  "features/results/resultsCharts.ts|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/resultsModel.test.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/results/resultsModel.test.ts|./resultsModel|value|features/results/resultsModel.ts",
  "features/results/resultsModel.ts|../../shared/format/decimal|value|shared/format/decimal.ts",
  "features/results/resultsModel.ts|../../shared/time/utc|value|shared/time/utc.ts",
  "features/results/resultsModel.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/results/useTradePagination.test.ts|./demo|value|features/results/demo.ts",
  "features/results/useTradePagination.test.ts|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/useTradePagination.test.ts|./useTradePagination|value|features/results/useTradePagination.ts",
  "features/results/useTradePagination.ts|./resultsModel|value|features/results/resultsModel.ts",
  "features/strategies/strategyCommandState.test.ts|../../api|type-only|api.ts",
  "features/strategies/strategyCommandState.test.ts|./strategyCommandState|value|features/strategies/strategyCommandState.ts",
  "features/strategies/strategyCommandState.ts|../../api|type-only|api.ts",
  "features/strategies/StrategyManager.test.tsx|../../api|type-only|api.ts",
  "features/strategies/StrategyManager.test.tsx|../../api|value|api.ts",
  "features/strategies/StrategyManager.test.tsx|../../shared/i18n|value|shared/i18n.ts",
  "features/strategies/StrategyManager.test.tsx|./StrategyManager|value|features/strategies/StrategyManager.tsx",
  "features/strategies/StrategyManager.tsx|../../shared/i18n|type-only|shared/i18n.ts",
  "features/strategies/StrategyManager.tsx|./StrategyManagerView|value|features/strategies/StrategyManagerView.tsx",
  "features/strategies/StrategyManager.tsx|./useStrategyManager|value|features/strategies/useStrategyManager.ts",
  "features/strategies/StrategyManagerView.test.tsx|../../shared/i18n|value|shared/i18n.ts",
  "features/strategies/StrategyManagerView.test.tsx|./strategyCommandState|type-only|features/strategies/strategyCommandState.ts",
  "features/strategies/StrategyManagerView.test.tsx|./StrategyManagerView|value|features/strategies/StrategyManagerView.tsx",
  "features/strategies/StrategyManagerView.tsx|../../shared/i18n|type-only|shared/i18n.ts",
  "features/strategies/StrategyManagerView.tsx|./strategyCommandState|type-only|features/strategies/strategyCommandState.ts",
  "features/strategies/useStrategyManager.test.ts|../../api|type-only|api.ts",
  "features/strategies/useStrategyManager.test.ts|../../api|value|api.ts",
  "features/strategies/useStrategyManager.test.ts|../../shared/i18n|value|shared/i18n.ts",
  "features/strategies/useStrategyManager.test.ts|./strategyCommandState|value|features/strategies/strategyCommandState.ts",
  "features/strategies/useStrategyManager.test.ts|./useStrategyManager|value|features/strategies/useStrategyManager.ts",
  "features/strategies/useStrategyManager.ts|../../api|value|api.ts",
  "features/strategies/useStrategyManager.ts|./strategyCommandState|value|features/strategies/strategyCommandState.ts",
  "features/trades/CandlestickChart.test.tsx|./CandlestickChart|value|features/trades/CandlestickChart.tsx",
  "features/trades/CandlestickChart.tsx|../../shared/charts/EChartCore|value|shared/charts/EChartCore.tsx",
  "features/trades/demo.test.ts|./demo|value|features/trades/demo.ts",
  "features/trades/demo.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/trades/demo.ts|./tradeModel|type-only|features/trades/tradeModel.ts",
  "features/trades/tradeCharts.test.ts|./tradeCharts|value|features/trades/tradeCharts.ts",
  "features/trades/tradeCharts.test.ts|./tradeModel|value|features/trades/tradeModel.ts",
  "features/trades/tradeCharts.ts|../../shared/theme|type-only|shared/theme.ts",
  "features/trades/tradeCharts.ts|./tradeModel|type-only|features/trades/tradeModel.ts",
  "features/trades/TradeChartView.test.tsx|../../shared/i18n|value|shared/i18n.ts",
  "features/trades/TradeChartView.test.tsx|./demo|value|features/trades/demo.ts",
  "features/trades/TradeChartView.test.tsx|./TradeChartView|value|features/trades/TradeChartView.tsx",
  "features/trades/TradeChartView.tsx|../../shared/format/decimal|value|shared/format/decimal.ts",
  "features/trades/TradeChartView.tsx|../../shared/i18n|type-only|shared/i18n.ts",
  "features/trades/TradeChartView.tsx|../../shared/theme|type-only|shared/theme.ts",
  "features/trades/TradeChartView.tsx|../../shared/time/utc|value|shared/time/utc.ts",
  "features/trades/TradeChartView.tsx|./CandlestickChart|value|features/trades/CandlestickChart.tsx",
  "features/trades/TradeChartView.tsx|./demo|value|features/trades/demo.ts",
  "features/trades/TradeChartView.tsx|./tradeCharts|value|features/trades/tradeCharts.ts",
  "features/trades/TradeChartView.tsx|./tradeModel|value|features/trades/tradeModel.ts",
  "features/trades/tradeModel.test.ts|./tradeModel|value|features/trades/tradeModel.ts",
  "features/trades/tradeModel.ts|../../shared/format/decimal|value|shared/format/decimal.ts",
  "features/trades/tradeModel.ts|../../shared/time/utc|value|shared/time/utc.ts",
  "features/trades/tradeModel.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "main.tsx|./app/App|value|app/App.tsx",
  "main.tsx|./shared/i18n|value|shared/i18n.ts",
  "main.tsx|./shared/theme|value|shared/theme.ts",
  "main.tsx|./styles/index.css|value|styles/index.css",
  "shared/charts/EChart.test.tsx|./EChart|value|shared/charts/EChart.tsx",
  "shared/charts/EChart.tsx|./EChartCore|value|shared/charts/EChartCore.tsx",
  "shared/format/decimal.test.ts|./decimal|value|shared/format/decimal.ts",
  "shared/i18n.test.ts|./i18n|value|shared/i18n.ts",
  "shared/i18n.ts|./locales/en|value|shared/locales/en.ts",
  "shared/i18n.ts|./locales/zh-TW|value|shared/locales/zh-TW.ts",
  "shared/locales/resources.test.ts|./en|value|shared/locales/en.ts",
  "shared/locales/resources.test.ts|./zh-TW|value|shared/locales/zh-TW.ts",
  "shared/theme.test.ts|./theme|value|shared/theme.ts",
  "shared/time/presentation.test.ts|./presentation|value|shared/time/presentation.ts",
  "shared/time/presentation.ts|./utc|value|shared/time/utc.ts",
  "shared/time/utc.test.ts|./utc|value|shared/time/utc.ts"
] as const;

const expectedPolicyGlobalLedger = [
  "api.ts|fetch()",
  "app/App.tsx|import.meta.env.DEV",
  "app/App.tsx|window.history.replaceState()",
  "app/App.tsx|window.location.href",
  "app/App.tsx|window.location.href",
  "app/App.tsx|window.location.search",
  "features/research/FitnessSurface3D.tsx|window.devicePixelRatio",
  "features/strategies/useStrategyManager.ts|window.confirm()",
  "features/strategies/useStrategyManager.ts|window.crypto.randomUUID()",
  "features/strategies/useStrategyManager.ts|window.sessionStorage.getItem()",
  "features/strategies/useStrategyManager.ts|window.sessionStorage.removeItem()",
  "features/strategies/useStrategyManager.ts|window.sessionStorage.setItem()",
  "main.tsx|document.getElementById()",
  "shared/i18n.ts|document.documentElement.lang",
  "shared/i18n.ts|document.documentElement.lang",
  "shared/i18n.ts|navigator",
  "shared/i18n.ts|navigator.language",
  "shared/i18n.ts|navigator.languages",
  "shared/i18n.ts|window.localStorage.getItem()",
  "shared/i18n.ts|window.localStorage.setItem()",
  "shared/theme.ts|document.documentElement.dataset.theme",
  'shared/theme.ts|document.querySelector("meta[name=\\"theme-color\\"]")',
  "shared/theme.ts|window.localStorage.getItem()",
  "shared/theme.ts|window.localStorage.setItem()",
  "shared/theme.ts|window.matchMedia?.()"
] as const;

const cssPayloadNames = [
  "tokens.css",
  "shell.css",
  "research.css",
  "results.css",
  "trades.css",
  "strategies.css",
  "responsive.css"
] as const;
const cssIndex = `@import "./tokens.css";
@import "./shell.css";
@import "./research.css";
@import "./results.css";
@import "./trades.css";
@import "./strategies.css";
@import "./responsive.css";
`;

async function sha256(bytes: Uint8Array): Promise<string> {
  const result = await globalThis.crypto.subtle.digest(
    "SHA-256",
    Uint8Array.from(bytes).buffer
  );
  return [...new Uint8Array(result)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function assertCompleteCssFragment(bytes: Uint8Array): void {
  type State = "normal" | "comment" | "single-quoted" | "double-quoted";
  let state: State = "normal";
  let escaped = false;
  let depth = 0;
  let topLevelPrelude = false;

  for (let index = 0; index < bytes.length; index += 1) {
    const byte = bytes[index];
    const next = bytes[index + 1];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (state === "comment") {
      if (byte === 42 && next === 47) {
        state = "normal";
        index += 1;
      }
      continue;
    }
    if (byte === 92) {
      if (depth === 0) {
        topLevelPrelude = true;
      }
      escaped = true;
      continue;
    }
    if (state === "single-quoted") {
      if (byte === 39) {
        state = "normal";
      }
      continue;
    }
    if (state === "double-quoted") {
      if (byte === 34) {
        state = "normal";
      }
      continue;
    }
    if (byte === 47 && next === 42) {
      state = "comment";
      index += 1;
    } else if (byte === 39) {
      if (depth === 0) {
        topLevelPrelude = true;
      }
      state = "single-quoted";
    } else if (byte === 34) {
      if (depth === 0) {
        topLevelPrelude = true;
      }
      state = "double-quoted";
    } else if (byte === 123) {
      if (depth === 0) {
        topLevelPrelude = false;
      }
      depth += 1;
    } else if (byte === 125) {
      depth -= 1;
      if (depth < 0) {
        throw new Error("CSS fragment closes an unopened block");
      }
    } else if (
      depth === 0 &&
      ![9, 10, 12, 13, 32].includes(byte)
    ) {
      topLevelPrelude = true;
    }
  }
  if (state !== "normal" || escaped || depth !== 0 || topLevelPrelude) {
    throw new Error(
      `CSS fragment is incomplete: state=${state} escaped=${escaped} depth=${depth} prelude=${topLevelPrelude}`
    );
  }
}

function walkFiles(root: string): string[] {
  return fs
    .readdirSync(root)
    .sort()
    .flatMap((name) => {
      const absolute = path.join(root, name);
      const status = fs.lstatSync(absolute);
      if (status.isSymbolicLink()) {
        throw new Error(`architecture inventory rejects symlink: ${absolute}`);
      }
      if (status.isDirectory()) {
        return walkFiles(absolute);
      }
      if (!status.isFile()) {
        throw new Error(
          `architecture inventory rejects non-regular entry: ${absolute}`
        );
      }
      return [absolute];
    })
    .sort();
}

function relativeInventory(root: string, prefix: string): string[] {
  return walkFiles(root).map((file) =>
    path
      .join(prefix, path.relative(root, file))
      .replaceAll(path.sep, "/")
  );
}

function rootExecutableFiles(root: string): string[] {
  return fs
    .readdirSync(root)
    .sort()
    .flatMap((name) => {
      if (!executableExtensions.has(path.extname(name))) {
        return [];
      }
      const absolute = path.join(root, name);
      const status = fs.lstatSync(absolute);
      if (status.isSymbolicLink() || !status.isFile()) {
        throw new Error(
          `architecture root executable is not regular: ${absolute}`
        );
      }
      return [absolute];
    });
}

function assertInventory(
  actual: readonly string[],
  expected: readonly string[],
  owner: string
): void {
  if (owner === "src") {
    const legacyTokens = new Set([
      "decimal",
      "echart",
      "echartcore",
      "i18n",
      "legacyresearchworkspace",
      "styles",
      "theme",
      "utc"
    ]);
    for (const entry of actual.filter(
      (candidate) => !expectedInventorySet.has(candidate)
    )) {
      const basename = path.basename(entry, path.extname(entry));
      if (legacyTokens.has(normalizedLegacyToken(basename))) {
        throw new Error(`architecture legacy owner rejected: ${entry}`);
      }
    }
  }
  if (
    actual.length !== expected.length ||
    actual.some((entry, index) => entry !== expected[index])
  ) {
    throw new Error(
      `architecture ${owner} inventory mismatch: ${JSON.stringify(actual)}`
    );
  }
}

function validatedArchitectureFiles(): string[] {
  const sourceInventory = relativeInventory(sourceRoot, "");
  assertInventory(sourceInventory, expectedInventory, "src");
  const e2eInventory = relativeInventory(e2eRoot, "e2e");
  assertInventory(e2eInventory, expectedE2eInventory, "e2e");
  const rootFiles = rootExecutableFiles(frontendRoot);
  const rootInventory = rootFiles.map((file) =>
    path.relative(frontendRoot, file).replaceAll(path.sep, "/")
  );
  assertInventory(
    rootInventory,
    expectedRootExecutableInventory,
    "root executable"
  );
  return [
    ...sourceInventory
      .filter((file) => [".ts", ".tsx"].includes(path.extname(file)))
      .map((file) => path.join(sourceRoot, file)),
    ...e2eInventory.map((file) => path.join(frontendRoot, file)),
    ...rootFiles
  ];
}

function sourceFiles(): string[] {
  return validatedArchitectureFiles()
    .filter((file) => path.resolve(file).startsWith(`${sourceRoot}${path.sep}`))
    .filter((file) => [".ts", ".tsx"].includes(path.extname(file)))
    .map((file) => {
      const status = fs.lstatSync(file);
      if (!status.isFile() || status.isSymbolicLink()) {
        throw new Error(`architecture source is no longer regular: ${file}`);
      }
      return file;
    });
}

function literalSpecifier(node: Expression | TypeNode): string {
  if (isLiteralTypeNode(node) && isStringLiteral(node.literal)) {
    return node.literal.text;
  }
  if (isStringLiteral(node) || isNoSubstitutionTemplateLiteral(node)) {
    return node.text;
  }
  throw new Error("architecture import specifiers must be string literals");
}

function importKind(node: ImportDeclaration): ImportKind {
  const clause = node.importClause;
  if (!clause) {
    return "value";
  }
  if (clause.phaseModifier === SyntaxKind.TypeKeyword) {
    return "type-only";
  }
  if (
    clause.namedBindings &&
    isNamedImports(clause.namedBindings) &&
    clause.namedBindings.elements.length > 0 &&
    clause.namedBindings.elements.every((element) => element.isTypeOnly)
  ) {
    return "type-only";
  }
  return "value";
}

function exportKind(node: Node): ImportKind {
  if (!isExportDeclaration(node)) {
    throw new Error("architecture export classifier received a non-export");
  }
  if (node.isTypeOnly) {
    return "type-only";
  }
  if (
    node.exportClause &&
    isNamedExports(node.exportClause) &&
    node.exportClause.elements.length > 0 &&
    node.exportClause.elements.every((element) => element.isTypeOnly)
  ) {
    return "type-only";
  }
  return "value";
}

function resolveSpecifier(
  importer: string,
  specifier: string,
  edgeRoot = sourceRoot
): string {
  if (!specifier.startsWith(".")) {
    return specifier;
  }
  const base = path.resolve(path.dirname(importer), specifier);
  if (
    path.resolve(importer) === path.join(sourceRoot, "main.tsx") &&
    specifier === "./styles/index.css"
  ) {
    const stylesheet = path.join(sourceRoot, "styles/index.css");
    if (!fs.lstatSync(stylesheet).isFile()) {
      throw new Error("architecture stylesheet entry is not regular");
    }
    return "styles/index.css";
  }
  const explicitExtension = path.extname(base);
  const substitutedCandidates =
    explicitExtension === ".js" || explicitExtension === ".jsx"
      ? [
          base.slice(0, -explicitExtension.length) + ".ts",
          base.slice(0, -explicitExtension.length) + ".tsx",
          base.slice(0, -explicitExtension.length) + ".d.ts"
        ]
      : explicitExtension === ".mjs"
        ? [
            base.slice(0, -explicitExtension.length) + ".mts",
            base.slice(0, -explicitExtension.length) + ".d.mts"
          ]
        : explicitExtension === ".cjs"
          ? [
              base.slice(0, -explicitExtension.length) + ".cts",
              base.slice(0, -explicitExtension.length) + ".d.cts"
            ]
          : [];
  for (const candidate of [
    ...substitutedCandidates,
    base,
    `${base}.ts`,
    `${base}.tsx`,
    `${base}.mts`,
    `${base}.cts`,
    `${base}.js`,
    `${base}.jsx`,
    `${base}.mjs`,
    `${base}.cjs`,
    `${base}.d.ts`,
    path.join(base, "index.ts"),
    path.join(base, "index.tsx"),
    path.join(base, "index.mts"),
    path.join(base, "index.cts")
  ]) {
    if (fs.existsSync(candidate)) {
      const status = fs.lstatSync(candidate);
      if (status.isSymbolicLink()) {
        throw new Error(`architecture import target is not regular: ${candidate}`);
      }
      if (status.isDirectory()) {
        continue;
      }
      if (!status.isFile()) {
        throw new Error(`architecture import target is not regular: ${candidate}`);
      }
      const relative = path.relative(edgeRoot, candidate);
      if (relative.startsWith("..") || path.isAbsolute(relative)) {
        throw new Error(`architecture import escapes its root: ${specifier}`);
      }
      return relative.replaceAll(path.sep, "/");
    }
  }
  throw new Error(`unresolved architecture import: ${specifier}`);
}

function collectImportsFromSource(
  sourceFile: SourceFile,
  importer = sourceFile.fileName,
  checker?: Checker,
  edgeRoot = sourceRoot,
  resolver: (importer: string, specifier: string, edgeRoot: string) => string =
    resolveSpecifier
): ImportEdge[] {
  const edges: ImportEdge[] = [];
  const add = (specifier: string, kind: ImportKind) => {
    edges.push({
      importer: path.relative(edgeRoot, importer).replaceAll(path.sep, "/"),
      kind,
      resolved: resolver(importer, specifier, edgeRoot),
      specifier
    });
  };
  const visit = (node: Node) => {
    if (isImportDeclaration(node)) {
      add(literalSpecifier(node.moduleSpecifier), importKind(node));
    } else if (isExportDeclaration(node) && node.moduleSpecifier) {
      add(literalSpecifier(node.moduleSpecifier), exportKind(node));
    } else if (isImportTypeNode(node)) {
      add(literalSpecifier(node.argument), "type-only");
    } else if (
      isCallExpression(node) &&
      (node.expression.kind === SyntaxKind.ImportKeyword ||
        (isIdentifier(node.expression) && node.expression.text === "require"))
    ) {
      if (
        isIdentifier(node.expression) &&
        checker
          ?.getSymbolAtLocation(node.expression)
          ?.declarations.some(
            (declaration) =>
              path.resolve(handleSourceFileName(declaration)) ===
              path.resolve(importer)
          )
      ) {
        return;
      }
      if (node.arguments.length !== 1) {
        throw new Error("architecture import calls require one literal");
      }
      add(literalSpecifier(node.arguments[0]), "value");
    }
    node.forEachChild(visit);
  };
  visit(sourceFile);
  return edges;
}

function collectImports(): ImportEdge[] {
  return sourceFiles()
    .flatMap((file) => {
      const sourceFile = compilerProject!.program.getSourceFile(file);
      if (!sourceFile) {
        throw new Error(`frontend compiler omitted ${file}`);
      }
      return collectImportsFromSource(sourceFile, file, compilerProject!.checker);
    })
    .sort((left, right) =>
      [left.importer, left.specifier, left.kind, left.resolved]
        .join("|")
        .localeCompare(
          [right.importer, right.specifier, right.kind, right.resolved].join("|"),
          "en"
        )
    );
}

function collectOutsideImports(): ImportEdge[] {
  const files = validatedArchitectureFiles().filter(
    (file) => !path.resolve(file).startsWith(`${sourceRoot}${path.sep}`)
  );
  for (const file of files) {
    const status = fs.lstatSync(file);
    if (status.isSymbolicLink() || !status.isFile()) {
      throw new Error(`architecture outside source is not regular: ${file}`);
    }
  }
  const api = new API();
  let snapshot: ReturnType<API["updateSnapshot"]> | null = null;
  try {
    snapshot = api.updateSnapshot({ openFiles: files });
    return files
      .flatMap((file) => {
        const project = snapshot!.getDefaultProjectForFile(file);
        if (!project) {
          throw new Error(`architecture project unavailable for ${file}`);
        }
        const sourceFile = project.program.getSourceFile(file);
        if (!sourceFile) {
          throw new Error(`architecture parser omitted ${file}`);
        }
        return collectImportsFromSource(
          sourceFile,
          file,
          project.checker,
          frontendRoot
        );
      })
      .sort((left, right) =>
        [left.importer, left.specifier, left.kind, left.resolved]
          .join("|")
          .localeCompare(
            [right.importer, right.specifier, right.kind, right.resolved].join(
              "|"
            ),
            "en"
          )
      );
  } finally {
    snapshot?.dispose();
    api.close();
  }
}

const featureOwnerPaths = new Set([
  "features/research/ResearchRoute.tsx",
  "features/results/BacktestResultsView.tsx",
  "features/results/useTradePagination.ts",
  "features/strategies/StrategyManager.tsx",
  "features/trades/TradeChartView.tsx"
]);
const featurePresentationPaths = new Set([
  "features/research/FitnessSurface3D.tsx",
  "features/research/ResearchPage.tsx",
  "features/strategies/StrategyManagerView.tsx",
  "features/trades/CandlestickChart.tsx"
]);
const featureModelPaths = new Set([
  "features/research/gaDomain.ts",
  "features/research/researchModel.ts",
  "features/results/resultsModel.ts",
  "features/strategies/strategyCommandState.ts",
  "features/trades/tradeModel.ts"
]);
const featureChartPaths = new Set([
  "features/research/gaCharts.ts",
  "features/results/resultsCharts.ts",
  "features/trades/tradeCharts.ts"
]);
const featureDemoPaths = new Set([
  "features/research/demo.ts",
  "features/results/demo.ts",
  "features/trades/demo.ts"
]);
const allowedTestPackages = new Set([
  "@testing-library/react",
  "react",
  "vitest"
]);
const sharedPurePrefixes = [
  "shared/format/",
  "shared/locales/",
  "shared/time/",
  "shared/trading/"
];

function featureName(relativePath: string): string | null {
  const match = /^features\/([^/]+)\//u.exec(relativePath);
  return match?.[1] ?? null;
}

function isSharedPurePath(relativePath: string): boolean {
  return sharedPurePrefixes.some((prefix) => relativePath.startsWith(prefix));
}

function normalizedLegacyToken(value: string): string {
  return value.toLowerCase().replaceAll(/[_-]/gu, "");
}

function architectureRole(importer: string): ArchitectureRole {
  if (importer === "main.tsx") return "main";
  if (importer === "api.ts") return "api";
  if (importer === "vite-env.d.ts") return "vite-env";
  if (importer === "app/navigation.ts") return "app-navigation";
  if (importer === "app/App.tsx") return "app-shell";
  if (importer === "features/research/useResearchWorkspace.ts") {
    return "research-io";
  }
  if (importer === "features/strategies/useStrategyManager.ts") {
    return "strategy-io";
  }
  if (featureOwnerPaths.has(importer)) return "feature-owner";
  if (featurePresentationPaths.has(importer)) return "feature-presentation";
  if (featureModelPaths.has(importer)) return "feature-model";
  if (featureChartPaths.has(importer)) return "feature-chart";
  if (featureDemoPaths.has(importer)) return "feature-demo";
  if (importer === "architecture.test.ts") return "architecture-test";
  if (importer === "shared/locales/resources.test.ts") {
    return "locale-resource-test";
  }
  if (importer === "vite.config.ts") return "vite-config";
  if (importer === "playwright.config.ts") return "playwright-config";
  if (importer === "toolchain-contract.test.js") return "toolchain-test";
  if (importer === "e2e/fixtures.ts") return "e2e-fixture";
  if (importer === "e2e/console-smoke.e2e.ts") return "e2e-test";
  if (
    expectedInventorySet.has(importer) &&
    /\.test\.[jt]sx?$/u.test(importer)
  ) {
    return "source-test";
  }
  if (importer.startsWith("shared/charts/")) return "shared-chart";
  if (importer === "shared/theme.ts") return "shared-theme";
  if (importer === "shared/i18n.ts") return "shared-i18n";
  if (isSharedPurePath(importer) && !/\.test\.[jt]sx?$/u.test(importer)) {
    return "shared-pure";
  }
  throw new Error(`architecture role is unknown: ${importer}`);
}

function denyEdge(edge: ImportEdge): never {
  throw new Error(
    `architecture edge denied: ${edge.importer} -> ${edge.specifier} (${edge.kind})`
  );
}

function isSameFeature(importer: string, resolved: string): boolean {
  const feature = featureName(importer);
  return feature !== null && featureName(resolved) === feature;
}

const outsideEdgeKeysByRole: Partial<
  Record<ArchitectureRole, ReadonlySet<string>>
> = {
  "vite-config": new Set([
    "@vitejs/plugin-react|value|@vitejs/plugin-react",
    "vite|value|vite"
  ]),
  "playwright-config": new Set([
    "@playwright/test|value|@playwright/test"
  ]),
  "toolchain-test": new Set([
    "./e2e/fixtures.ts|value|e2e/fixtures.ts",
    "./playwright.config.ts|value|playwright.config.ts",
    "node:child_process|value|node:child_process",
    "node:fs|value|node:fs",
    "node:path|value|node:path",
    "vitest|value|vitest"
  ]),
  "e2e-fixture": new Set(),
  "e2e-test": new Set([
    "./fixtures|value|e2e/fixtures.ts",
    "@playwright/test|value|@playwright/test"
  ])
};

function assertOutsideEdge(edge: ImportEdge, role: ArchitectureRole): void {
  if (
    !outsideEdgeKeysByRole[role]?.has(
      `${edge.specifier}|${edge.kind}|${edge.resolved}`
    )
  ) {
    denyEdge(edge);
  }
}

function assertOutsideEdgeSet(
  importer: string,
  edges: readonly ImportEdge[]
): void {
  const role = architectureRole(importer);
  const expected = [...(outsideEdgeKeysByRole[role] ?? [])].sort();
  const actual = edges
    .filter((edge) => edge.importer === importer)
    .map((edge) => `${edge.specifier}|${edge.kind}|${edge.resolved}`)
    .sort();
  if (
    actual.length !== expected.length ||
    actual.some((entry, index) => entry !== expected[index])
  ) {
    throw new Error(
      `architecture outside edge set mismatch: ${importer} -> ${JSON.stringify(actual)}`
    );
  }
}

function assertBareEdge(edge: ImportEdge, role: ArchitectureRole): void {
  if (
    [
      "vite-config",
      "playwright-config",
      "toolchain-test",
      "e2e-fixture",
      "e2e-test"
    ].includes(role)
  ) {
    assertOutsideEdge(edge, role);
    return;
  }
  if (role === "architecture-test") {
    if (
      !new Set([
        "node:fs",
        "node:path",
        "typescript/unstable/ast",
        "typescript/unstable/sync",
        "vitest"
      ]).has(edge.specifier)
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (role === "locale-resource-test" || role === "source-test") {
    if (!allowedTestPackages.has(edge.specifier)) denyEdge(edge);
    if (
      edge.specifier === "react" &&
      edge.importer !== "app/App.test.tsx"
    ) {
      denyEdge(edge);
    }
    if (
      edge.specifier === "@testing-library/react" &&
      !new Set([
        "app/App.test.tsx",
        "features/research/FitnessSurface3D.test.tsx",
        "features/research/ResearchPage.test.tsx",
        "features/research/ResearchRoute.test.tsx",
        "features/research/useResearchWorkspace.test.ts",
        "features/results/BacktestResultsView.test.tsx",
        "features/results/useTradePagination.test.ts",
        "features/strategies/StrategyManager.test.tsx",
        "features/strategies/StrategyManagerView.test.tsx",
        "features/strategies/useStrategyManager.test.ts",
        "features/trades/CandlestickChart.test.tsx",
        "features/trades/TradeChartView.test.tsx",
        "shared/charts/EChart.test.tsx"
      ]).has(edge.importer)
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (
    role === "feature-presentation" &&
    edge.specifier.startsWith("echarts") &&
    edge.importer !== "features/trades/CandlestickChart.tsx"
  ) {
    denyEdge(edge);
  }
  const exactByRole: Partial<Record<ArchitectureRole, ReadonlySet<string>>> = {
    main: new Set(["react|value", "react-dom/client|value"]),
    "app-shell": new Set(["react|value", "react-i18next|value"]),
    "research-io": new Set(["react|value"]),
    "strategy-io": new Set(["react|value", "react-i18next|type-only"]),
    "feature-owner": new Set([
      "react|value",
      "react-i18next|type-only",
      "react-i18next|value"
    ]),
    "feature-presentation": new Set([
      "echarts/charts|value",
      "echarts/components|value",
      "echarts/core|value",
      "react|value",
      "react-i18next|type-only",
      "react-i18next|value"
    ]),
    "feature-chart": new Set(["echarts|type-only", "echarts/core|type-only"]),
    "shared-chart": new Set([
      "echarts/charts|value",
      "echarts/components|value",
      "echarts/core|type-only",
      "echarts/core|value",
      "echarts/renderers|value",
      "react|value"
    ]),
    "shared-i18n": new Set(["i18next|value", "react-i18next|value"])
  };
  if (!exactByRole[role]?.has(`${edge.specifier}|${edge.kind}`)) {
    denyEdge(edge);
  }
}

function assertSourceTestRelativeEdge(edge: ImportEdge): void {
  if (edge.importer === "shared/locales/resources.test.ts") {
    if (!new Set(["shared/locales/en.ts", "shared/locales/zh-TW.ts"]).has(edge.resolved)) {
      denyEdge(edge);
    }
    return;
  }
  if (edge.importer === "architecture.test.ts") denyEdge(edge);
  const importerFeature = featureName(edge.importer);
  const resolvedFeature = featureName(edge.resolved);
  if (importerFeature !== null) {
    if (
      resolvedFeature !== importerFeature &&
      !edge.resolved.startsWith("shared/") &&
      edge.resolved !== "api.ts"
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (edge.importer.startsWith("app/")) {
    if (
      !edge.resolved.startsWith("app/") &&
      !edge.resolved.startsWith("shared/") &&
      edge.resolved !== "api.ts"
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (edge.importer.startsWith("shared/")) {
    if (!edge.resolved.startsWith("shared/")) denyEdge(edge);
    return;
  }
  if (edge.importer === "api.test.ts" && edge.resolved === "api.ts") return;
  denyEdge(edge);
}

function assertRepositoryEdge(edge: ImportEdge, role: ArchitectureRole): void {
  if (
    [
      "vite-config",
      "playwright-config",
      "toolchain-test",
      "e2e-fixture",
      "e2e-test"
    ].includes(role)
  ) {
    assertOutsideEdge(edge, role);
    return;
  }
  if (role === "source-test" || role === "locale-resource-test") {
    assertSourceTestRelativeEdge(edge);
    return;
  }
  if (role === "architecture-test") denyEdge(edge);
  if (role === "main") {
    if (
      !new Set([
        "app/App.tsx",
        "shared/i18n.ts",
        "shared/theme.ts",
        "styles/index.css"
      ]).has(edge.resolved)
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (role === "api" || role === "vite-env" || role === "app-navigation") {
    denyEdge(edge);
  }
  if (role === "app-shell") {
    if (
      !new Set([
        "app/navigation.ts",
        "features/research/ResearchRoute.tsx",
        "features/results/BacktestResultsView.tsx",
        "features/strategies/StrategyManager.tsx",
        "features/trades/TradeChartView.tsx",
        "shared/i18n.ts",
        "shared/theme.ts"
      ]).has(edge.resolved)
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (role === "research-io" || role === "strategy-io") {
    if (
      !isSameFeature(edge.importer, edge.resolved) &&
      !edge.resolved.startsWith("shared/") &&
      edge.resolved !== "api.ts"
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (role === "feature-owner" || role === "feature-presentation") {
    if (
      !isSameFeature(edge.importer, edge.resolved) &&
      !edge.resolved.startsWith("shared/")
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (role === "feature-model") {
    if (edge.resolved === "api.ts") {
      if (edge.kind !== "type-only") denyEdge(edge);
      return;
    }
    if (
      (isSameFeature(edge.importer, edge.resolved) &&
        featureModelPaths.has(edge.resolved)) ||
      isSharedPurePath(edge.resolved)
    ) {
      return;
    }
    denyEdge(edge);
  }
  if (role === "feature-chart") {
    if (
      isSameFeature(edge.importer, edge.resolved) &&
      featureModelPaths.has(edge.resolved)
    ) {
      return;
    }
    if (
      edge.resolved.startsWith("shared/charts/") ||
      isSharedPurePath(edge.resolved) ||
      ["shared/theme.ts", "shared/i18n.ts"].includes(edge.resolved)
    ) {
      if (
        ["shared/theme.ts", "shared/i18n.ts"].includes(edge.resolved) &&
        edge.kind !== "type-only"
      ) {
        denyEdge(edge);
      }
      return;
    }
    denyEdge(edge);
  }
  if (role === "feature-demo") {
    if (
      (isSameFeature(edge.importer, edge.resolved) &&
        featureModelPaths.has(edge.resolved)) ||
      (isSharedPurePath(edge.resolved) && edge.kind === "type-only")
    ) {
      return;
    }
    denyEdge(edge);
  }
  if (role === "shared-chart") {
    if (
      !edge.resolved.startsWith("shared/charts/") &&
      !isSharedPurePath(edge.resolved)
    ) {
      denyEdge(edge);
    }
    return;
  }
  if (role === "shared-pure") {
    if (!isSharedPurePath(edge.resolved)) denyEdge(edge);
    return;
  }
  if (role === "shared-theme") denyEdge(edge);
  if (role === "shared-i18n") {
    if (!new Set(["shared/locales/en.ts", "shared/locales/zh-TW.ts"]).has(edge.resolved)) {
      denyEdge(edge);
    }
    return;
  }
  denyEdge(edge);
}

function assertArchitectureEdge(edge: ImportEdge): void {
  const role = architectureRole(edge.importer);
  if (edge.specifier.startsWith(".")) {
    assertRepositoryEdge(edge, role);
  } else {
    assertBareEdge(edge, role);
  }
  if (edge.resolved.endsWith("/demo.ts")) {
    const feature = featureName(edge.resolved);
    const allowed = new Set([
      `features/${feature}/BacktestResultsView.tsx`,
      `features/${feature}/TradeChartView.tsx`,
      `features/${feature}/useResearchWorkspace.ts`
    ]);
    if (!allowed.has(edge.importer) && !/\.test\.[jt]sx?$/u.test(edge.importer)) {
      denyEdge(edge);
    }
  }
  if (
    !/\.test\.[jt]sx?$/u.test(edge.importer) &&
    /\.test\.[jt]sx?$/u.test(edge.resolved)
  ) {
    denyEdge(edge);
  }
}

const policyGlobalNames = new Set([
  "confirm",
  "crypto",
  "devicePixelRatio",
  "document",
  "fetch",
  "frames",
  "globalThis",
  "history",
  "localStorage",
  "location",
  "matchMedia",
  "navigator",
  "opener",
  "parent",
  "self",
  "sessionStorage",
  "top",
  "window"
]);

function handleSourceFileName(handle: NodeHandle): string {
  const node = handle.resolve();
  if (!node) {
    throw new Error("compiler declaration has no source file");
  }
  return node.getSourceFile().fileName;
}

function symbolIsDeclaredInSource(
  node: Node,
  sourceFile: SourceFile,
  checker: Checker
): boolean {
  return (
    checker
      .getSymbolAtLocation(node)
      ?.declarations.some(
        (declaration) =>
          path.resolve(handleSourceFileName(declaration)) ===
          path.resolve(sourceFile.fileName)
      ) ?? false
  );
}

function maximalPolicyAccess(node: Node): { access: string; outer: Node } {
  let access = node.getText(node.getSourceFile());
  let outer = node;
  while (
    outer.parent &&
    isPropertyAccessExpression(outer.parent) &&
    outer.parent.expression === outer
  ) {
    access += `${outer.parent.questionDotToken ? "?." : "."}${outer.parent.name.text}`;
    outer = outer.parent;
  }
  if (
    outer.parent &&
    isCallExpression(outer.parent) &&
    outer.parent.expression === outer
  ) {
    const call = outer.parent;
    if (access === "document.querySelector") {
      if (call.arguments.length !== 1 || !isStringLiteral(call.arguments[0])) {
        throw new Error(
          "document.querySelector policy requires one literal selector"
        );
      }
      access += `(${JSON.stringify(call.arguments[0].text)})`;
    } else {
      access += call.questionDotToken ? "?.()" : "()";
    }
    outer = call;
  }
  return { access, outer };
}

function collectPolicyGlobalUsesFromSource(
  sourceFile: SourceFile,
  checker: Checker,
  importerOverride?: string
): PolicyGlobalUse[] {
  const uses: PolicyGlobalUse[] = [];
  const importer =
    importerOverride ??
    path.relative(sourceRoot, sourceFile.fileName).replaceAll(path.sep, "/");
  const add = (node: Node) => {
    uses.push({ importer, access: maximalPolicyAccess(node).access });
  };
  const visit = (node: Node) => {
    if (
      isIdentifier(node) &&
      policyGlobalNames.has(node.text) &&
      !(
        isPropertyAccessExpression(node.parent) &&
        node.parent.name === node
      ) &&
      !symbolIsDeclaredInSource(node, sourceFile, checker)
    ) {
      if (!checker.getSymbolAtLocation(node)) {
        throw new Error(`unresolved policy global: ${node.text}`);
      }
      add(node);
    } else if (
      isMetaProperty(node) &&
      node.keywordToken === SyntaxKind.ImportKeyword &&
      node.name.text === "meta"
    ) {
      add(node);
    }
    node.forEachChild(visit);
  };
  visit(sourceFile);
  return uses;
}

function collectPolicyGlobalUses(): PolicyGlobalUse[] {
  return sourceFiles()
    .filter((file) => !/\.test\.[cm]?[jt]sx?$/.test(file))
    .flatMap((file) => {
      const sourceFile = compilerProject!.program.getSourceFile(file);
      if (!sourceFile) {
        throw new Error(`frontend compiler omitted ${file}`);
      }
      return collectPolicyGlobalUsesFromSource(
        sourceFile,
        compilerProject!.checker
      );
    })
    .sort((left, right) =>
      `${left.importer}|${left.access}`.localeCompare(
        `${right.importer}|${right.access}`,
        "en"
      )
    );
}

const allowedPolicyGlobals = new Map<string, ReadonlySet<string>>([
  ["api.ts", new Set(["fetch()"])],
  ["main.tsx", new Set(["document.getElementById()"])],
  [
    "app/App.tsx",
    new Set([
      "import.meta.env.DEV",
      "window.history.replaceState()",
      "window.location.href",
      "window.location.search"
    ])
  ],
  [
    "features/strategies/useStrategyManager.ts",
    new Set([
      "window.confirm()",
      "window.crypto.randomUUID()",
      "window.sessionStorage.getItem()",
      "window.sessionStorage.removeItem()",
      "window.sessionStorage.setItem()"
    ])
  ],
  [
    "shared/theme.ts",
    new Set([
      "document.documentElement.dataset.theme",
      'document.querySelector("meta[name=\\"theme-color\\"]")',
      "window.localStorage.getItem()",
      "window.localStorage.setItem()",
      "window.matchMedia?.()"
    ])
  ],
  [
    "shared/i18n.ts",
    new Set([
      "document.documentElement.lang",
      "navigator",
      "navigator.language",
      "navigator.languages",
      "window.localStorage.getItem()",
      "window.localStorage.setItem()"
    ])
  ],
  [
    "features/research/FitnessSurface3D.tsx",
    new Set(["window.devicePixelRatio"])
  ]
]);

function assertPolicyGlobalUse(use: PolicyGlobalUse): void {
  if (!allowedPolicyGlobals.get(use.importer)?.has(use.access)) {
    throw new Error(
      `architecture policy global denied: ${use.importer} -> ${use.access}`
    );
  }
}

function assertImportMetaContract(
  sourceFile: SourceFile,
  importer: string
): void {
  const metaUses = descendants(sourceFile).filter(
    (node) =>
      isMetaProperty(node) &&
      node.keywordToken === SyntaxKind.ImportKeyword &&
      node.name.text === "meta"
  );
  if (importer !== "app/App.tsx" && metaUses.length > 0) {
    throw new Error(`architecture import.meta denied: ${importer}`);
  }
  for (const meta of metaUses) {
    const env = meta.parent;
    const dev = env?.parent;
    if (
      !env ||
      !isPropertyAccessExpression(env) ||
      env.expression !== meta ||
      env.questionDotToken ||
      env.name.text !== "env" ||
      !dev ||
      !isPropertyAccessExpression(dev) ||
      dev.expression !== env ||
      dev.questionDotToken ||
      dev.name.text !== "DEV"
    ) {
      throw new Error(`architecture import.meta chain denied: ${importer}`);
    }
    const call = dev.parent;
    if (
      !isCallExpression(call) ||
      call.arguments.length !== 2 ||
      call.arguments[1] !== dev ||
      call.arguments[0].getText(sourceFile).replaceAll(/\s+/gu, "") !==
        "newURL(window.location.href)" ||
      !isIdentifier(call.expression) ||
      call.expression.text !== "parseDemoMode"
    ) {
      throw new Error(`architecture import.meta use denied: ${importer}`);
    }
    const declaration = call.parent;
    if (
      !isVariableDeclaration(declaration) ||
      declaration.initializer !== call ||
      !isIdentifier(declaration.name) ||
      declaration.name.text !== "demoMode" ||
      !isVariableDeclarationList(declaration.parent) ||
      (declaration.parent.flags & NodeFlags.Const) === 0
    ) {
      throw new Error(`architecture import.meta result denied: ${importer}`);
    }
    let owner: Node | undefined = declaration.parent;
    while (owner && !isFunctionDeclaration(owner)) {
      owner = owner.parent;
    }
    if (!owner || owner.name?.text !== "App") {
      throw new Error(`architecture import.meta owner denied: ${importer}`);
    }
  }
  if (importer === "app/App.tsx" && metaUses.length !== 1) {
    throw new Error("architecture App requires one exact import.meta.env.DEV");
  }
}

function assertParseDemoModeContract(sourceFile: SourceFile): void {
  const functions = findNamedFunction(sourceFile, "parseDemoMode");
  if (functions.length !== 1) {
    throw new Error("architecture requires one parseDemoMode owner");
  }
  const normalized = functions[0]
    .getText(sourceFile)
    .replaceAll(/\s+/gu, "");
  const exact = [
    "exportfunctionparseDemoMode(currentUrl:URL,dev:boolean):boolean{",
    'returndev===true&&currentUrl.searchParams.get("demo")==="1";',
    "}"
  ].join("");
  if (normalized !== exact) {
    throw new Error("architecture parseDemoMode contract changed");
  }
}

function assertAppDemoPropagation(
  sourceFile: SourceFile,
  checker: Checker
): void {
  const app = findNamedFunction(sourceFile, "App");
  if (app.length !== 1) {
    throw new Error("architecture requires one App owner");
  }
  const declarations = descendants(app[0]).filter(
    (node) =>
      isVariableDeclaration(node) &&
      isIdentifier(node.name) &&
      node.name.text === "demoMode"
  );
  if (declarations.length !== 1 || !isVariableDeclaration(declarations[0])) {
    throw new Error("architecture App requires one local demoMode owner");
  }
  const demoSymbol = checker.getSymbolAtLocation(declarations[0].name);
  if (!demoSymbol) {
    throw new Error("architecture App demoMode symbol is unavailable");
  }
  const allDemoAttributes = descendants(app[0]).filter(
    (node) => isJsxAttribute(node) && node.name.getText() === "demoMode"
  );
  if (allDemoAttributes.length !== 3) {
    throw new Error("architecture App requires exactly three demoMode props");
  }
  for (const facade of [
    "ResearchRoute",
    "BacktestResultsView",
    "TradeChartView"
  ]) {
    const matches = descendants(app[0]).filter((node) => {
      if (
        !(isJsxOpeningElement(node) || isJsxSelfClosingElement(node)) ||
        !isIdentifier(node.tagName) ||
        node.tagName.text !== facade
      ) {
        return false;
      }
      const demoAttribute = node.attributes.properties.find(
        (attribute) =>
          isJsxAttribute(attribute) && attribute.name.getText() === "demoMode"
      );
      return (
        demoAttribute !== undefined &&
        isJsxAttribute(demoAttribute) &&
        demoAttribute.initializer !== undefined &&
        isJsxExpression(demoAttribute.initializer) &&
        demoAttribute.initializer.expression !== undefined &&
        isIdentifier(demoAttribute.initializer.expression) &&
        demoAttribute.initializer.expression.text === "demoMode" &&
        checker.getSymbolAtLocation(demoAttribute.initializer.expression) ===
          demoSymbol
      );
    });
    if (matches.length !== 1) {
      throw new Error(`architecture demo propagation changed: ${facade}`);
    }
  }
}

function directRelativeSpecifiers(edges: ImportEdge[], importer: string) {
  return edges
    .filter((edge) => edge.importer === importer && edge.specifier.startsWith("."))
    .map((edge) => `${edge.specifier}|${edge.kind}|${edge.resolved}`)
    .sort();
}

function findNamedFunction(sourceFile: SourceFile, name: string) {
  const matches: FunctionDeclaration[] = [];
  const visit = (node: Node) => {
    if (isFunctionDeclaration(node) && node.name?.text === name) {
      matches.push(node);
    }
    node.forEachChild(visit);
  };
  visit(sourceFile);
  return matches;
}

function descendants(node: Node): Node[] {
  const nodes: Node[] = [];
  const visit = (child: Node) => {
    nodes.push(child);
    child.forEachChild(visit);
  };
  node.forEachChild(visit);
  return nodes;
}

function declaredNames(node: Node): string[] {
  const names: string[] = [];
  for (const descendant of descendants(node)) {
    if (isVariableDeclaration(descendant)) {
      if (isIdentifier(descendant.name)) {
        names.push(descendant.name.text);
      } else if (isArrayBindingPattern(descendant.name)) {
        names.push(
          ...descendant.name.elements.flatMap((element) =>
            isBindingElement(element) &&
            element.name !== undefined &&
            isIdentifier(element.name)
              ? [element.name.text]
              : []
          )
        );
      }
    }
  }
  return names;
}

function withCompilerFixture<T>(
  source: string,
  inspect: (sourceFile: SourceFile, checker: Checker) => T,
  extension = ".ts"
): T {
  const fixturePath = path.join(
    "/tmp",
    `fluxtrade-architecture-${process.pid}-${compilerFixtureSequence++}${extension}`
  );
  fs.writeFileSync(fixturePath, source, "utf8");
  const api = new API();
  let snapshot: ReturnType<API["updateSnapshot"]> | null = null;
  try {
    snapshot = api.updateSnapshot({ openFiles: [fixturePath] });
    const project = snapshot.getDefaultProjectForFile(fixturePath);
    if (!project) {
      throw new Error("architecture fixture project is unavailable");
    }
    const sourceFile = project.program.getSourceFile(fixturePath);
    if (!sourceFile) {
      throw new Error("architecture fixture was not parsed");
    }
    return inspect(sourceFile, project.checker);
  } finally {
    snapshot?.dispose();
    api.close();
    fs.rmSync(fixturePath, { force: true });
  }
}

function fixtureEdges(
  source: string,
  importer: string,
  relativeResolution = "shared/forbidden.ts"
): ImportEdge[] {
  return withCompilerFixture(source, (sourceFile, checker) =>
    collectImportsFromSource(
      sourceFile,
      sourceFile.fileName,
      checker,
      path.dirname(sourceFile.fileName),
      (_fixtureImporter, specifier) =>
        specifier.startsWith(".") ? relativeResolution : specifier
    ).map((edge) => ({ ...edge, importer }))
  );
}

function architectureEdge(
  importer: string,
  specifier: string,
  resolved: string,
  kind: ImportKind = "value"
): ImportEdge {
  return { importer, specifier, resolved, kind };
}

function assertViteEnvContract(content: string): void {
  if (content !== '/// <reference types="vite/client" />\n') {
    throw new Error("architecture vite-env.d.ts bytes changed");
  }
}

function assertCurrentArchitecture(): void {
  const sourceEdges = collectImports();
  const outsideEdges = collectOutsideImports();
  for (const edge of [...sourceEdges, ...outsideEdges]) {
    assertArchitectureEdge(edge);
  }
  for (const importer of [
    "vite.config.ts",
    "playwright.config.ts",
    "toolchain-contract.test.js",
    "e2e/fixtures.ts",
    "e2e/console-smoke.e2e.ts"
  ]) {
    assertOutsideEdgeSet(importer, outsideEdges);
  }
  for (const use of collectPolicyGlobalUses()) {
    assertPolicyGlobalUse(use);
  }
  for (const file of sourceFiles().filter(
    (candidate) => !/\.test\.[cm]?[jt]sx?$/u.test(candidate)
  )) {
    const sourceFile = compilerProject!.program.getSourceFile(file);
    if (!sourceFile) {
      throw new Error(`frontend compiler omitted ${file}`);
    }
    assertImportMetaContract(
      sourceFile,
      path.relative(sourceRoot, file).replaceAll(path.sep, "/")
    );
  }
  const navigation = compilerProject!.program.getSourceFile(
    path.join(sourceRoot, "app/navigation.ts")
  );
  const app = compilerProject!.program.getSourceFile(
    path.join(sourceRoot, "app/App.tsx")
  );
  if (!navigation) {
    throw new Error("navigation.ts is unavailable to architecture ratchet");
  }
  if (!app) {
    throw new Error("App.tsx is unavailable to architecture ratchet");
  }
  assertParseDemoModeContract(navigation);
  assertAppDemoPropagation(app, compilerProject!.checker);
  assertViteEnvContract(
    fs.readFileSync(path.join(sourceRoot, "vite-env.d.ts"), "utf8")
  );
}

describe("frontend architecture ratchet", () => {
  it("freezes the exact repository-relative import ledger", () => {
    expect(
      collectImports()
        .filter((edge) => edge.specifier.startsWith("."))
        .map(
          (edge) =>
            `${edge.importer}|${edge.specifier}|${edge.kind}|${edge.resolved}`
        )
    ).toEqual(expectedRelativeImportLedger);
  });
  it("freezes the exact bare-package import ledger", () => {
    expect(
      collectImports()
        .filter((edge) => !edge.specifier.startsWith("."))
        .map((edge) => `${edge.importer}|${edge.specifier}|${edge.kind}`)
    ).toEqual(expectedBareImportLedger);
  });
  it("freezes every outside-src tooling and e2e import", () => {
    const edges = collectOutsideImports();
    expect(
      edges.map(
        (edge) =>
          `${edge.importer}|${edge.specifier}|${edge.kind}|${edge.resolved}`
      )
    ).toEqual(expectedOutsideImportLedger);
    for (const importer of [
      "vite.config.ts",
      "playwright.config.ts",
      "toolchain-contract.test.js",
      "e2e/fixtures.ts",
      "e2e/console-smoke.e2e.ts"
    ]) {
      expect(() => assertOutsideEdgeSet(importer, edges)).not.toThrow();
    }
  });

  it("rejects removal of every required outside-src import independently", () => {
    const edges = collectOutsideImports();
    for (const importer of [
      "vite.config.ts",
      "playwright.config.ts",
      "toolchain-contract.test.js",
      "e2e/console-smoke.e2e.ts"
    ]) {
      const owned = edges.filter((edge) => edge.importer === importer);
      if (importer === "toolchain-contract.test.js") {
        expect(owned).toHaveLength(6);
      }
      for (const removed of owned) {
        expect(() =>
          assertOutsideEdgeSet(
            importer,
            edges.filter((edge) => edge !== removed)
          )
        ).toThrow("architecture outside edge set mismatch");
      }
    }
  });

  it.each([
    architectureEdge("vite.config.ts", "@playwright/test", "@playwright/test"),
    architectureEdge("vite.config.ts", "node:fs", "node:fs"),
    architectureEdge("vite.config.ts", "nanoid", "nanoid"),
    architectureEdge("vite.config.ts", "./src/api", "src/api.ts"),
    architectureEdge("playwright.config.ts", "@vitejs/plugin-react", "@vitejs/plugin-react"),
    architectureEdge("playwright.config.ts", "node:fs", "node:fs"),
    architectureEdge("playwright.config.ts", "nanoid", "nanoid"),
    architectureEdge("playwright.config.ts", "./src/api", "src/api.ts"),
    architectureEdge("toolchain-contract.test.js", "node:crypto", "node:crypto"),
    architectureEdge("toolchain-contract.test.js", "@playwright/test", "@playwright/test"),
    architectureEdge("toolchain-contract.test.js", "./src/api", "src/api.ts"),
    architectureEdge("e2e/fixtures.ts", "node:fs", "node:fs"),
    architectureEdge("e2e/fixtures.ts", "nanoid", "nanoid"),
    architectureEdge("e2e/fixtures.ts", "./src/api", "src/api.ts"),
    architectureEdge("e2e/console-smoke.e2e.ts", "vitest", "vitest"),
    architectureEdge("e2e/console-smoke.e2e.ts", "node:fs", "node:fs"),
    architectureEdge("e2e/console-smoke.e2e.ts", "nanoid", "nanoid"),
    architectureEdge("e2e/console-smoke.e2e.ts", "../src/api", "src/api.ts")
  ])("rejects outside-src dependency mutant $importer -> $specifier", (edge) => {
    expect(() => assertArchitectureEdge(edge)).toThrow(
      "architecture edge denied"
    );
  });

  it("applies the final role, edge, global, demo, and import.meta policy", () => {
    expect(() => assertCurrentArchitecture()).not.toThrow();
  });

  it("freezes the complete Node H physical inventory", () => {
    const inventory = walkFiles(sourceRoot).map((file) =>
      path.relative(sourceRoot, file).replaceAll(path.sep, "/")
    );
    expect(inventory).toEqual(expectedInventory);
    expect(
      inventory.filter((file) => executableExtensions.has(path.extname(file)))
    ).toEqual(inventory.filter((file) => /\.(?:ts|tsx)$/.test(file)));
    expect(relativeInventory(e2eRoot, "e2e")).toEqual(expectedE2eInventory);
    expect(
      rootExecutableFiles(frontendRoot).map((file) =>
        path.relative(frontendRoot, file).replaceAll(path.sep, "/")
      )
    ).toEqual(expectedRootExecutableInventory);
  });

  it.each([
    ".cjs",
    ".cts",
    ".js",
    ".jsx",
    ".mjs",
    ".mts",
    ".svg",
    ".ts",
    ".tsx"
  ])("rejects an extra source owner with %s before import analysis", (extension) => {
    const actual = [...expectedInventory, `legacy_owner${extension}`].sort();
    expect(() => assertInventory(actual, expectedInventory, "src")).toThrow(
      "architecture src inventory mismatch"
    );
  });

  it("rejects a hidden source owner before import analysis", () => {
    expect(() =>
      assertInventory(
        [...expectedInventory, ".hidden/owner.ts"].sort(),
        expectedInventory,
        "src"
      )
    ).toThrow("architecture src inventory mismatch");
  });

  it.each([
    "vite.config.js",
    "vitest.config.js",
    "architecture-bypass.test.js"
  ])("rejects root executable mutant %s before import analysis", (mutant) => {
    const actual = [...expectedRootExecutableInventory, mutant].sort();
    expect(() =>
      assertInventory(actual, expectedRootExecutableInventory, "root executable")
    ).toThrow("architecture root executable inventory mismatch");
  });

  it.each([
    ["main.tsx", "main"],
    ["api.ts", "api"],
    ["vite-env.d.ts", "vite-env"],
    ["app/navigation.ts", "app-navigation"],
    ["app/App.tsx", "app-shell"],
    ["features/research/useResearchWorkspace.ts", "research-io"],
    ["features/strategies/useStrategyManager.ts", "strategy-io"],
    ["features/research/ResearchRoute.tsx", "feature-owner"],
    ["features/research/ResearchPage.tsx", "feature-presentation"],
    ["features/research/gaDomain.ts", "feature-model"],
    ["features/research/gaCharts.ts", "feature-chart"],
    ["features/research/demo.ts", "feature-demo"],
    ["shared/charts/EChart.tsx", "shared-chart"],
    ["shared/format/decimal.ts", "shared-pure"],
    ["shared/theme.ts", "shared-theme"],
    ["shared/i18n.ts", "shared-i18n"],
    ["features/results/resultsModel.test.ts", "source-test"],
    ["architecture.test.ts", "architecture-test"],
    ["shared/locales/resources.test.ts", "locale-resource-test"],
    ["vite.config.ts", "vite-config"],
    ["playwright.config.ts", "playwright-config"],
    ["toolchain-contract.test.js", "toolchain-test"],
    ["e2e/fixtures.ts", "e2e-fixture"],
    ["e2e/console-smoke.e2e.ts", "e2e-test"]
  ] as const)("classifies %s as the exact %s role", (importer, role) => {
    expect(architectureRole(importer)).toBe(role);
  });

  it("rejects every unknown root, app, feature, shared, test, and tooling role", () => {
    for (const importer of [
      "legacy.ts",
      "app/unknown.ts",
      "features/research/unknown.ts",
      "features/unknown/Owner.tsx",
      "shared/unknown.ts",
      "unknown.test.ts",
      "vitest.config.ts",
      "e2e/unknown.ts"
    ]) {
      expect(() => architectureRole(importer), importer).toThrow(
        "architecture role is unknown"
      );
    }
  });

  it.each([
    architectureEdge("main.tsx", "./app/App", "app/App.tsx"),
    architectureEdge("main.tsx", "react", "react"),
    architectureEdge(
      "app/App.tsx",
      "../features/research/ResearchRoute",
      "features/research/ResearchRoute.tsx"
    ),
    architectureEdge(
      "features/research/useResearchWorkspace.ts",
      "../../api",
      "api.ts"
    ),
    architectureEdge(
      "features/strategies/useStrategyManager.ts",
      "../../api",
      "api.ts"
    ),
    architectureEdge(
      "features/results/BacktestResultsView.tsx",
      "./resultsModel",
      "features/results/resultsModel.ts"
    ),
    architectureEdge(
      "features/research/ResearchPage.tsx",
      "../../shared/charts/EChart",
      "shared/charts/EChart.tsx"
    ),
    architectureEdge(
      "features/research/gaDomain.ts",
      "../../api",
      "api.ts",
      "type-only"
    ),
    architectureEdge(
      "features/research/gaCharts.ts",
      "../../shared/theme",
      "shared/theme.ts",
      "type-only"
    ),
    architectureEdge(
      "features/results/demo.ts",
      "./resultsModel",
      "features/results/resultsModel.ts",
      "type-only"
    ),
    architectureEdge(
      "shared/charts/EChart.tsx",
      "../format/decimal",
      "shared/format/decimal.ts"
    ),
    architectureEdge(
      "shared/time/utc.ts",
      "../trading/closedTrade",
      "shared/trading/closedTrade.ts"
    ),
    architectureEdge(
      "shared/i18n.ts",
      "./locales/en",
      "shared/locales/en.ts"
    ),
    architectureEdge(
      "features/results/resultsModel.test.ts",
      "./resultsModel",
      "features/results/resultsModel.ts"
    ),
    architectureEdge("architecture.test.ts", "node:fs", "node:fs"),
    architectureEdge("vite.config.ts", "vite", "vite"),
    architectureEdge(
      "playwright.config.ts",
      "@playwright/test",
      "@playwright/test"
    ),
    architectureEdge(
      "toolchain-contract.test.js",
      "node:child_process",
      "node:child_process"
    ),
    architectureEdge(
      "e2e/console-smoke.e2e.ts",
      "./fixtures",
      "e2e/fixtures.ts"
    )
  ])("accepts a positive edge for $importer -> $specifier", (edge) => {
    expect(() => assertArchitectureEdge(edge)).not.toThrow();
  });

  it.each([
    architectureEdge("main.tsx", "./features/research/gaDomain", "features/research/gaDomain.ts"),
    architectureEdge("api.ts", "./shared/time/utc", "shared/time/utc.ts"),
    architectureEdge("vite-env.d.ts", "react", "react"),
    architectureEdge("app/navigation.ts", "../api", "api.ts"),
    architectureEdge("app/App.tsx", "../features/research/gaDomain", "features/research/gaDomain.ts"),
    architectureEdge("features/research/useResearchWorkspace.ts", "../results/resultsModel", "features/results/resultsModel.ts"),
    architectureEdge("features/strategies/useStrategyManager.ts", "../trades/tradeModel", "features/trades/tradeModel.ts"),
    architectureEdge("features/results/BacktestResultsView.tsx", "../../api", "api.ts"),
    architectureEdge("features/research/ResearchPage.tsx", "../../api", "api.ts", "type-only"),
    architectureEdge("features/research/ResearchPage.tsx", "echarts/core", "echarts/core", "type-only"),
    architectureEdge("features/research/ResearchPage.tsx", "./demo", "features/research/demo.ts"),
    architectureEdge("features/research/gaDomain.ts", "../../api", "api.ts"),
    architectureEdge("features/research/gaCharts.ts", "../../api", "api.ts", "type-only"),
    architectureEdge("features/research/demo.ts", "../../api", "api.ts", "type-only"),
    architectureEdge("shared/charts/EChart.tsx", "../../features/research/gaDomain", "features/research/gaDomain.ts"),
    architectureEdge("shared/charts/EChart.tsx", "../theme", "shared/theme.ts"),
    architectureEdge("shared/format/decimal.ts", "../../features/results/resultsModel", "features/results/resultsModel.ts"),
    architectureEdge("shared/theme.ts", "./time/utc", "shared/time/utc.ts"),
    architectureEdge("shared/i18n.ts", "./theme", "shared/theme.ts"),
    architectureEdge("features/results/resultsModel.test.ts", "../trades/tradeModel", "features/trades/tradeModel.ts"),
    architectureEdge("features/results/resultsModel.test.ts", "react", "react"),
    architectureEdge("architecture.test.ts", "./api", "api.ts"),
    architectureEdge("shared/locales/resources.test.ts", "../../api", "api.ts"),
    architectureEdge("vite.config.ts", "node:fs", "node:fs"),
    architectureEdge("playwright.config.ts", "vite", "vite"),
    architectureEdge("toolchain-contract.test.js", "@playwright/test", "@playwright/test"),
    architectureEdge("e2e/fixtures.ts", "vitest", "vitest"),
    architectureEdge("e2e/console-smoke.e2e.ts", "node:fs", "node:fs")
  ])("rejects the forbidden role edge $importer -> $specifier", (edge) => {
    expect(() => assertArchitectureEdge(edge)).toThrow(
      `architecture edge denied: ${edge.importer} -> ${edge.specifier} (${edge.kind})`
    );
  });

  it.each([
    ["static import", 'import "node:fs";'],
    ["export-from", 'export * from "node:fs";'],
    ["import type", 'type Forbidden = import("node:fs").Forbidden;'],
    ["dynamic import", 'void import("node:fs");'],
    ["require", 'require("node:fs");']
  ])("rejects a forbidden bare edge through %s", (_name, source) => {
    const edges = fixtureEdges(source, "api.ts");
    expect(edges).toHaveLength(1);
    expect(() => assertArchitectureEdge(edges[0])).toThrow(
      "architecture edge denied: api.ts -> node:fs"
    );
  });

  it.each([
    ["static import", 'import "./forbidden";'],
    ["export-from", 'export * from "./forbidden";'],
    ["import type", 'type Forbidden = import("./forbidden").Forbidden;'],
    ["dynamic import", 'void import("./forbidden");'],
    ["require", 'require("./forbidden");']
  ])("rejects a forbidden relative edge through %s", (_name, source) => {
    const edges = fixtureEdges(
      source,
      "app/App.tsx",
      "features/research/gaDomain.ts"
    );
    expect(edges).toHaveLength(1);
    expect(() => assertArchitectureEdge(edges[0])).toThrow(
      "architecture edge denied: app/App.tsx -> ./forbidden"
    );
  });

  it.each([
    architectureEdge("api.ts", "nanoid", "nanoid"),
    architectureEdge("main.tsx", "react/jsx-runtime", "react/jsx-runtime"),
    architectureEdge("features/research/gaCharts.ts", "echarts/charts", "echarts/charts", "type-only"),
    architectureEdge("features/results/resultsModel.ts", "@playwright/test", "@playwright/test", "type-only")
  ])("rejects transitive, subpath, and type-only package laundering", (edge) => {
    expect(() => assertArchitectureEdge(edge)).toThrow(
      "architecture edge denied"
    );
  });

  it("resolves TypeScript source substitutions, extensionless, and index owners", () => {
    const root = fs.mkdtempSync(path.join("/tmp", "fluxtrade-resolution-"));
    try {
      const importer = path.join(root, "owner.ts");
      fs.writeFileSync(importer, "");
      fs.writeFileSync(path.join(root, "direct.ts"), "");
      fs.writeFileSync(path.join(root, "module.ts"), "");
      fs.writeFileSync(path.join(root, "module.js"), "");
      fs.writeFileSync(path.join(root, "component.tsx"), "");
      fs.writeFileSync(path.join(root, "native.mts"), "");
      fs.writeFileSync(path.join(root, "common.cts"), "");
      fs.mkdirSync(path.join(root, "nested"));
      fs.writeFileSync(path.join(root, "nested/index.ts"), "");
      expect(resolveSpecifier(importer, "./direct", root)).toBe("direct.ts");
      expect(resolveSpecifier(importer, "./module.js", root)).toBe(
        "module.ts"
      );
      expect(resolveSpecifier(importer, "./component.jsx", root)).toBe(
        "component.tsx"
      );
      expect(resolveSpecifier(importer, "./native.mjs", root)).toBe(
        "native.mts"
      );
      expect(resolveSpecifier(importer, "./common.cjs", root)).toBe(
        "common.cts"
      );
      expect(resolveSpecifier(importer, "./nested", root)).toBe(
        "nested/index.ts"
      );
      expect(() => resolveSpecifier(importer, "./other.css", root)).toThrow(
        "unresolved architecture import"
      );
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each([
    ["EChart", "echart"],
    ["E_CHART", "echart"],
    ["e-chart", "echart"],
    ["Ä_EChart", "äechart"],
    ["Legacy_Research-Workspace", "legacyresearchworkspace"],
    ["UTC", "utc"]
  ])("normalizes legacy owner %s exactly", (input, expected) => {
    expect(normalizedLegacyToken(input)).toBe(expected);
  });

  it.each(["EChart.tsx", "e_chart.tsx", "E-CHART.tsx"])(
    "rejects legacy root owner variant %s during inventory classification",
    (mutant) => {
      expect(() =>
        assertInventory([...expectedInventory, mutant].sort(), expectedInventory, "src")
      ).toThrow("architecture legacy owner rejected");
    }
  );

  it.each([
    "",
    '/// <reference types="vite/client" />',
    '/// <reference types="vite/client" />\n/// <reference types="node" />\n',
    '/// <reference types="vite/client" />\ndeclare const bypass: boolean;\n',
    'import "vite/client";\n'
  ])("rejects a noncanonical vite-env payload %#", (content) => {
    expect(() => assertViteEnvContract(content)).toThrow(
      "architecture vite-env.d.ts bytes changed"
    );
  });

  it("preserves the exact stylesheet payload and import cascade", async () => {
    const styleRoot = path.join(sourceRoot, "styles");
    expect(fs.readdirSync(styleRoot).sort()).toEqual(
      ["index.css", ...cssPayloadNames].sort()
    );
    const indexBytes = fs.readFileSync(path.join(styleRoot, "index.css"));
    expect(indexBytes.toString("utf8")).toBe(cssIndex);
    await expect(sha256(indexBytes)).resolves.toBe(
      "0c00067b8144bcd4aaefd82ce00f86e0621ba92d78114e75b757e2064cf00e71"
    );

    const fragments = cssPayloadNames.map((name) =>
      fs.readFileSync(path.join(styleRoot, name))
    );
    for (const fragment of fragments) {
      expect(() => assertCompleteCssFragment(fragment)).not.toThrow();
    }
    const payload = new Uint8Array(
      fragments.reduce((length, fragment) => length + fragment.length, 0)
    );
    let offset = 0;
    for (const fragment of fragments) {
      payload.set(fragment, offset);
      offset += fragment.length;
    }
    expect(new TextDecoder().decode(payload)).not.toMatch(
      /@charset|@import|url\(/
    );
    await expect(sha256(payload)).resolves.toBe(
      "9171580172458b2ab20e97024f2ef987996990c50e1492f9d51575a9ba61b44d"
    );
  });

  it.each([
    ["selector", ".owner"],
    ["media", "@media (max-width: 1px) {"],
    ["keyframes", "@keyframes pulse { from { opacity: 0; }"],
    ["comment", "/* incomplete"],
    ["string", '.owner { content: "incomplete']
  ])("rejects a CSS split inside %s", (_name, fragment) => {
    expect(() =>
      assertCompleteCssFragment(new TextEncoder().encode(fragment))
    ).toThrow("CSS fragment is incomplete");
  });

  it("enumerates physical files without ignores or links", () => {
    const fixtureRoot = fs.mkdtempSync(
      path.join("/tmp", "fluxtrade-architecture-inventory-")
    );
    try {
      fs.mkdirSync(path.join(fixtureRoot, ".hidden"));
      fs.writeFileSync(path.join(fixtureRoot, ".hidden", "owner.mjs"), "");
      fs.writeFileSync(path.join(fixtureRoot, "owner.ts"), "");
      expect(
        walkFiles(fixtureRoot).map((file) =>
          path.relative(fixtureRoot, file).replaceAll(path.sep, "/")
        )
      ).toEqual([".hidden/owner.mjs", "owner.ts"]);
      fs.symlinkSync(
        path.join(fixtureRoot, "owner.ts"),
        path.join(fixtureRoot, "bypass.ts")
      );
      expect(() => walkFiles(fixtureRoot)).toThrow(
        "architecture inventory rejects symlink"
      );
    } finally {
      fs.rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  it("rejects selected root symlinks and non-regular executable entries", () => {
    const fixtureRoot = fs.mkdtempSync(
      path.join("/tmp", "fluxtrade-architecture-root-")
    );
    try {
      fs.writeFileSync(path.join(fixtureRoot, "owner.ts"), "");
      fs.symlinkSync(
        path.join(fixtureRoot, "owner.ts"),
        path.join(fixtureRoot, "vite.config.ts")
      );
      expect(() => rootExecutableFiles(fixtureRoot)).toThrow(
        "architecture root executable is not regular"
      );
      fs.rmSync(path.join(fixtureRoot, "vite.config.ts"));
      fs.mkdirSync(path.join(fixtureRoot, "playwright.config.ts"));
      expect(() => rootExecutableFiles(fixtureRoot)).toThrow(
        "architecture root executable is not regular"
      );
    } finally {
      fs.rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });

  it("freezes every final shared repository-relative owner edge", () => {
    const edges = collectImports();
    expect(expectedInventory).not.toEqual(
      expect.arrayContaining([
        "EChart.tsx",
        "EChartCore.tsx",
        "decimal.ts",
        "i18n.ts",
        "styles.css",
        "theme.ts",
        "utc.ts"
      ])
    );
    expect(directRelativeSpecifiers(edges, "main.tsx")).toEqual([
      "./app/App|value|app/App.tsx",
      "./shared/i18n|value|shared/i18n.ts",
      "./shared/theme|value|shared/theme.ts",
      "./styles/index.css|value|styles/index.css"
    ]);
    expect(directRelativeSpecifiers(edges, "app/App.tsx")).toEqual([
      "../features/research/ResearchRoute|value|features/research/ResearchRoute.tsx",
      "../features/results/BacktestResultsView|value|features/results/BacktestResultsView.tsx",
      "../features/strategies/StrategyManager|value|features/strategies/StrategyManager.tsx",
      "../features/trades/TradeChartView|value|features/trades/TradeChartView.tsx",
      "../shared/i18n|type-only|shared/i18n.ts",
      "../shared/theme|value|shared/theme.ts",
      "./navigation|value|app/navigation.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "features/research/gaDomain.ts")).toEqual([
      "../../api|type-only|api.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "features/research/gaCharts.ts")).toEqual([
      "../../shared/theme|type-only|shared/theme.ts",
      "./gaDomain|value|features/research/gaDomain.ts"
    ]);
    expect(
      directRelativeSpecifiers(edges, "features/research/useResearchWorkspace.ts")
    ).toEqual([
      "../../api|value|api.ts",
      "./demo|value|features/research/demo.ts",
      "./gaDomain|value|features/research/gaDomain.ts",
      "./researchModel|value|features/research/researchModel.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "shared/i18n.ts")).toEqual([
      "./locales/en|value|shared/locales/en.ts",
      "./locales/zh-TW|value|shared/locales/zh-TW.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "shared/theme.ts")).toEqual([]);
    expect(directRelativeSpecifiers(edges, "shared/format/decimal.ts")).toEqual(
      []
    );
    expect(directRelativeSpecifiers(edges, "shared/time/utc.ts")).toEqual([]);
  });

  it("detects static, type-only, dynamic, require, and nonliteral edges", () => {
    expect(
      withCompilerFixture(
        [
          'import value from "fixture-static";',
          'import type { Epoch } from "fixture-type";',
          'export { fixture } from "fixture-export";',
          'export { type Fixture } from "fixture-export-specifier-type";',
          'export type { Other } from "fixture-export-type";',
          'type Imported = import("fixture-import-type").Imported;',
          'void import("fixture-dynamic");',
          'require("fixture-require");'
        ].join("\n"),
        (fixture, checker) =>
          collectImportsFromSource(fixture, fixture.fileName, checker).map(
            ({ kind, specifier }) => `${specifier}|${kind}`
          )
      )
    ).toEqual([
      "fixture-static|value",
      "fixture-type|type-only",
      "fixture-export|value",
      "fixture-export-specifier-type|type-only",
      "fixture-export-type|type-only",
      "fixture-import-type|type-only",
      "fixture-dynamic|value",
      "fixture-require|value"
    ]);
    expect(() =>
      withCompilerFixture("void import(target);", (fixture, checker) =>
        collectImportsFromSource(fixture, fixture.fileName, checker)
      )
    ).toThrow("architecture import specifiers must be string literals");
    expect(() =>
      withCompilerFixture("require(target);", (fixture, checker) =>
        collectImportsFromSource(fixture, fixture.fileName, checker)
      )
    ).toThrow("architecture import specifiers must be string literals");
    expect(
      withCompilerFixture(
        [
          "export {};",
          "const require = (value: string) => value;",
          'require("shadowed-require");'
        ].join("\n"),
        (fixture, checker) =>
          collectImportsFromSource(fixture, fixture.fileName, checker)
      )
    ).toEqual([]);
  });

  it("accepts only the exact App import.meta.env.DEV invocation", () => {
    expect(() =>
      withCompilerFixture(
        [
          "declare function parseDemoMode(url: URL, dev: boolean): boolean;",
          "export function App() {",
          "  const demoMode = parseDemoMode(new URL(window.location.href), import.meta.env.DEV);",
          "  return demoMode;",
          "}"
        ].join("\n"),
        (fixture) => assertImportMetaContract(fixture, "app/App.tsx")
      )
    ).not.toThrow();
  });

  it.each([
    ["glob", 'void import.meta.glob("./*.ts");'],
    ["glob eager false", 'void import.meta.glob("./*.ts", { eager: false });'],
    ["glob eager true", 'void import.meta.glob("./*.ts", { eager: true });'],
    ["globEager", 'void import.meta.globEager("./*.ts");'],
    ["url", "void import.meta.url;"],
    ["hot", "void import.meta.hot;"],
    ["computed env", 'void parseDemoMode(new URL(window.location.href), import.meta.env["DEV"]);'],
    ["meta alias", "const meta = import.meta;"],
    ["env destructure", "const { DEV } = import.meta.env;"],
    ["detached value", "const dev = import.meta.env.DEV;"],
    ["alternate result", "const alternate = parseDemoMode(new URL(window.location.href), import.meta.env.DEV);"],
    ["different URL", "void parseDemoMode(new URL('/other', window.location.href), import.meta.env.DEV);"],
    ["swapped arguments", "void parseDemoMode(import.meta.env.DEV, new URL(window.location.href));"]
  ])("rejects import.meta mutant %s", (_name, statement) => {
    expect(() =>
      withCompilerFixture(statement, (fixture) =>
        assertImportMetaContract(fixture, "app/App.tsx")
      )
    ).toThrow(/architecture import\.meta/u);
  });

  it("rejects import.meta.env outside App", () => {
    expect(() =>
      withCompilerFixture("void import.meta.env.DEV;", (fixture) =>
        assertImportMetaContract(fixture, "features/research/ResearchPage.tsx")
      )
    ).toThrow("architecture import.meta denied");
  });

  it("freezes parseDemoMode and App-to-facade boolean propagation", () => {
    const navigation = compilerProject!.program.getSourceFile(
      path.join(sourceRoot, "app/navigation.ts")
    );
    const app = compilerProject!.program.getSourceFile(
      path.join(sourceRoot, "app/App.tsx")
    );
    if (!navigation || !app) {
      throw new Error("demo architecture owners are unavailable");
    }
    expect(() => assertParseDemoModeContract(navigation)).not.toThrow();
    expect(() =>
      assertAppDemoPropagation(app, compilerProject!.checker)
    ).not.toThrow();
    expect(() =>
      withCompilerFixture(
        [
          "export function parseDemoMode(currentUrl: URL, dev: boolean): boolean {",
          '  return dev && currentUrl.searchParams.get("demo") === "1";',
          "}"
        ].join("\n"),
        (fixture) => assertParseDemoModeContract(fixture)
      )
    ).toThrow("architecture parseDemoMode contract changed");
    expect(() =>
      withCompilerFixture(
        [
          "declare function ResearchRoute(props: unknown): unknown;",
          "declare function BacktestResultsView(props: unknown): unknown;",
          "declare function TradeChartView(props: unknown): unknown;",
          "export function App() {",
          "  const demoMode = true;",
          "  return <><ResearchRoute demoMode={demoMode} />",
          "    <BacktestResultsView demoMode={false} />",
          "    <TradeChartView demoMode={demoMode} /></>;",
          "}"
        ].join("\n"),
        (fixture, checker) => assertAppDemoPropagation(fixture, checker),
        ".tsx"
      )
    ).toThrow("architecture demo propagation changed: BacktestResultsView");
  });

  it.each([
    [
      "mutable owner",
      [
        "declare function parseDemoMode(url: URL, dev: boolean): boolean;",
        "export function App() {",
        "  let demoMode = parseDemoMode(new URL(window.location.href), import.meta.env.DEV);",
        "  return demoMode;",
        "}"
      ].join("\n")
    ],
    [
      "owner outside App",
      [
        "declare function parseDemoMode(url: URL, dev: boolean): boolean;",
        "const demoMode = parseDemoMode(new URL(window.location.href), import.meta.env.DEV);",
        "export function App() { return demoMode; }"
      ].join("\n")
    ]
  ])("rejects import.meta demo ownership mutant %s", (_name, source) => {
    expect(() =>
      withCompilerFixture(source, (fixture) =>
        assertImportMetaContract(fixture, "app/App.tsx")
      )
    ).toThrow(/architecture import\.meta/u);
  });

  it("rejects a shadowed App demoMode propagation symbol", () => {
    expect(() =>
      withCompilerFixture(
        [
          "declare function ResearchRoute(props: unknown): unknown;",
          "declare function BacktestResultsView(props: unknown): unknown;",
          "declare function TradeChartView(props: unknown): unknown;",
          "export function App() {",
          "  const demoMode = false;",
          "  const content = (() => {",
          "    const demoMode = true;",
          "    return <ResearchRoute demoMode={demoMode} />;",
          "  })();",
          "  return <>{content}<BacktestResultsView demoMode={demoMode} />",
          "    <TradeChartView demoMode={demoMode} /></>;",
          "}"
        ].join("\n"),
        (fixture, checker) => assertAppDemoPropagation(fixture, checker),
        ".tsx"
      )
    ).toThrow("architecture App requires one local demoMode owner");
  });

  it("freezes exact unshadowed production policy-global uses", () => {
    expect(
      collectPolicyGlobalUses().map(
        ({ importer, access }) => `${importer}|${access}`
      )
    ).toEqual(expectedPolicyGlobalLedger);
    for (const use of collectPolicyGlobalUses()) {
      expect(() => assertPolicyGlobalUse(use)).not.toThrow();
    }

    const appPath = path.join(sourceRoot, "app/App.tsx");
    const app = compilerProject!.program.getSourceFile(appPath);
    if (!app) {
      throw new Error("App.tsx is unavailable to the architecture compiler");
    }
    const importMetaUses = descendants(app).filter(
      (node) =>
        isMetaProperty(node) &&
        node.keywordToken === SyntaxKind.ImportKeyword &&
        node.name.text === "meta"
    );
    expect(importMetaUses).toHaveLength(1);
    const { access, outer } = maximalPolicyAccess(importMetaUses[0]);
    expect(access).toBe("import.meta.env.DEV");
    expect(isCallExpression(outer.parent)).toBe(true);
    if (!isCallExpression(outer.parent)) {
      throw new Error("import.meta.env.DEV is detached from parseDemoMode");
    }
    expect(outer.parent.arguments[1]).toBe(outer);
    expect(
      isIdentifier(outer.parent.expression) &&
        outer.parent.expression.text === "parseDemoMode"
    ).toBe(true);
  });

  it.each([
    ["bare fetch alias", "api.ts", "const request = fetch; void request;"],
    ["window destructure", "features/strategies/useStrategyManager.ts", "const { confirm } = window; void confirm;"],
    ["globalThis network", "api.ts", 'void globalThis.fetch("/forbidden");'],
    ["feature query read", "features/research/ResearchPage.tsx", "void window.location.search;"],
    ["bare location", "app/App.tsx", "void location.href;"]
  ])("rejects policy-global laundering via %s", (_name, importer, source) => {
    const uses = withCompilerFixture(source, (fixture, checker) =>
      collectPolicyGlobalUsesFromSource(fixture, checker, importer)
    );
    expect(uses.length).toBeGreaterThan(0);
    expect(() => uses.forEach(assertPolicyGlobalUse)).toThrow(
      "architecture policy global denied"
    );
  });

  it.each([
    ["confirm", "void confirm('x');"],
    ["crypto", "void crypto.randomUUID();"],
    ["devicePixelRatio", "void devicePixelRatio;"],
    ["document", "void document.body;"],
    ["fetch", "void fetch('/x');"],
    ["frames", "void frames;"],
    ["globalThis", "void globalThis.fetch('/x');"],
    ["history", "void history.state;"],
    ["localStorage", "void localStorage.getItem('x');"],
    ["location", "void location.href;"],
    ["matchMedia", "void matchMedia('(min-width:1px)');"],
    ["navigator", "void navigator.language;"],
    ["opener", "void opener;"],
    ["parent", "void parent;"],
    ["self", "void self.fetch('/x');"],
    ["sessionStorage", "void sessionStorage.getItem('x');"],
    ["top", "void top;"],
    ["window", "void window.location.href;"]
  ])("rejects forbidden policy global %s", (_name, source) => {
    const uses = withCompilerFixture(source, (fixture, checker) =>
      collectPolicyGlobalUsesFromSource(
        fixture,
        checker,
        "features/results/resultsModel.ts"
      )
    );
    expect(uses.length).toBeGreaterThan(0);
    expect(() => uses.forEach(assertPolicyGlobalUse)).toThrow(
      "architecture policy global denied"
    );
  });

  it("uses compiler symbols to reject global laundering without flagging shadows", () => {
    expect(
      withCompilerFixture(
        [
          "export {};",
          "const window = { location: { href: 'local' } };",
          "const fetch = () => Promise.resolve();",
          "const navigator = { language: 'local' };",
          "void window.location.href;",
          "void fetch();",
          "void navigator.language;"
        ].join("\n"),
        (fixture, checker) =>
          collectPolicyGlobalUsesFromSource(fixture, checker).map(
            ({ access }) => access
          )
      )
    ).toEqual([]);
    expect(
      withCompilerFixture(
        [
          'void window.fetch("/forbidden");',
          "void document.body;",
          "void import.meta.env.PROD;",
          'void globalThis.fetch("/forbidden");',
          'void self.fetch("/forbidden");',
          "void history.replaceState(null, '', '/');",
          "void location.href;",
          "void confirm('forbidden');",
          "void matchMedia('(min-width: 1px)');",
          "void devicePixelRatio;"
        ].join("\n"),
        (fixture, checker) =>
          collectPolicyGlobalUsesFromSource(fixture, checker).map(
            ({ access }) => access
          )
      )
    ).toEqual([
      "window.fetch()",
      "document.body",
      "import.meta.env.PROD",
      "globalThis.fetch()",
      "self.fetch()",
      "history.replaceState()",
      "location.href",
      "confirm()",
      "matchMedia()",
      "devicePixelRatio"
    ]);
  });

  it("keeps the final research owner behind one shell facade", () => {
    const source = (relativePath: string) => {
      const file = compilerProject!.program.getSourceFile(
        path.join(sourceRoot, relativePath)
      );
      if (!file) {
        throw new Error(`${relativePath} is unavailable to the architecture compiler`);
      }
      return file;
    };
    const appSource = source("app/App.tsx");
    const routeSource = source("features/research/ResearchRoute.tsx");
    const workspaceSource = source("features/research/useResearchWorkspace.ts");
    const pageSource = source("features/research/ResearchPage.tsx");
    const app = findNamedFunction(appSource, "App");
    const route = findNamedFunction(routeSource, "ResearchRoute");
    const workspace = findNamedFunction(
      workspaceSource,
      "useResearchWorkspace"
    );
    expect(app).toHaveLength(1);
    expect(route).toHaveLength(1);
    expect(workspace).toHaveLength(1);

    const researchStateNames = [
      "epochs",
      "setEpochs",
      "epochId",
      "setEpochId",
      "summaries",
      "setSummaries",
      "generationIndex",
      "setGenerationIndex",
      "genes",
      "setGenes",
      "selectedGeneId",
      "setSelectedGeneId",
      "xParameter",
      "setXParameter",
      "yParameter",
      "setYParameter",
      "surfaceMode",
      "setSurfaceMode",
      "loading",
      "setLoading",
      "failure",
      "setFailure",
      "epochsLoaded",
      "setEpochsLoaded",
      "reloadToken",
      "setReloadToken"
    ];
    const appNames = declaredNames(app[0]);
    for (const name of researchStateNames) {
      expect(appNames).not.toContain(name);
    }
    expect(
      descendants(app[0]).filter(
        (node) =>
          isJsxOpeningElement(node) &&
          isIdentifier(node.tagName) &&
          node.tagName.text === "ResearchRoute"
      )
    ).toHaveLength(1);

    const workspaceNames = declaredNames(workspace[0]);
    for (const name of researchStateNames) {
      expect(workspaceNames.filter((candidate) => candidate === name)).toHaveLength(
        1
      );
    }
    const statePairs = descendants(workspace[0]).flatMap((node) => {
      if (
        !isVariableDeclaration(node) ||
        !isArrayBindingPattern(node.name) ||
        !node.initializer ||
        !isCallExpression(node.initializer) ||
        !isIdentifier(node.initializer.expression) ||
        node.initializer.expression.text !== "useState"
      ) {
        return [];
      }
      return [
        node.name.elements
          .flatMap((element) =>
            isBindingElement(element) &&
            element.name !== undefined &&
            isIdentifier(element.name)
              ? [element.name.text]
              : []
          )
          .join("/")
      ];
    });
    expect(statePairs).toEqual([
      "epochs/setEpochs",
      "epochId/setEpochId",
      "summaries/setSummaries",
      "generationIndex/setGenerationIndex",
      "genes/setGenes",
      "selectedGeneId/setSelectedGeneId",
      "xParameter/setXParameter",
      "yParameter/setYParameter",
      "surfaceMode/setSurfaceMode",
      "loading/setLoading",
      "failure/setFailure",
      "epochsLoaded/setEpochsLoaded",
      "reloadToken/setReloadToken"
    ]);
    const workspaceCalls = descendants(workspace[0]).filter(isCallExpression);
    expect(
      workspaceCalls.filter(
        (node) =>
          isIdentifier(node.expression) && node.expression.text === "useEffect"
      )
    ).toHaveLength(4);
    expect(
      workspaceCalls.filter(
        (node) =>
          isIdentifier(node.expression) && node.expression.text === "useMemo"
      )
    ).toHaveLength(3);

    expect(
      descendants(route[0]).filter(
        (node) =>
          isCallExpression(node) &&
          isIdentifier(node.expression) &&
          node.expression.text === "useResearchWorkspace"
      )
    ).toHaveLength(1);
    expect(
      descendants(route[0]).filter(
        (node) =>
          (isJsxOpeningElement(node) || isJsxSelfClosingElement(node)) &&
          isIdentifier(node.tagName) &&
          node.tagName.text === "ResearchPage"
      )
    ).toHaveLength(1);
    const researchDynamicImports = descendants(pageSource).flatMap((node) => {
      if (
        !isCallExpression(node) ||
        node.expression.kind !== SyntaxKind.ImportKeyword ||
        node.arguments.length !== 1 ||
        !isStringLiteral(node.arguments[0])
      ) {
        return [];
      }
      return [node.arguments[0].text];
    });
    expect(researchDynamicImports).toEqual([
      "../../shared/charts/EChart",
      "./FitnessSurface3D"
    ]);

    const chooseViewDeclaration = descendants(app[0]).find(
      (node) =>
        isVariableDeclaration(node) &&
        isIdentifier(node.name) &&
        node.name.text === "chooseView"
    );
    if (!chooseViewDeclaration) {
      throw new Error("App chooseView declaration is missing");
    }
    const chooseViewSource = chooseViewDeclaration.getText(appSource);
    const orderedOperations = [
      "setResearchActivated(true)",
      "serializeNavigation(",
      "setInspectedTradeId(navigation.inspectedTradeId)",
      'window.history.replaceState(null, "", navigation.relativeUrl)',
      "setView(nextView)"
    ];
    const indexes = orderedOperations.map((operation) =>
      chooseViewSource.indexOf(operation)
    );
    expect(indexes.every((index) => index >= 0)).toBe(true);
    expect(indexes).toEqual([...indexes].sort((left, right) => left - right));
  });

  it("keeps the final results owner behind one shell facade", () => {
    const source = (relativePath: string) => {
      const file = compilerProject!.program.getSourceFile(
        path.join(sourceRoot, relativePath)
      );
      if (!file) {
        throw new Error(
          `${relativePath} is unavailable to the architecture compiler`
        );
      }
      return file;
    };
    const appSource = source("app/App.tsx");
    const facadeSource = source("features/results/BacktestResultsView.tsx");
    const paginationSource = source("features/results/useTradePagination.ts");
    const modelSource = source("features/results/resultsModel.ts");
    const chartsSource = source("features/results/resultsCharts.ts");
    const app = findNamedFunction(appSource, "App");
    const facade = findNamedFunction(facadeSource, "BacktestResultsView");
    const pagination = findNamedFunction(
      paginationSource,
      "useTradePagination"
    );
    expect(app).toHaveLength(1);
    expect(facade).toHaveLength(1);
    expect(pagination).toHaveLength(1);

    const resultStateNames = [
      "tradePage",
      "setTradePage",
      "tradePageLoading",
      "setTradePageLoading",
      "tradePageError",
      "setTradePageError",
      "requestGeneration",
      "requestInFlight"
    ];
    const appNames = declaredNames(app[0]);
    for (const name of resultStateNames) {
      expect(appNames).not.toContain(name);
    }
    expect(
      descendants(app[0]).filter(
        (node) =>
          (isJsxOpeningElement(node) || isJsxSelfClosingElement(node)) &&
          isIdentifier(node.tagName) &&
          node.tagName.text === "BacktestResultsView"
      )
    ).toHaveLength(1);
    expect(
      descendants(facade[0]).filter(
        (node) =>
          isCallExpression(node) &&
          isIdentifier(node.expression) &&
          node.expression.text === "useTradePagination"
      )
    ).toHaveLength(1);

    const paginationNames = declaredNames(pagination[0]);
    for (const name of resultStateNames) {
      expect(
        paginationNames.filter((candidate) => candidate === name)
      ).toHaveLength(1);
    }
    const paginationCalls = descendants(pagination[0]).filter(isCallExpression);
    expect(
      paginationCalls.filter(
        (node) =>
          isIdentifier(node.expression) && node.expression.text === "useState"
      )
    ).toHaveLength(3);
    expect(
      paginationCalls.filter(
        (node) =>
          isIdentifier(node.expression) && node.expression.text === "useRef"
      )
    ).toHaveLength(2);
    expect(
      paginationCalls.filter(
        (node) =>
          isIdentifier(node.expression) && node.expression.text === "useEffect"
      )
    ).toHaveLength(1);

    expect(modelSource.getText()).not.toMatch(/\b(?:React|ECharts|fetch)\b/);
    expect(chartsSource.getText()).not.toMatch(
      /\b(?:React|fetch|localStorage|sessionStorage)\b/
    );
  });

  it("keeps the final trades owner behind one shell facade", () => {
    const source = (relativePath: string) => {
      const file = compilerProject!.program.getSourceFile(
        path.join(sourceRoot, relativePath)
      );
      if (!file) {
        throw new Error(
          `${relativePath} is unavailable to the architecture compiler`
        );
      }
      return file;
    };
    const edges = collectImports();
    const facadeSource = source("features/trades/TradeChartView.tsx");
    const modelSource = source("features/trades/tradeModel.ts");
    const chartsSource = source("features/trades/tradeCharts.ts");

    expect(findNamedFunction(facadeSource, "TradeChartView")).toHaveLength(1);
    expect(findNamedFunction(modelSource, "buildTradeChartModel")).toHaveLength(
      1
    );
    expect(findNamedFunction(chartsSource, "tradeChartOption")).toHaveLength(1);
    expect(
      directRelativeSpecifiers(edges, "features/trades/tradeModel.ts")
    ).toEqual([
      "../../shared/format/decimal|value|shared/format/decimal.ts",
      "../../shared/time/utc|value|shared/time/utc.ts",
      "../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts"
    ]);
    expect(
      directRelativeSpecifiers(edges, "features/trades/tradeCharts.ts")
    ).toEqual([
      "../../shared/theme|type-only|shared/theme.ts",
      "./tradeModel|type-only|features/trades/tradeModel.ts"
    ]);
    expect(
      directRelativeSpecifiers(edges, "features/trades/CandlestickChart.tsx")
    ).toEqual(["../../shared/charts/EChartCore|value|shared/charts/EChartCore.tsx"]);
    expect(
      directRelativeSpecifiers(edges, "features/trades/TradeChartView.tsx")
    ).toEqual([
      "../../shared/format/decimal|value|shared/format/decimal.ts",
      "../../shared/i18n|type-only|shared/i18n.ts",
      "../../shared/theme|type-only|shared/theme.ts",
      "../../shared/time/utc|value|shared/time/utc.ts",
      "./CandlestickChart|value|features/trades/CandlestickChart.tsx",
      "./demo|value|features/trades/demo.ts",
      "./tradeCharts|value|features/trades/tradeCharts.ts",
      "./tradeModel|value|features/trades/tradeModel.ts"
    ]);

    expect(modelSource.getText()).not.toMatch(
      /\b(?:React|ECharts|fetch|localStorage|sessionStorage)\b/
    );
    expect(chartsSource.getText()).not.toMatch(
      /\b(?:React|fetch|localStorage|sessionStorage|ClosedTrade)\b/
    );
    const dynamicImports = descendants(facadeSource).flatMap((node) => {
      if (
        !isCallExpression(node) ||
        node.expression.kind !== SyntaxKind.ImportKeyword ||
        node.arguments.length !== 1 ||
        !isStringLiteral(node.arguments[0])
      ) {
        return [];
      }
      return [node.arguments[0].text];
    });
    expect(dynamicImports).toEqual(["./CandlestickChart"]);
  });

  it("keeps the final strategy owner behind one pure state and one I/O owner", () => {
    const source = (relativePath: string) => {
      const file = compilerProject!.program.getSourceFile(
        path.join(sourceRoot, relativePath)
      );
      if (!file) {
        throw new Error(
          `${relativePath} is unavailable to the architecture compiler`
        );
      }
      return file;
    };
    const edges = collectImports();
    const facadeSource = source("features/strategies/StrategyManager.tsx");
    const hookSource = source("features/strategies/useStrategyManager.ts");
    const stateSource = source("features/strategies/strategyCommandState.ts");
    const viewSource = source("features/strategies/StrategyManagerView.tsx");

    expect(findNamedFunction(facadeSource, "StrategyManager")).toHaveLength(1);
    expect(findNamedFunction(hookSource, "useStrategyManager")).toHaveLength(1);
    expect(
      findNamedFunction(stateSource, "transitionStrategyCommandState")
    ).toHaveLength(1);
    expect(
      findNamedFunction(viewSource, "StrategyManagerView")
    ).toHaveLength(1);
    expect(
      directRelativeSpecifiers(edges, "features/strategies/StrategyManager.tsx")
    ).toEqual([
      "../../shared/i18n|type-only|shared/i18n.ts",
      "./StrategyManagerView|value|features/strategies/StrategyManagerView.tsx",
      "./useStrategyManager|value|features/strategies/useStrategyManager.ts"
    ]);
    expect(
      directRelativeSpecifiers(
        edges,
        "features/strategies/useStrategyManager.ts"
      )
    ).toEqual([
      "../../api|value|api.ts",
      "./strategyCommandState|value|features/strategies/strategyCommandState.ts"
    ]);
    expect(
      directRelativeSpecifiers(
        edges,
        "features/strategies/strategyCommandState.ts"
      )
    ).toEqual(["../../api|type-only|api.ts"]);
    expect(
      directRelativeSpecifiers(
        edges,
        "features/strategies/StrategyManagerView.tsx"
      )
    ).toEqual([
      "../../shared/i18n|type-only|shared/i18n.ts",
      "./strategyCommandState|type-only|features/strategies/strategyCommandState.ts"
    ]);
    expect(stateSource.getText()).not.toMatch(
      /\b(?:React|window|document|fetch|localStorage|sessionStorage|crypto|navigator)\b/
    );
    expect(viewSource.getText()).not.toMatch(
      /\b(?:window|document|fetch|localStorage|sessionStorage|crypto|navigator)\b/
    );
    expect(hookSource.getText()).toMatch(/window\.confirm/);
    expect(hookSource.getText()).toMatch(/window\.crypto\.randomUUID/);
    expect(hookSource.getText()).toMatch(/window\.sessionStorage/);
  });

  it("keeps navigation pure", () => {
    const navigation = fs.readFileSync(
      path.join(sourceRoot, "app/navigation.ts"),
      "utf8"
    );
    expect(navigation).not.toMatch(
      /\b(?:window|document|fetch|localStorage|sessionStorage|crypto|navigator)\b/
    );
  });
});
