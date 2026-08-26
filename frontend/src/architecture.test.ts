import fs from "node:fs";
import path from "node:path";
import {
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
  isJsxOpeningElement,
  isJsxSelfClosingElement,
  isLiteralTypeNode,
  isMetaProperty,
  isNamedImports,
  isNamedExports,
  isNoSubstitutionTemplateLiteral,
  isPropertyAccessExpression,
  isStringLiteral,
  isVariableDeclaration
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

const sourceRoot = path.resolve(process.cwd(), "src");
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
  "CandlestickChart.tsx",
  "EChart.test.tsx",
  "EChart.tsx",
  "EChartCore.tsx",
  "StrategyManager.test.tsx",
  "StrategyManager.tsx",
  "TradeChartView.test.tsx",
  "TradeChartView.tsx",
  "api.test.ts",
  "api.ts",
  "app/App.test.tsx",
  "app/App.tsx",
  "app/navigation.test.ts",
  "app/navigation.ts",
  "architecture.test.ts",
  "decimal.ts",
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
  "i18n.ts",
  "main.tsx",
  "shared/trading/closedTrade.ts",
  "styles.css",
  "theme.ts",
  "tradeChart.test.ts",
  "tradeChart.ts",
  "tradeDemo.ts",
  "utc.ts",
  "vite-env.d.ts"
] as const;

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
  "CandlestickChart.tsx|echarts/charts|value",
  "CandlestickChart.tsx|echarts/components|value",
  "CandlestickChart.tsx|echarts/core|value",
  "EChart.test.tsx|@testing-library/react|value",
  "EChart.test.tsx|vitest|value",
  "EChart.tsx|echarts/charts|value",
  "EChart.tsx|echarts/components|value",
  "EChart.tsx|echarts/core|value",
  "EChartCore.tsx|echarts/core|type-only",
  "EChartCore.tsx|echarts/core|value",
  "EChartCore.tsx|echarts/renderers|value",
  "EChartCore.tsx|react|value",
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
  "features/research/ResearchPage.tsx|echarts/core|type-only",
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
  "i18n.ts|i18next|value",
  "i18n.ts|react-i18next|value",
  "main.tsx|react-dom/client|value",
  "main.tsx|react|value",
  "StrategyManager.test.tsx|@testing-library/react|value",
  "StrategyManager.test.tsx|vitest|value",
  "StrategyManager.tsx|react-i18next|value",
  "StrategyManager.tsx|react|value",
  "tradeChart.test.ts|vitest|value",
  "tradeChart.ts|echarts/core|type-only",
  "TradeChartView.test.tsx|@testing-library/react|value",
  "TradeChartView.test.tsx|vitest|value",
  "TradeChartView.tsx|react-i18next|value",
  "TradeChartView.tsx|react|value"
] as const;

const expectedRelativeImportLedger = [
  "api.test.ts|./api|value|api.ts",
  "app/App.test.tsx|../api|type-only|api.ts",
  "app/App.test.tsx|../api|type-only|api.ts",
  "app/App.test.tsx|../i18n|value|i18n.ts",
  "app/App.test.tsx|./App|value|app/App.tsx",
  "app/App.tsx|../features/research/ResearchRoute|value|features/research/ResearchRoute.tsx",
  "app/App.tsx|../features/results/BacktestResultsView|value|features/results/BacktestResultsView.tsx",
  "app/App.tsx|../i18n|type-only|i18n.ts",
  "app/App.tsx|../StrategyManager|value|StrategyManager.tsx",
  "app/App.tsx|../theme|value|theme.ts",
  "app/App.tsx|../TradeChartView|value|TradeChartView.tsx",
  "app/App.tsx|./navigation|value|app/navigation.ts",
  "app/navigation.test.ts|./navigation|value|app/navigation.ts",
  "CandlestickChart.tsx|./EChartCore|value|EChartCore.tsx",
  "EChart.test.tsx|./EChart|value|EChart.tsx",
  "EChart.tsx|./EChartCore|value|EChartCore.tsx",
  "features/research/demo.test.ts|./demo|value|features/research/demo.ts",
  "features/research/demo.ts|../../api|type-only|api.ts",
  "features/research/FitnessSurface3D.test.tsx|./FitnessSurface3D|value|features/research/FitnessSurface3D.tsx",
  "features/research/FitnessSurface3D.test.tsx|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/FitnessSurface3D.tsx|../../theme|type-only|theme.ts",
  "features/research/FitnessSurface3D.tsx|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/gaCharts.test.ts|../../api|type-only|api.ts",
  "features/research/gaCharts.test.ts|./demo|value|features/research/demo.ts",
  "features/research/gaCharts.test.ts|./gaCharts|value|features/research/gaCharts.ts",
  "features/research/gaCharts.test.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/gaCharts.ts|../../api|type-only|api.ts",
  "features/research/gaCharts.ts|../../theme|type-only|theme.ts",
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
  "features/research/ResearchPage.test.tsx|../../i18n|value|i18n.ts",
  "features/research/ResearchPage.test.tsx|./researchModel|value|features/research/researchModel.ts",
  "features/research/ResearchPage.test.tsx|./ResearchPage|value|features/research/ResearchPage.tsx",
  "features/research/ResearchPage.test.tsx|./useResearchWorkspace|type-only|features/research/useResearchWorkspace.ts",
  "features/research/ResearchPage.tsx|../../api|value|api.ts",
  "features/research/ResearchPage.tsx|../../EChart|value|EChart.tsx",
  "features/research/ResearchPage.tsx|../../theme|type-only|theme.ts",
  "features/research/ResearchPage.tsx|./FitnessSurface3D|value|features/research/FitnessSurface3D.tsx",
  "features/research/ResearchPage.tsx|./gaCharts|type-only|features/research/gaCharts.ts",
  "features/research/ResearchPage.tsx|./gaDomain|type-only|features/research/gaDomain.ts",
  "features/research/ResearchPage.tsx|./researchModel|value|features/research/researchModel.ts",
  "features/research/ResearchPage.tsx|./useResearchWorkspace|type-only|features/research/useResearchWorkspace.ts",
  "features/research/ResearchRoute.test.tsx|../../api|type-only|api.ts",
  "features/research/ResearchRoute.test.tsx|../../api|type-only|api.ts",
  "features/research/ResearchRoute.test.tsx|../../i18n|value|i18n.ts",
  "features/research/ResearchRoute.test.tsx|./ResearchRoute|value|features/research/ResearchRoute.tsx",
  "features/research/ResearchRoute.tsx|../../i18n|type-only|i18n.ts",
  "features/research/ResearchRoute.tsx|../../theme|type-only|theme.ts",
  "features/research/ResearchRoute.tsx|./gaCharts|value|features/research/gaCharts.ts",
  "features/research/ResearchRoute.tsx|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/ResearchRoute.tsx|./ResearchPage|value|features/research/ResearchPage.tsx",
  "features/research/ResearchRoute.tsx|./useResearchWorkspace|value|features/research/useResearchWorkspace.ts",
  "features/research/useResearchWorkspace.test.ts|../../api|type-only|api.ts",
  "features/research/useResearchWorkspace.test.ts|../../api|type-only|api.ts",
  "features/research/useResearchWorkspace.test.ts|./useResearchWorkspace|value|features/research/useResearchWorkspace.ts",
  "features/research/useResearchWorkspace.ts|../../api|value|api.ts",
  "features/research/useResearchWorkspace.ts|./demo|value|features/research/demo.ts",
  "features/research/useResearchWorkspace.ts|./gaDomain|value|features/research/gaDomain.ts",
  "features/research/useResearchWorkspace.ts|./researchModel|value|features/research/researchModel.ts",
  "features/results/BacktestResultsView.test.tsx|../../i18n|value|i18n.ts",
  "features/results/BacktestResultsView.test.tsx|./BacktestResultsView|value|features/results/BacktestResultsView.tsx",
  "features/results/BacktestResultsView.test.tsx|./demo|value|features/results/demo.ts",
  "features/results/BacktestResultsView.test.tsx|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/BacktestResultsView.tsx|../../decimal|value|decimal.ts",
  "features/results/BacktestResultsView.tsx|../../EChart|value|EChart.tsx",
  "features/results/BacktestResultsView.tsx|../../i18n|type-only|i18n.ts",
  "features/results/BacktestResultsView.tsx|../../theme|type-only|theme.ts",
  "features/results/BacktestResultsView.tsx|../../utc|value|utc.ts",
  "features/results/BacktestResultsView.tsx|./demo|value|features/results/demo.ts",
  "features/results/BacktestResultsView.tsx|./resultsCharts|value|features/results/resultsCharts.ts",
  "features/results/BacktestResultsView.tsx|./resultsModel|value|features/results/resultsModel.ts",
  "features/results/BacktestResultsView.tsx|./useTradePagination|value|features/results/useTradePagination.ts",
  "features/results/demo.test.ts|./demo|value|features/results/demo.ts",
  "features/results/demo.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/results/demo.ts|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/resultsCharts.test.ts|./demo|value|features/results/demo.ts",
  "features/results/resultsCharts.test.ts|./resultsCharts|value|features/results/resultsCharts.ts",
  "features/results/resultsCharts.ts|../../decimal|value|decimal.ts",
  "features/results/resultsCharts.ts|../../i18n|type-only|i18n.ts",
  "features/results/resultsCharts.ts|../../theme|type-only|theme.ts",
  "features/results/resultsCharts.ts|../../utc|value|utc.ts",
  "features/results/resultsCharts.ts|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/resultsModel.test.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/results/resultsModel.test.ts|./resultsModel|value|features/results/resultsModel.ts",
  "features/results/resultsModel.ts|../../decimal|value|decimal.ts",
  "features/results/resultsModel.ts|../../shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "features/results/resultsModel.ts|../../utc|value|utc.ts",
  "features/results/useTradePagination.test.ts|./demo|value|features/results/demo.ts",
  "features/results/useTradePagination.test.ts|./resultsModel|type-only|features/results/resultsModel.ts",
  "features/results/useTradePagination.test.ts|./useTradePagination|value|features/results/useTradePagination.ts",
  "features/results/useTradePagination.ts|./resultsModel|value|features/results/resultsModel.ts",
  "main.tsx|./app/App|value|app/App.tsx",
  "main.tsx|./i18n|value|i18n.ts",
  "main.tsx|./styles.css|value|styles.css",
  "main.tsx|./theme|value|theme.ts",
  "StrategyManager.test.tsx|./api|type-only|api.ts",
  "StrategyManager.test.tsx|./api|value|api.ts",
  "StrategyManager.test.tsx|./i18n|value|i18n.ts",
  "StrategyManager.test.tsx|./StrategyManager|value|StrategyManager.tsx",
  "StrategyManager.tsx|./api|value|api.ts",
  "StrategyManager.tsx|./i18n|type-only|i18n.ts",
  "tradeChart.test.ts|./tradeChart|value|tradeChart.ts",
  "tradeChart.ts|./decimal|value|decimal.ts",
  "tradeChart.ts|./shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "tradeChart.ts|./theme|type-only|theme.ts",
  "tradeChart.ts|./utc|value|utc.ts",
  "TradeChartView.test.tsx|./i18n|value|i18n.ts",
  "TradeChartView.test.tsx|./TradeChartView|value|TradeChartView.tsx",
  "TradeChartView.test.tsx|./tradeDemo|value|tradeDemo.ts",
  "TradeChartView.tsx|./CandlestickChart|value|CandlestickChart.tsx",
  "TradeChartView.tsx|./decimal|value|decimal.ts",
  "TradeChartView.tsx|./i18n|type-only|i18n.ts",
  "TradeChartView.tsx|./theme|type-only|theme.ts",
  "TradeChartView.tsx|./tradeChart|value|tradeChart.ts",
  "TradeChartView.tsx|./tradeDemo|value|tradeDemo.ts",
  "TradeChartView.tsx|./utc|value|utc.ts",
  "tradeDemo.ts|./shared/trading/closedTrade|type-only|shared/trading/closedTrade.ts",
  "tradeDemo.ts|./tradeChart|type-only|tradeChart.ts"
] as const;

const expectedPolicyGlobalLedger = [
  "api.ts|fetch()",
  "app/App.tsx|import.meta.env.DEV",
  "app/App.tsx|window.history.replaceState()",
  "app/App.tsx|window.location.href",
  "app/App.tsx|window.location.href",
  "app/App.tsx|window.location.search",
  "features/research/FitnessSurface3D.tsx|window.devicePixelRatio",
  "i18n.ts|document.documentElement.lang",
  "i18n.ts|document.documentElement.lang",
  "i18n.ts|navigator",
  "i18n.ts|navigator.language",
  "i18n.ts|navigator.languages",
  "i18n.ts|window.localStorage.getItem()",
  "i18n.ts|window.localStorage.setItem()",
  "main.tsx|document.getElementById()",
  "StrategyManager.tsx|window.confirm()",
  "StrategyManager.tsx|window.crypto.randomUUID()",
  "StrategyManager.tsx|window.sessionStorage.getItem()",
  "StrategyManager.tsx|window.sessionStorage.removeItem()",
  "StrategyManager.tsx|window.sessionStorage.setItem()",
  "theme.ts|document.documentElement.dataset.theme",
  'theme.ts|document.querySelector("meta[name=\\"theme-color\\"]")',
  "theme.ts|window.localStorage.getItem()",
  "theme.ts|window.localStorage.setItem()",
  "theme.ts|window.matchMedia?.()"
] as const;

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

function sourceFiles(): string[] {
  return walkFiles(sourceRoot)
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

function resolveSpecifier(importer: string, specifier: string): string {
  if (!specifier.startsWith(".")) {
    return specifier;
  }
  const base = path.resolve(path.dirname(importer), specifier);
  for (const candidate of [
    base,
    `${base}.ts`,
    `${base}.tsx`,
    `${base}.d.ts`,
    path.join(base, "index.ts"),
    path.join(base, "index.tsx")
  ]) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return path.relative(sourceRoot, candidate).replaceAll(path.sep, "/");
    }
  }
  throw new Error(`unresolved architecture import: ${specifier}`);
}

function collectImportsFromSource(
  sourceFile: SourceFile,
  importer = sourceFile.fileName,
  checker?: Checker
): ImportEdge[] {
  const edges: ImportEdge[] = [];
  const add = (specifier: string, kind: ImportKind) => {
    edges.push({
      importer: path.relative(sourceRoot, importer).replaceAll(path.sep, "/"),
      kind,
      resolved: resolveSpecifier(importer, specifier),
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
  checker: Checker
): PolicyGlobalUse[] {
  const uses: PolicyGlobalUse[] = [];
  const importer = path
    .relative(sourceRoot, sourceFile.fileName)
    .replaceAll(path.sep, "/");
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
  inspect: (sourceFile: SourceFile, checker: Checker) => T
): T {
  const fixturePath = path.join(
    "/tmp",
    `fluxtrade-architecture-${process.pid}-${compilerFixtureSequence++}.ts`
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
  it("freezes the complete Node D source inventory", () => {
    const inventory = walkFiles(sourceRoot).map((file) =>
      path.relative(sourceRoot, file).replaceAll(path.sep, "/")
    );
    expect(inventory).toEqual(expectedInventory);
    expect(
      inventory.filter((file) => executableExtensions.has(path.extname(file)))
    ).toEqual(inventory.filter((file) => /\.(?:ts|tsx)$/.test(file)));
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

  it("freezes every transitional repository-relative owner edge", () => {
    const edges = collectImports();
    expect(directRelativeSpecifiers(edges, "main.tsx")).toEqual([
      "./app/App|value|app/App.tsx",
      "./i18n|value|i18n.ts",
      "./styles.css|value|styles.css",
      "./theme|value|theme.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "app/App.tsx")).toEqual([
      "../StrategyManager|value|StrategyManager.tsx",
      "../TradeChartView|value|TradeChartView.tsx",
      "../features/research/ResearchRoute|value|features/research/ResearchRoute.tsx",
      "../features/results/BacktestResultsView|value|features/results/BacktestResultsView.tsx",
      "../i18n|type-only|i18n.ts",
      "../theme|value|theme.ts",
      "./navigation|value|app/navigation.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "features/research/gaDomain.ts")).toEqual([
      "../../api|type-only|api.ts"
    ]);
    expect(directRelativeSpecifiers(edges, "features/research/gaCharts.ts")).toEqual([
      "../../api|type-only|api.ts",
      "../../theme|type-only|theme.ts",
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

  it("freezes exact unshadowed production policy-global uses", () => {
    expect(
      collectPolicyGlobalUses().map(
        ({ importer, access }) => `${importer}|${access}`
      )
    ).toEqual(expectedPolicyGlobalLedger);

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
      "../../EChart",
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
