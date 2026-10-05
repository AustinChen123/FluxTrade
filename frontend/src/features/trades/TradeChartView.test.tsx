// @vitest-environment jsdom

import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type {
  BacktestResultsCandlesPage,
  BacktestResultsDetail,
  BacktestResultsTrade
} from "../../api";
import i18n from "../../shared/i18n";
import { demoTradeSnapshot } from "./demo";
import { TradeChartView } from "./TradeChartView";

const tradesRead = vi.hoisted(() => ({ useBacktestTrades: vi.fn() }));
const resultsApi = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadBacktestResult: vi.fn(),
  loadBacktestResultTrades: vi.fn(),
  loadBacktestResultCandles: vi.fn(),
  ApiError: class ApiError extends Error {
    constructor(message: string, readonly status: number) {
      super(message);
    }
  }
}));

vi.mock("./useBacktestTrades", () => tradesRead);
vi.mock("../../api", () => resultsApi);

vi.mock("./CandlestickChart", () => ({
  CandlestickChart: ({
    ariaLabel,
    onDataClick
  }: {
    ariaLabel: string;
    onDataClick?: (data: unknown, dataIndex: number) => void;
  }) => (
    <button
      type="button"
      aria-label={ariaLabel}
      onClick={() =>
        onDataClick?.(
          {
            tradeId: "trade-000185",
            event: "entry",
            side: "SHORT",
            value: ["2026-07-28T16:20:00.000Z", 19858.5]
          },
          0
        )
      }
    />
  )
}));

function wireTrade(jobId: string, index: number, entryMinute: number): BacktestResultsTrade {
  const timestamp = (minute: number) =>
    `2026-01-01T00:${String(minute).padStart(2, "0")}:00.000Z`;
  return {
    id: `${jobId}:${index}`,
    entry_time: timestamp(entryMinute),
    exit_time: timestamp(entryMinute + 1),
    entry_price: "10.000000000000000001",
    exit_price: "11.000000000000000001",
    side: index % 2 === 0 ? "LONG" : "SHORT",
    quantity: "0.000000000000000001",
    pnl: "0.0000000000000000001",
    fee: "0.00000000000000000001"
  };
}

function wireDetail(jobId: string): BacktestResultsDetail {
  return {
    job_id: jobId,
    strategy_id: "strategy-1",
    subject_id: "strategy-1:v1",
    dataset_id: "dataset-1",
    product_id: "RITHMIC:MNQ-CONTINUOUS",
    timeframe: "1m",
    started_at: "2026-01-01T00:00:00.000Z",
    ended_at: "2026-01-01T00:20:00.000Z",
    currency: "USDT",
    metrics: {
      net_pnl: "0.0000000000000000001",
      return_pct: "0.000000000000000001",
      max_drawdown: "0",
      sharpe: "1",
      sortino: "1",
      calmar: "1"
    },
    equity: [],
    monthly_returns: [],
    pnl_distribution: [],
    trade_page: {
      items: [wireTrade(jobId, 0, 7), wireTrade(jobId, 1, 9)],
      total_count: 3,
      next_cursor: "signed-page-1"
    },
    input_digest: "a".repeat(64),
    result_digest: "b".repeat(64),
    revision: 1
  };
}

function wireCandles(): BacktestResultsCandlesPage {
  return {
    items: [7, 8, 9, 10].map((minute) => ({
      timestamp: `2026-01-01T00:${String(minute).padStart(2, "0")}:00.000Z`,
      open: "9.0000000000000000001",
      high: "12.0000000000000000001",
      low: "8.0000000000000000001",
      close: "10.0000000000000000001",
      volume: "1.0000000000000000001"
    })),
    next_cursor: null,
    revision: 1
  };
}

function deferredApi<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

describe("TradeChartView", () => {
  beforeEach(async () => {
    await i18n.changeLanguage("zh-TW");
    tradesRead.useBacktestTrades.mockReturnValue({
      state: { status: "unavailable", resultId: null, tradeId: null },
      loadMoreTrades: vi.fn(),
      tradePageLoading: false
    });
  });

  afterEach(() => {
    cleanup();
  });

  it("selects the same trade from chart markers and the accessible ledger", async () => {
    const onSelectTrade = vi.fn();
    render(
      <TradeChartView
        demoMode
        theme="light"
        onSelectTrade={onSelectTrade}
      />
    );

    fireEvent.click(
      await screen.findByRole("button", {
        name: "顯示策略進出場標記的互動式 K 線圖"
      })
    );
    expect(screen.getAllByText("trade-000185")).toHaveLength(2);
    expect(
      screen.getByRole("button", { name: /trade-000185/ }).getAttribute(
        "aria-pressed"
      )
    ).toBe("true");
    expect(onSelectTrade).toHaveBeenLastCalledWith("trade-000185");

    fireEvent.click(screen.getByRole("button", { name: /trade-000184/ }));
    expect(screen.getAllByText("trade-000184")).toHaveLength(2);
    expect(onSelectTrade).toHaveBeenLastCalledWith("trade-000184");
    expect(tradesRead.useBacktestTrades).toHaveBeenCalledWith({
      selectedResultId: null,
      selectedTradeId: null,
      enabled: false
    });
  });

  it("preselects a trade opened from the backtest result ledger", async () => {
    render(
      <TradeChartView
        demoMode
        theme="light"
        initialTradeId="trade-000185"
      />
    );

    const selectedTrade = await screen.findByRole("button", {
      name: /trade-000185/
    });
    expect(selectedTrade.getAttribute("aria-pressed")).toBe("true");
    expect(selectedTrade.textContent).toContain("18:20");
    expect(selectedTrade.textContent).toContain("GMT+2");
    expect(screen.getAllByText("trade-000185")).toHaveLength(2);
  });

  it("formats trade financial text without losing Decimal precision", () => {
    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        snapshot={{
          ...demoTradeSnapshot,
          trades: [
            {
              ...demoTradeSnapshot.trades[0],
              pnl: "9007199254740993.00"
            }
          ]
        }}
      />
    );

    expect(
      screen.getByRole("button", { name: /trade-000184/ }).textContent
    ).toContain("9,007,199,254,740,993.00");
  });

  it.each(["0x10", "1e2", "", "Infinity"])(
    "rejects non-decimal trade value %j",
    (pnl) => {
      render(
        <TradeChartView
          demoMode={false}
          theme="light"
          snapshot={{
            ...demoTradeSnapshot,
            trades: [
              {
                ...demoTradeSnapshot.trades[0],
                pnl
              }
            ]
          }}
        />
      );

      expect(
        screen.getByRole("button", { name: /trade-000184/ }).textContent
      ).toContain("—");
    }
  );

  it("fails closed when production data is unavailable", () => {
    render(<TradeChartView demoMode={false} theme="dark" />);

    expect(screen.getByText("找不到這筆結果或交易")).toBeTruthy();
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
  });

  it("uses controlled production IDs and retains the ledger when candles are empty", () => {
    const trade = {
      ...demoTradeSnapshot.trades[0],
      fee: "0.00000000000000000001",
      pnl: "0.0000000000000000001"
    };
    const loadMoreTrades = vi.fn();
    tradesRead.useBacktestTrades.mockReturnValue({
      state: {
        status: "valid",
        resultId: "job-production-1",
        tradeId: trade.id,
        identity: {
          resultId: "job-production-1",
          strategyId: "strategy-1",
          productId: "RITHMIC:MNQ-CONTINUOUS",
          currency: "USDT",
          timeframe: "5m",
          timeframeMs: 300_000,
          coverageStart: 1_785_240_000_000,
          coverageEnd: 1_785_244_800_001,
          tradePage: { items: [trade], totalCount: 101, nextCursor: "signed.next" }
        },
        tradePage: { items: [trade], totalCount: 101, nextCursor: "signed.next" },
        totalCount: 101,
        snapshot: { ...demoTradeSnapshot, candles: [], trades: [trade] }
      },
      loadMoreTrades,
      tradePageLoading: true
    });

    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        selectedResultId="job-production-1"
        selectedTradeId={trade.id}
      />
    );

    expect(tradesRead.useBacktestTrades).toHaveBeenCalledWith({
      selectedResultId: "job-production-1",
      selectedTradeId: trade.id,
      enabled: true
    });
    expect(screen.getByText("這段結果沒有可顯示的 K 線")).toBeTruthy();
    expect(screen.getAllByText("101").length).toBeGreaterThan(0);
    expect(screen.getAllByText(trade.id).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/USDT/).length).toBeGreaterThan(0);
    expect(screen.getByText("策略判斷週期")).toBeTruthy();
    expect(screen.getByText("來源 K 線")).toBeTruthy();
    expect(screen.getAllByText("0.0000000000000000001 USDT").length).toBeGreaterThan(0);
    expect(screen.getAllByText("0.00000000000000000001 USDT").length).toBeGreaterThan(0);
    expect(screen.queryByText(/已略過/)).toBeNull();
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
    expect(screen.getByRole("button", { name: "載入交易中" }).hasAttribute("disabled")).toBe(true);
    expect(screen.getByText("載入交易中")).toBeTruthy();
    expect(loadMoreTrades).not.toHaveBeenCalled();
  });

  it("reports skipped quality when every nonempty production candle is rejected by the chart model", () => {
    const trade = demoTradeSnapshot.trades[0];
    tradesRead.useBacktestTrades.mockReturnValue({
      state: {
        status: "valid",
        resultId: "job-invalid-candles",
        tradeId: trade.id,
        identity: {
          resultId: "job-invalid-candles",
          strategyId: "strategy-1",
          productId: "RITHMIC:MNQ-CONTINUOUS",
          currency: "USD",
          timeframe: "5m",
          timeframeMs: 300_000,
          coverageStart: 1_785_240_000_000,
          coverageEnd: 1_785_244_800_001,
          tradePage: { items: [trade], totalCount: 1, nextCursor: null }
        },
        tradePage: { items: [trade], totalCount: 1, nextCursor: null },
        totalCount: 1,
        snapshot: {
          ...demoTradeSnapshot,
          candles: [{
            ...demoTradeSnapshot.candles[0],
            high: "1",
            low: "1"
          }],
          trades: [trade]
        }
      },
      loadMoreTrades: vi.fn(),
      tradePageLoading: false
    });

    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        selectedResultId="job-invalid-candles"
        selectedTradeId={trade.id}
      />
    );

    expect(screen.getByText("已略過 1 根無效 K 線與 2 個無法精確對齊的交易標記。"))
      .toBeTruthy();
    expect(screen.getByRole("button", { name: /trade-000184/ })).toBeTruthy();
  });

  it("composes the real read hook, shared pager and controlled selection without stale chart data", async () => {
    vi.clearAllMocks();
    const actual = await vi.importActual<typeof import("./useBacktestTrades")>(
      "./useBacktestTrades"
    );
    tradesRead.useBacktestTrades.mockImplementation(actual.useBacktestTrades);
    resultsApi.ensureBrowserSession.mockResolvedValue({ actor: "operator" });

    const nextTrades = deferredApi<{
      items: BacktestResultsTrade[];
      next_cursor: null;
      revision: 1;
    }>();
    const secondTradeCandles = deferredApi<BacktestResultsCandlesPage>();
    const nextTrade = wireTrade("job-B", 2, 11);
    const detailB = deferredApi<BacktestResultsDetail>();
    resultsApi.loadBacktestResult.mockImplementation((jobId: string) =>
      jobId === "job-B" ? detailB.promise : Promise.resolve(wireDetail(jobId))
    );
    resultsApi.loadBacktestResultTrades.mockReturnValue(nextTrades.promise);
    resultsApi.loadBacktestResultCandles.mockImplementation(
      (_jobId: string, query: { start: number }) =>
        query.start === Date.parse("2026-01-01T00:07:00.000Z")
          ? secondTradeCandles.promise
          : Promise.resolve(wireCandles())
    );

    const onSelectTrade = vi.fn();
    const view = (resultId: string, tradeId: string) => (
      <TradeChartView
        demoMode={false}
        theme="light"
        selectedResultId={resultId}
        selectedTradeId={tradeId}
        onSelectTrade={onSelectTrade}
      />
    );
    const { rerender, container } = render(view("job-A", "job-A:0"));
    const chartLabel = "顯示策略進出場標記的互動式 K 線圖";
    await screen.findByLabelText(chartLabel);
    expect(screen.getByText("已載入 2／3 筆")).toBeTruthy();
    expect(screen.getAllByText("job-A:0").length).toBeGreaterThan(0);

    rerender(view("job-B", "job-B:0"));
    expect(screen.queryByLabelText(chartLabel)).toBeNull();
    expect(container.querySelector(".trade-detail")).toBeNull();
    expect(screen.queryByText("job-A:0")).toBeNull();
    await waitFor(() => expect(resultsApi.loadBacktestResult).toHaveBeenLastCalledWith("job-B"));
    await act(async () => detailB.resolve(wireDetail("job-B")));
    await screen.findByLabelText(chartLabel);

    fireEvent.click(screen.getByRole("button", { name: /job-B:1/ }));
    expect(onSelectTrade).toHaveBeenLastCalledWith("job-B:1");
    rerender(view("job-B", "job-B:1"));
    expect(screen.queryByLabelText(chartLabel)).toBeNull();
    expect(container.querySelector(".trade-detail")).toBeNull();
    await waitFor(() => expect(resultsApi.loadBacktestResultCandles).toHaveBeenCalledTimes(2));
    await act(async () => secondTradeCandles.resolve(wireCandles()));
    await screen.findByLabelText(chartLabel);
    expect(screen.getAllByText("job-B:1").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "載入更多交易" }));
    const loadingButton = await screen.findByRole("button", { name: "載入交易中" });
    expect((loadingButton as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByLabelText(chartLabel)).toBeTruthy();
    expect(screen.getByText("已載入 2／3 筆")).toBeTruthy();
    expect(resultsApi.loadBacktestResultTrades).toHaveBeenCalledWith("job-B", {
      cursor: "signed-page-1"
    });

    await act(async () => nextTrades.reject(new resultsApi.ApiError("private detail", 403)));
    await waitFor(() => expect(screen.queryByLabelText(chartLabel)).toBeNull());
    expect(container.querySelector(".trade-detail")).toBeNull();
    expect(screen.getAllByText("目前工作階段無法讀取所選結果。請返回結果頁選擇其他結果。").length)
      .toBeGreaterThan(0);
  });

  it("renders the selectable page without requesting a chart before a trade is chosen", () => {
    const first = demoTradeSnapshot.trades[0];
    const second = demoTradeSnapshot.trades[1];
    const loadMoreTrades = vi.fn();
    tradesRead.useBacktestTrades.mockReturnValue({
      state: {
        status: "selection",
        resultId: "job-production-2",
        tradeId: null,
        identity: {
          resultId: "job-production-2",
          strategyId: "strategy-1",
          productId: "RITHMIC:MNQ-CONTINUOUS",
          currency: "USD",
          timeframe: "5m",
          timeframeMs: 300_000,
          coverageStart: 1_785_240_000_000,
          coverageEnd: 1_785_244_800_001,
          tradePage: { items: [first, second], totalCount: 3, nextCursor: "signed.next" }
        },
        tradePage: { items: [first, second], totalCount: 3, nextCursor: "signed.next" },
        totalCount: 3
      },
      loadMoreTrades,
      tradePageLoading: false
    });

    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        selectedResultId="job-production-2"
        selectedTradeId={null}
      />
    );

    expect(screen.getAllByText("點擊圖表標記或交易列以查看明細。").length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: /trade-000184/ })).toBeTruthy();
    expect(screen.getByRole("button", { name: /trade-000185/ })).toBeTruthy();
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
    expect(tradesRead.useBacktestTrades).toHaveBeenCalledWith({
      selectedResultId: "job-production-2",
      selectedTradeId: null,
      enabled: true
    });
    fireEvent.click(screen.getByRole("button", { name: "載入更多交易" }));
    expect(loadMoreTrades).toHaveBeenCalledOnce();
  });

  it("keeps the zero-trade result empty state distinct from missing candles", () => {
    tradesRead.useBacktestTrades.mockReturnValue({
      state: {
        status: "empty",
        resultId: "job-empty",
        tradeId: null,
        identity: {
          resultId: "job-empty",
          strategyId: "strategy-1",
          productId: "RITHMIC:MNQ-CONTINUOUS",
          currency: "USD",
          timeframe: "5m",
          timeframeMs: 300_000,
          coverageStart: 1_785_240_000_000,
          coverageEnd: 1_785_244_800_001,
          tradePage: { items: [], totalCount: 0, nextCursor: null }
        },
        tradePage: { items: [], totalCount: 0, nextCursor: null },
        totalCount: 0
      },
      loadMoreTrades: vi.fn(),
      tradePageLoading: false
    });

    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        selectedResultId="job-empty"
        selectedTradeId={null}
      />
    );

    expect(screen.getAllByText("這段 K 線沒有已平倉交易。").length).toBeGreaterThan(0);
    expect(screen.queryByText("這段結果沒有可顯示的 K 線")).toBeNull();
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
  });

  it.each([
    ["pending", "載入 K 線圖表", "正在載入所選結果與交易清冊。"],
    ["permission-unavailable", "無法讀取這筆交易", "目前工作階段無法讀取所選結果。請返回結果頁選擇其他結果。"],
    ["unavailable", "找不到這筆結果或交易", "請返回結果頁並選擇可用的結果。"],
    ["error", "無法讀取交易資料", "請返回結果頁。所選交易資料可用前，不會顯示圖表。"],
    ["invalid-data", "交易資料無效", "所選資料無法安全顯示。請返回結果頁並選擇其他結果。"],
    ["candle-window-unavailable", "這筆交易的 K 線範圍不可用", "所選交易範圍超出可用 K 線資料。請返回結果頁並選擇其他交易。"]
  ])("renders a fixed localized %s state without an old chart", (status, title, body) => {
    tradesRead.useBacktestTrades.mockReturnValue({
      state: { status, resultId: "job-failed", tradeId: "job-failed:0" },
      loadMoreTrades: vi.fn(),
      tradePageLoading: false
    });

    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        selectedResultId="job-failed"
        selectedTradeId="job-failed:0"
        onNavigateResults={vi.fn()}
      />
    );

    expect(screen.getByText(title)).toBeTruthy();
    expect(screen.getAllByText(body).length).toBeGreaterThan(0);
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
    expect(screen.getByRole("button", { name: "返回結果頁" })).toBeTruthy();
  });

  it("does not let the development demo override an explicit null snapshot", () => {
    render(<TradeChartView demoMode theme="dark" snapshot={null} />);

    expect(screen.getByText("尚未連接正式 K 線資料")).toBeTruthy();
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
    expect(tradesRead.useBacktestTrades).toHaveBeenCalledWith({
      selectedResultId: null,
      selectedTradeId: null,
      enabled: false
    });
  });

  it("shows zero trades without hiding valid candles", async () => {
    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        snapshot={{ ...demoTradeSnapshot, trades: [] }}
      />
    );

    expect(screen.getByText("這段 K 線沒有已平倉交易。")).toBeTruthy();
    expect(
      await screen.findByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeTruthy();
  });

  it("reports invalid candles and markers instead of shifting them", () => {
    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        snapshot={{
          ...demoTradeSnapshot,
          candles: [
            ...demoTradeSnapshot.candles,
            { ...demoTradeSnapshot.candles[0] }
          ],
          trades: [
            {
              ...demoTradeSnapshot.trades[0],
              entryTime: "2026-07-28T13:30:01.000Z"
            }
          ]
        }}
      />
    );

    expect(
      screen.getByText("已略過 1 根無效 K 線與 1 個無法精確對齊的交易標記。")
    ).toBeTruthy();
  });

  it("reports data quality when every candle is invalid", () => {
    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        snapshot={{
          ...demoTradeSnapshot,
          candles: [
            {
              ...demoTradeSnapshot.candles[0],
              timestamp: "not-a-timestamp"
            }
          ],
          trades: []
        }}
      />
    );

    expect(screen.getByText("這段結果沒有可顯示的 K 線")).toBeTruthy();
    expect(
      screen.getByText("已略過 1 根無效 K 線與 0 個無法精確對齊的交易標記。")
    ).toBeTruthy();
  });

  it("keeps the ledger available when a trade timestamp is invalid", () => {
    render(
      <TradeChartView
        demoMode={false}
        theme="light"
        snapshot={{
          ...demoTradeSnapshot,
          trades: [
            {
              ...demoTradeSnapshot.trades[0],
              entryTime: "not-a-timestamp"
            }
          ]
        }}
      />
    );

    expect(
      screen.getByText("已略過 0 根無效 K 線與 1 個無法精確對齊的交易標記。")
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: /trade-000184/ }).textContent
    ).toContain("—");
  });
});
