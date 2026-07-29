// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "./i18n";
import { demoTradeSnapshot } from "./tradeDemo";
import { TradeChartView } from "./TradeChartView";

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

describe("TradeChartView", () => {
  beforeEach(async () => {
    await i18n.changeLanguage("zh-TW");
  });

  afterEach(() => {
    cleanup();
  });

  it("selects the same trade from chart markers and the accessible ledger", async () => {
    render(<TradeChartView demoMode theme="light" />);

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

    fireEvent.click(screen.getByRole("button", { name: /trade-000184/ }));
    expect(screen.getAllByText("trade-000184")).toHaveLength(2);
  });

  it("fails closed when production data is unavailable", () => {
    render(<TradeChartView demoMode={false} theme="dark" />);

    expect(screen.getByText("尚未連接正式 K 線資料")).toBeTruthy();
    expect(
      screen.queryByLabelText("顯示策略進出場標記的互動式 K 線圖")
    ).toBeNull();
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
