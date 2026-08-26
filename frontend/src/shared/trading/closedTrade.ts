export type ClosedTrade = {
  id: string;
  side: "LONG" | "SHORT";
  quantity: string;
  entryTime: string;
  entryPrice: string;
  exitTime: string;
  exitPrice: string;
  fee: string;
  pnl: string;
};
