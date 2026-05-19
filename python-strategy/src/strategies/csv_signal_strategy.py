"""Strategy that replays pre-computed signals from a CSV file."""
import csv
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType


def _parse_decimal(value: str, field_name: str = "") -> Optional[Decimal]:
    """Parse a CSV field into Decimal, returning None for empty/whitespace.

    Raises ValueError for non-empty invalid values (e.g. 'abc').
    """
    value = value.strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        raise ValueError(
            f"Invalid Decimal value '{value}'"
            + (f" for field '{field_name}'" if field_name else "")
        )


class CsvSignalStrategy(BaseStrategy):
    """Replay pre-computed signals from a CSV file by timestamp matching.

    CSV format (header required):
        timestamp,type[,price,stop_loss,take_profit,trailing_distance,quantity]

    Only timestamp and type are required. Other fields are optional.

    Usage:
        strategy = CsvSignalStrategy("replay_v1", "signals.csv", "BINANCE:BTCUSDT-PERP", "1h")
        runner = BacktestRunner(data_source=source, strategies=[strategy])
    """

    def __init__(
        self,
        strategy_id: str,
        csv_path: str,
        product_id: str,
        timeframe: str = "1h",
        lookback_window: int = 1,
    ):
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe
        self._lookback_window = lookback_window
        self._signals: Dict[int, Signal] = self._load_signals(csv_path)

    def _load_signals(self, path: str) -> Dict[int, Signal]:
        signals: Dict[int, Signal] = {}
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = int(row["timestamp"].strip())
                type_str = row["type"].strip()
                try:
                    signal_type = SignalType(type_str)
                except ValueError:
                    raise ValueError(
                        f"Invalid signal type '{type_str}' at timestamp {ts}. "
                        f"Valid types: {[t.value for t in SignalType]}"
                    )

                signals[ts] = Signal(
                    strategy_id=self.strategy_id,
                    product_id=self.product_id,
                    timeframe=self._timeframe,
                    timestamp=ts,
                    type=signal_type,
                    price=_parse_decimal(row.get("price", ""), "price"),
                    stop_loss=_parse_decimal(row.get("stop_loss", ""), "stop_loss"),
                    take_profit=_parse_decimal(row.get("take_profit", ""), "take_profit"),
                    trailing_distance=_parse_decimal(row.get("trailing_distance", ""), "trailing_distance"),
                    quantity=_parse_decimal(row.get("quantity", ""), "quantity"),
                )

        if not signals:
            raise ValueError(f"CSV '{path}' contains no signals (empty or header-only)")
        return signals

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self._timeframe,
            lookback_window=self._lookback_window,
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        signal = self._signals.get(candle.timestamp)
        if signal is not None:
            return signal
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self._timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
            value=candle.close,
        )
