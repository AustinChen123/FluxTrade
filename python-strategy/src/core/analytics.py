from decimal import Decimal
from typing import List, Dict
import pandas as pd
import numpy as np
from src.core.models import Trade

def calculate_metrics(trade_history: List[Trade]) -> Dict:
    """
    Calculate performance metrics from a list of trades.
    metrics: Total PnL, Max Drawdown, Sharpe Ratio, Win Rate
    Uses a FIFO/Netting approach to calculate Realized PnL.
    """
    if not trade_history:
        return {
            "total_pnl": Decimal("0.00"),
            "max_drawdown": Decimal("0.00"),
            "sharpe_ratio": 0.0,
            "win_rate": 0.0
        }

    # Convert to DataFrame for easier calculation
    trades = []
    for t in trade_history:
        trades.append({
            "timestamp": t.timestamp,
            "side": t.side, # 'buy' or 'sell'
            "price": float(t.price),
            "quantity": float(t.quantity),
        })
    
    df = pd.DataFrame(trades)
    df.sort_values("timestamp", inplace=True)

    total_pnl = 0.0
    wins = 0
    losses = 0
    
    # Netting Position Logic
    net_qty = 0.0
    avg_entry_price = 0.0
    
    equity_curve = [0.0]
    trade_pnls = []

    for _, row in df.iterrows():
        qty = row['quantity']
        price = row['price']
        side = row['side']
        
        # Signed quantity: Buy is positive, Sell is negative
        signed_qty = qty if side.lower() == 'buy' else -qty
        
        # Check if this trade reduces existing position
        is_reducing = (net_qty > 0 and signed_qty < 0) or (net_qty < 0 and signed_qty > 0)
        
        if is_reducing:
            qty_closing = min(abs(net_qty), abs(signed_qty))
            
            # Calculate PnL on the closed portion
            if net_qty > 0: # Closing Long
                pnl = (price - avg_entry_price) * qty_closing
            else: # Closing Short
                pnl = (avg_entry_price - price) * qty_closing
            
            total_pnl += pnl
            trade_pnls.append(pnl)
            
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
            
            # Update Position
            remaining_after_close = abs(signed_qty) - qty_closing
            
            # Reduce net_qty towards zero
            if net_qty > 0:
                net_qty -= qty_closing
            else:
                net_qty += qty_closing
                
            # If the trade was large enough to flip position
            if remaining_after_close > 0:
                # We have a remainder that opens a new position in the direction of the trade
                # New net_qty will be the remainder with the sign of the trade
                net_qty = remaining_after_close if signed_qty > 0 else -remaining_after_close
                avg_entry_price = price
        else:
            # Increasing Position or opening new
            total_cost = (abs(net_qty) * avg_entry_price) + (abs(signed_qty) * price)
            new_qty = abs(net_qty) + abs(signed_qty)
            
            if new_qty > 0:
                avg_entry_price = total_cost / new_qty
            
            net_qty += signed_qty

        equity_curve.append(total_pnl)

    # Metrics Calculation
    win_rate = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0
    
    # Max Drawdown
    equity_series = pd.Series(equity_curve)
    rolling_max = equity_series.cummax()
    drawdown = equity_series - rolling_max
    max_drawdown = drawdown.min()
    
    # Sharpe Ratio (Simplified: Mean Trade PnL / Std Dev Trade PnL)
    sharpe = 0.0
    if len(trade_pnls) > 1:
        returns = np.array(trade_pnls)
        std_dev = np.std(returns)
        if std_dev != 0:
            sharpe = np.mean(returns) / std_dev

    return {
        "total_pnl": Decimal(f"{total_pnl:.2f}"),
        "max_drawdown": Decimal(f"{max_drawdown:.2f}"),
        "sharpe_ratio": float(f"{sharpe:.2f}"),
        "win_rate": float(f"{win_rate:.2f}")
    }
