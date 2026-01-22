import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
from src.data_provider import DataProvider, redis_client
import json

st.set_page_config(page_title="FluxTrade Dashboard", layout="wide")

def main():
    st.sidebar.title("FluxTrade Control")
    page = st.sidebar.selectbox("Choose a page", ["Market Overview", "Trade History", "Risk Monitor", "Rule Verification"])

    if page == "Market Overview":
        show_market_overview()
    elif page == "Trade History":
        show_trade_history()
    elif page == "Risk Monitor":
        show_risk_monitor()
    elif page == "Rule Verification":
        show_rule_verification()

def show_rule_verification():
    st.title("🛡️ Rule Verification")
    st.write("Detailed audit trail of strategy signals and risk decisions.")
    
    df_audit = DataProvider.get_signal_audits()
    if df_audit.empty:
        st.info("No signal audits found yet.")
        return

    # Display summary table
    st.subheader("Decision History")
    selected_id = st.selectbox("Select an Audit ID to view details", df_audit['id'].tolist())
    
    st.dataframe(df_audit[['id', 'timestamp', 'strategy_id', 'product_id', 'signal_type', 'risk_status', 'risk_message']], use_container_width=True)
    
    # Show Details
    if selected_id:
        detail_row = df_audit[df_audit['id'] == selected_id].iloc[0]
        st.divider()
        st.subheader(f"Details for Audit #{selected_id}")
        
        col1, col2 = st.columns(2)
        with col1:
            st.json({
                "Strategy": detail_row['strategy_id'],
                "Product": detail_row['product_id'],
                "Signal": detail_row['signal_type'],
                "Risk": detail_row['risk_status'],
                "Order ID": detail_row['order_id']
            })
        with col2:
            st.text("Decision Context (Raw JSON):")
            st.json(detail_row['details_json'])

def show_market_overview():
    st.title("📊 Market Overview")
    
    product_id = "BINANCE:BTCUSDT-PERP"
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        chart_placeholder = st.empty()
        
    with col2:
        st.subheader("Latest Info")
        info_placeholder = st.empty()

    # Simple Real-time Loop
    # In a real app, use streamlit-elements or similar for better perf
    for _ in range(100): # Run for some time
        raw_candle = redis_client.get(f"latest_candle:{product_id}")
        if raw_candle:
            candle_data = json.loads(raw_candle)
            
            # Show Metrics
            with info_placeholder.container():
                st.metric("Price", f"${candle_data['close']}", delta=f"{float(candle_data['close']) - float(candle_data['open']):.2f}")
                st.write(f"Timestamp: {pd.to_datetime(candle_data['timestamp'], unit='ms')}")
            
            # Show Simple Chart (Mocking historical data with a single point or last 20)
            # In a real system, we'd query DB for last 100 candles
            fig = go.Figure(data=[go.Candlestick(
                x=[pd.to_datetime(candle_data['timestamp'], unit='ms')],
                open=[candle_data['open']],
                high=[candle_data['high']],
                low=[candle_data['low']],
                close=[candle_data['close']]
            )])
            fig.update_layout(title=f"{product_id} Real-time", xaxis_rangeslider_visible=False)
            chart_placeholder.plotly_chart(fig, use_container_width=True)
            
        time.sleep(1)

def show_trade_history():
    st.title("📜 Trade & Order History")
    
    st.subheader("Current Positions")
    df_pos = DataProvider.get_positions()
    st.dataframe(df_pos, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Latest Orders")
        df_orders = DataProvider.get_latest_orders()
        st.dataframe(df_orders, use_container_width=True)
        
    with col2:
        st.subheader("Latest Trades")
        df_trades = DataProvider.get_trades()
        st.dataframe(df_trades, use_container_width=True)

def show_risk_monitor():
    st.title("🛡️ Risk Monitor")
    st.warning("Risk rules are actively monitored by the Strategy Service.")
    
    # Placeholder for risk metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Daily PNL", "$0.00", "0%")
    col2.metric("Max Exposure", "$50,000", "Safe")
    col3.metric("System Status", "Running", "Green")

if __name__ == "__main__":
    main()
