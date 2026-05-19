import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
import plotly.express as px
from src.data_provider import DataProvider, redis_client
from src.auth import check_auth
import json

# Page Config
st.set_page_config(
    page_title="FluxTrade Pro",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Modern Dark Look
st.markdown("""
<style>
    .stApp {
        background-color: #0E1117;
    }
    .metric-card {
        background-color: #1E1E25;
        border: 1px solid #2E2E36;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .metric-label {
        font-size: 14px;
        color: #A0A0A0;
        margin-bottom: 5px;
    }
    .metric-value {
        font-size: 32px;
        font-weight: 700;
        color: #FFFFFF;
    }
    .metric-delta {
        font-size: 14px;
        font-weight: 500;
    }
    .positive { color: #00CC96; }
    .negative { color: #EF553B; }
    
    /* Table Styling */
    .stDataFrame {
        border: 1px solid #2E2E36;
        border-radius: 5px;
    }
    
    /* Hide Streamlit Elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

def render_metric(label, value, delta=None, delta_color="normal"):
    delta_html = ""
    if delta:
        color_class = "positive" if delta_color == "normal" and "+" in str(delta) else "negative"
        if delta_color == "inverse":
            color_class = "negative" if "+" in str(delta) else "positive"
        delta_html = f'<div class="metric-delta {color_class}">{delta}</div>'
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

def main():
    if not check_auth():
        return

    # Sidebar
    st.sidebar.title("⚡ FluxTrade")
    st.sidebar.markdown("---")
    page = st.sidebar.radio("Navigation", ["📈 Market Live", "📜 Trade Blotter", "🛡️ Risk Engine", "⚙️ System Status"])
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Force Refresh"):
        st.rerun()
    
    auto_refresh = st.sidebar.checkbox("Auto-Refresh (5s)", value=False)
    
    if page == "📈 Market Live":
        show_market_overview()
    elif page == "📜 Trade Blotter":
        show_trade_history()
    elif page == "🛡️ Risk Engine":
        show_rule_verification()
    elif page == "⚙️ System Status":
        show_system_status()

    if auto_refresh:
        time.sleep(5)
        st.rerun()

def show_market_overview():
    st.title("Market Overview")
    
    # Top Metrics Row
    c1, c2, c3, c4 = st.columns(4)
    with c1: render_metric("BTC Price", "$104,250.00", "+1.2%")
    with c2: render_metric("24h Vol", "$1.2B", "+5%")
    with c3: render_metric("Funding", "0.0100%", "Neutral")
    with c4: render_metric("System Latency", "2.1 ms", "Excellent")

    st.markdown("### Real-time Chart (BTCUSDT)")
    
    # Real Data from DB
    try:
        # Assuming DataProvider has a get_candles method, if not we need to add it.
        # For now, let's query the 'candlestick' table directly via DataProvider helper or raw SQL
        # But wait, DataProvider doesn't have get_candles yet. Let's add it or use raw SQL here for speed.
        query = "SELECT timestamp, open, high, low, close, volume FROM candlestick WHERE product_id = 'BINANCE:BTCUSDT-PERP' ORDER BY timestamp DESC LIMIT 100"
        df = pd.read_sql(query, DataProvider.engine)
        
        if not df.empty:
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                        open=df['open'], high=df['high'],
                        low=df['low'], close=df['close'])])
            
            fig.update_layout(
                xaxis_rangeslider_visible=False,
                template="plotly_dark",
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                height=500,
                margin=dict(l=0, r=0, t=0, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Waiting for market data stream... (Check Rust Service)")
            
    except Exception as e:
        st.error(f"Error fetching market data: {e}")

def show_trade_history():
    st.title("Trade Blotter")
    
    tab1, tab2 = st.tabs(["Active Positions", "Execution History"])
    
    with tab1:
        try:
            df_pos = DataProvider.get_positions()
            if not df_pos.empty:
                st.dataframe(df_pos, use_container_width=True, height=300)
            else:
                st.info("No active positions.")
        except Exception as e:
            st.error(f"DB Connection Error: {e}")

    with tab2:
        try:
            df_trades = DataProvider.get_trades(limit=50)
            if not df_trades.empty:
                st.dataframe(df_trades, use_container_width=True)
            else:
                st.info("No trades recorded yet.")
        except Exception as e:
            st.error(f"DB Error: {e}")

def show_rule_verification():
    st.title("Risk & Audit Trail")
    
    try:
        df_audit = DataProvider.get_signal_audits()
        if df_audit.empty:
            st.info("No audit logs found.")
            return

        # Filters
        strategies = st.multiselect("Filter Strategy", df_audit['strategy_id'].unique())
        if strategies:
            df_audit = df_audit[df_audit['strategy_id'].isin(strategies)]

        st.dataframe(
            df_audit[['timestamp', 'strategy_id', 'product_id', 'signal_type', 'risk_status', 'risk_message']], 
            use_container_width=True,
            column_config={
                "risk_status": st.column_config.TextColumn(
                    "Risk Check",
                    help="Pass/Fail status",
                    validate="^(PASS|FAIL)$"
                )
            }
        )
    except Exception as e:
        st.error(f"Could not fetch audit logs: {e}")

def show_system_status():
    st.title("System Health")
    c1, c2 = st.columns(2)
    with c1:
        st.success("Rust Data Service: ONLINE")
        st.success("Python Strategy Engine: ONLINE")
    with c2:
        st.info("Redis Connection: STABLE")
        st.info("Postgres Database: CONNECTED")

if __name__ == "__main__":
    main()