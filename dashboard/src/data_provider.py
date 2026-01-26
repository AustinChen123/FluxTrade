import os
import pandas as pd
import redis
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load env from root
load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))

POSTGRES_USER = os.getenv('POSTGRES_USER', 'fluxtrade')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'fluxtrade')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'fluxtrade')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)

db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(db_url)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

class DataProvider:
    @staticmethod
    def get_latest_orders(limit=20):
        query = "SELECT * FROM \"order\" ORDER BY timestamp DESC LIMIT :limit"
        return pd.read_sql(text(query), engine, params={"limit": limit})

    @staticmethod
    def get_positions():
        query = "SELECT * FROM position"
        return pd.read_sql(text(query), engine)

    @staticmethod
    def get_trades(limit=50):
        query = "SELECT * FROM trade ORDER BY timestamp DESC LIMIT :limit"
        return pd.read_sql(text(query), engine, params={"limit": limit})

    @staticmethod
    def get_signal_audits(limit=50):
        query = "SELECT * FROM signal_audit ORDER BY timestamp DESC LIMIT :limit"
        return pd.read_sql(text(query), engine, params={"limit": limit})

    @staticmethod
    def get_realtime_candle(product_id: str):
        # Redis channel: market_data.BINANCE.BTCUSDT-PERP.1m
        exchange, symbol = product_id.split(':')
        channel = f"market_data.{exchange}.{symbol}.1m"
        
        # In a real app, we'd use a thread to listen. 
        # For Streamlit, we'll try to get the last message if possible, 
        # but Redis PubSub doesn't store history. 
        # BETTER: In python-strategy, we could also SET the latest candle to a Key.
        # For now, we return None and let Streamlit handle the fallback.
        return None
