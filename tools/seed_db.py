import os
import uuid
import time
import random
from sqlalchemy import create_engine, text
from decimal import Decimal

# DB Config (Hardcoded to match docker-compose.prod.yml)
# If running locally (outside docker), use localhost:5432
DB_URL = "postgresql://fluxtrade:fluxtrade@localhost:5432/fluxtrade"

def seed():
    print(f"🌱 Seeding DB at {DB_URL}...")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            # 1. Create Exchange & Product & Strategy
            conn.execute(text("INSERT INTO exchange (id, name) VALUES ('BINANCE', 'binance') ON CONFLICT DO NOTHING"))
            conn.execute(text("INSERT INTO product (id, exchange_id, base_asset, quote_asset) VALUES ('BINANCE:BTCUSDT-PERP', 'BINANCE', 'BTC', 'USDT') ON CONFLICT DO NOTHING"))
            conn.execute(text("INSERT INTO strategy (id, name, configuration_json) VALUES ('GoldenCross', 'Golden Cross Strategy', '{}') ON CONFLICT DO NOTHING"))
            
            # 2. Create Fake Orders
            for i in range(10):
                order_id = str(uuid.uuid4())
                price = 100000 + random.randint(-500, 500)
                conn.execute(text("""
                    INSERT INTO "order" (id, exchange_order_id, strategy_id, product_id, exchange_id, type, side, price, quantity, status, timestamp, filled_quantity, filled_price)
                    VALUES (:id, :ex_id, 'GoldenCross', 'BINANCE:BTCUSDT-PERP', 'BINANCE', 'limit', :side, :price, 0.1, 'closed', :ts, 0.1, :price)
                """), {
                    "id": order_id,
                    "ex_id": f"sim_{i}",
                    "side": random.choice(['buy', 'sell']),
                    "price": price,
                    "ts": int(time.time() * 1000) - (i * 60000)
                })
                
                # 3. Create Fake Trades
                conn.execute(text("""
                    INSERT INTO trade (id, order_id, exchange_trade_id, product_id, side, price, quantity, fee, fee_asset, timestamp)
                    VALUES (:id, :oid, :ex_tid, 'BINANCE:BTCUSDT-PERP', :side, :price, 0.1, 0.5, 'USDT', :ts)
                """), {
                    "id": str(uuid.uuid4()),
                    "oid": order_id,
                    "ex_tid": f"trd_{i}",
                    "side": random.choice(['BUY', 'SELL']),
                    "price": price,
                    "ts": int(time.time() * 1000) - (i * 60000)
                })

            # 4. Create Signal Audits
            for i in range(5):
                conn.execute(text("""
                    INSERT INTO signal_audit (id, timestamp, strategy_id, product_id, signal_type, risk_status, risk_message, order_id, details_json)
                    VALUES (:id, :ts, 'GoldenCross', 'BINANCE:BTCUSDT-PERP', 'LONG', 'PASS', 'Risk check passed', :oid, '{}')
                """), {
                    "id": i + 1,
                    "ts": int(time.time() * 1000) - (i * 300000),
                    "oid": str(uuid.uuid4())
                })
                
            conn.commit()
            print("✅ Database seeded with fake data!")
            
    except Exception as e:
        print(f"❌ Seeding failed: {e}")
        print("Note: If running outside Docker, ensure Postgres port 5432 is exposed and accessible.")

if __name__ == "__main__":
    seed()
