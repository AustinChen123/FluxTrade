import time
import os
import redis
import json
import subprocess
from sqlalchemy import create_engine, text

# Config
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DB_URL = "postgresql://fluxtrade:fluxtrade@localhost:5432/fluxtrade"
STRATEGY_DIR = "python-strategy/strategies_hot"
STRATEGY_FILE = os.path.join(STRATEGY_DIR, "smoke_strategy.py")

# Sample Strategy Code
STRATEGY_CODE = """
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal

class SmokeStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            lookback_window=10  # Low requirement for fast testing
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        # Simple dummy signal
        return None
"""

def run_cmd(cmd, cwd=None):
    print(f"🚀 Executing: {cmd}")
    subprocess.run(cmd, shell=True, check=True, cwd=cwd)

def redis_cmd(action, **kwargs):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    msg = {"command": action, "params": kwargs}
    r.publish("cmd:strategy:control", json.dumps(msg))
    print(f"📡 Redis Pub: {msg}")
    time.sleep(1) # Wait for engine to process

def get_strategy_status(strategy_id):
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT status, performance_json FROM strategy_state WHERE strategy_id = :id"), {"id": strategy_id}).fetchone()
        return result

def main():
    print("🔥 Starting Hot-Plug Smoke Test...")
    
    # 1. Reset & Start
    print("\n--- [Step 1] Reset & Start System ---")
    run_cmd("docker rm -f fluxtrade-redis fluxtrade-db fluxtrade-rust fluxtrade-python fluxtrade-dashboard || true")
    run_cmd("docker-compose -f docker-compose.prod.yml down -v")
    
    print("⏳ Waiting for port 5432 to clear...")
    time.sleep(5)
    
    run_cmd("docker-compose -f docker-compose.prod.yml up -d")
    print("⏳ Waiting 15s for services...")
    time.sleep(15)
    
    # Run DB Migration (Ensure tables exist)
    print("🛠️ Running Migrations...")
    run_cmd("source python-strategy/.venv/bin/activate && cd database && alembic upgrade head")
    
    # Unlock System (Reset Redis State from previous crash)
    redis_cmd("SYSTEM_RESET") # This command might not exist, better set key directly
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    r.delete("system:state")
    print("🔓 System Unlocked.")

    # Restart Python Engine to pick up DB changes and recover from crash
    print("🔄 Restarting Python Engine...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    # Aggressively clear lockdown state to prevent Watchdog race condition
    # Watchdog might re-lock it while Python is restarting
    for _ in range(5):
        r.delete("system:state")
        time.sleep(1)
        
    run_cmd("docker restart fluxtrade-python")
    
    # Keep clearing it for a few seconds after restart
    for _ in range(5):
        r.delete("system:state")
        time.sleep(1)
        
    print("⏳ Waiting 10s for Engine to initialize...")
    time.sleep(10) 

    # 2. Inject Strategy
    print("\n--- [Step 2] Inject New Strategy ---")
    os.makedirs(STRATEGY_DIR, exist_ok=True)
    with open(STRATEGY_FILE, "w") as f:
        f.write(STRATEGY_CODE)
    print(f"📄 Created {STRATEGY_FILE}")

    # 3. Discovery
    print("\n--- [Step 3] Discovery Scan ---")
    redis_cmd("SCAN")
    time.sleep(2)
    redis_cmd("SCAN") # Send again to be safe
    time.sleep(2)
    
    strat_id = "smoke_strategy.py::SmokeStrategy"
    state = get_strategy_status(strat_id)
    if state and state[0] == "DISCOVERED":
        print("✅ PASS: Strategy DISCOVERED.")
    else:
        print(f"❌ FAIL: Strategy status is {state}")
        return

    # 4. Data Check (Expect Fail)
    print("\n--- [Step 4] Test Run (Expect Data Missing) ---")
    redis_cmd("TEST_RUN", id=strat_id, days=1)
    time.sleep(10) # Give it time to check DB and calculate gaps
    
    state = get_strategy_status(strat_id)
    if state and state[0] == "WARNING":
        perf_data = json.loads(state[1])
        backfill_cmd = perf_data.get("backfill_command")
        print(f"✅ PASS: Got WARNING as expected. Cmd: {backfill_cmd[:50]}...")
    else:
        print(f"❌ FAIL: Expected WARNING, got {state}")
        # Print logs to debug
        print("--- Container Logs ---")
        subprocess.run("docker logs --tail 20 fluxtrade-python", shell=True)
        return

    # 5. Backfill
    print("\n--- [Step 5] Execute Backfill ---")
    print(f"🛠️ Running: {backfill_cmd}")
    # We run it via subprocess on host, targeting the container
    subprocess.run(backfill_cmd, shell=True, check=True)
    print("✅ Backfill Done.")

    # 6. Re-Test (Expect Success)
    print("\n--- [Step 6] Test Run Again (Expect Ready) ---")
    redis_cmd("TEST_RUN", id=strat_id, days=1)
    time.sleep(3)
    
    state = get_strategy_status(strat_id)
    if state and state[0] == "READY":
        print("✅ PASS: Strategy READY.")
    else:
        print(f"❌ FAIL: Expected READY, got {state}")
        return

    # 7. Start
    print("\n--- [Step 7] Start Strategy ---")
    redis_cmd("START", id=strat_id)
    time.sleep(2)
    
    state = get_strategy_status(strat_id)
    if state and state[0] == "ACTIVE":
        print("✅ PASS: Strategy ACTIVE.")
    else:
        print(f"❌ FAIL: Expected ACTIVE, got {state}")
        return

    print("\n🎉 SMOKE TEST COMPLETED SUCCESSFULLY! 🎉")

if __name__ == "__main__":
    main()
