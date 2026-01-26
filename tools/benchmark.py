import redis
import time
import json
import statistics
import os
import sys
from datetime import datetime

# Config
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
STREAM_KEY = "stream:market:binance:btcusdt" # We'll test on the most active pair
GROUP = "benchmark_group"
CONSUMER = "benchmarker"

def run_benchmark(duration_sec=30):
    print(f"📊 Starting Latency Benchmark on {STREAM_KEY} for {duration_sec}s...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Reset/Create Group
    try:
        r.xgroup_destroy(STREAM_KEY, GROUP)
    except: pass
    try:
        r.xgroup_create(STREAM_KEY, GROUP, id='$', mkstream=True)
    except: pass

    latencies_e2e = [] # Exchange to Consumer
    latencies_sys = [] # Redis to Consumer (Internal overhead)
    start_time = time.time()
    msg_count = 0

    try:
        while time.time() - start_time < duration_sec:
            # Read new
            messages = r.xreadgroup(GROUP, CONSUMER, {STREAM_KEY: '>'}, count=100, block=100)
            
            if not messages:
                continue
                
            now_ms = time.time() * 1000
            
            for _, msgs in messages:
                for msg_id, data in msgs:
                    msg_count += 1
                    
                    # Calculate Internal Latency (Redis ID timestamp vs Now)
                    # Redis ID: "1700000000000-0"
                    redis_ts = int(msg_id.split('-')[0])
                    sys_lat = now_ms - redis_ts
                    latencies_sys.append(sys_lat)
                    
                    # Calculate E2E Latency (Event timestamp vs Now)
                    if 'json' in data:
                        try:
                            payload = json.loads(data['json'])
                            # Binance/Backpack trade/candle usually has 'timestamp' or 'T' or 'E'
                            # Our internal model uses 'timestamp' (ms)
                            if 'timestamp' in payload:
                                event_ts = int(payload['timestamp'])
                                e2e_lat = now_ms - event_ts
                                latencies_e2e.append(e2e_lat)
                        except:
                            pass
                            
    except KeyboardInterrupt:
        pass
    
    # Stats
    print("\n" + "="*40)
    print(f"🏁 Benchmark Result (Duration: {time.time() - start_time:.2f}s)")
    print(f"📥 Total Messages Processed: {msg_count}")
    print(f"⚡ Throughput: {msg_count / duration_sec:.2f} msg/sec")
    
    if latencies_sys:
        print("\n[Internal System Latency] (Rust -> Redis -> Python)")
        print(f"  Avg: {statistics.mean(latencies_sys):.2f} ms")
        print(f"  Min: {min(latencies_sys):.2f} ms")
        print(f"  Max: {max(latencies_sys):.2f} ms")
        print(f"  P99: {statistics.quantiles(latencies_sys, n=100)[98]:.2f} ms") # 99th percentile
        
    if latencies_e2e:
        print("\n[End-to-End Latency] (Exchange -> ... -> Python)")
        print(f"  Avg: {statistics.mean(latencies_e2e):.2f} ms")
        print(f"  Min: {min(latencies_e2e):.2f} ms")
        print(f"  Max: {max(latencies_e2e):.2f} ms")
        # Note: E2E depends on clock sync, so negative values possible if local clock is behind
    print("="*40 + "\n")

if __name__ == "__main__":
    run_benchmark()
