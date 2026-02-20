import json
import signal
import time
from collections import deque, defaultdict

import polars as pl
import redis
from confluent_kafka import Consumer, KafkaError


# Configuration
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cryptopulse-fast',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}
TOPIC = "crypto_trades"

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# Initialize Redis connection
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# In-memory sliding windows
buffers = defaultdict(lambda: deque(maxlen=300))
message_counts = defaultdict(int)
running = True

def signal_handler(sig, frame):
    """Gracefully shut down the consumer."""
    global running
    print("\n[Shutting down] Draining the firehose...")
    running = False

def calculate_rsi(prices: pl.Series, period: int = 14) -> float:
    """Calculates the Relative Strength Index (RSI)."""
    if len(prices) <= period:
        return 50.0

    deltas = prices.diff()
    gains = deltas.clip(lower_bound=0)
    losses = -deltas.clip(upper_bound=0)
    
    avg_gain = gains.ewm_mean(alpha=1/period, adjust=False, min_periods=period)
    avg_loss = losses.ewm_mean(alpha=1/period, adjust=False, min_periods=period)

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    latest_rsi = rsi.drop_nulls().tail(1)
    return latest_rsi.item() if not latest_rsi.is_empty() else 50.0

def push_to_redis(symbol: str, price: float, vwap_1s: float, vwap_5s: float, rsi: float, pct_change: float, tps: float):
    """Pipes the calculated metrics into Redis for downstream dashboards."""
    try:
        current_ts = int(time.time() * 1000)
        pipe = redis_client.pipeline()

        # 1. HASH: Latest Snapshot (For single-stat gauges)
        metrics_key = f"cryptopulse:metrics:{symbol}"
        snapshot = {
            "price": price,
            "vwap_1s": vwap_1s,
            "vwap_5s": vwap_5s,
            "rsi": rsi,
            "price_change_pct": pct_change,
            "tps": tps,
            "updated_at": current_ts
        }
        pipe.hset(metrics_key, mapping=snapshot)
        pipe.expire(metrics_key, 60)

        # 2. SORTED SETS: Time-Series (For line charts)
        ts_fields = ["price", "vwap_1s", "vwap_5s", "rsi", "tps"]
        
        for field in ts_fields:
            ts_key = f"cryptopulse:ts:{symbol}:{field}"
            value = snapshot[field]
            
            # Format: "{timestamp}:{value}" to guarantee uniqueness in the ZSET
            member = f"{current_ts}:{value}"
            
            pipe.zadd(ts_key, {member: current_ts})
            
            # Cap the memory usage: Keep only the last 3600 data points (approx 1 hour)
            pipe.zremrangebyrank(ts_key, 0, -3601)
            pipe.expire(ts_key, 3600)

        pipe.execute()

    except Exception as redis_err:
        print(f"Redis write failed for {symbol}: {redis_err}")

def process_batch(symbol: str, buffer: deque):
    """Calculates real-time financial indicators and pushes to cache."""
    try:
        df = pl.DataFrame(list(buffer))
        
        df = df.with_columns(
            pl.col("timestamp").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")
        )
        
        # 1-second VWAP
        vwap_1s_df = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="1s")
            .agg([
                (pl.col("price") * pl.col("quantity")).sum().alias("price_volume"),
                pl.col("quantity").sum().alias("total_volume")
            ])
            .with_columns((pl.col("price_volume") / pl.col("total_volume")).alias("vwap"))
        )
        
        # 5-second VWAP
        vwap_5s_df = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="5s")
            .agg([
                (pl.col("price") * pl.col("quantity")).sum().alias("price_volume"),
                pl.col("quantity").sum().alias("total_volume")
            ])
            .with_columns((pl.col("price_volume") / pl.col("total_volume")).alias("vwap"))
        )
        
        last_price = df["price"].tail(1).item()
        vwap_1s = vwap_1s_df["vwap"].tail(1).item() if not vwap_1s_df.is_empty() else last_price
        vwap_5s = vwap_5s_df["vwap"].tail(1).item() if not vwap_5s_df.is_empty() else last_price
        
        rsi = calculate_rsi(df["price"])
        
        time_span_s = (df["ingested_at"].max() - df["ingested_at"].min()) / 1000
        tps = len(df) / time_span_s if time_span_s > 0 else 0
        
        first_price = df["price"].head(1).item()
        pct_change = ((last_price - first_price) / first_price) * 100
        
        print(f"[{symbol:<5}] Price: ${last_price:<8.2f} | VWAP(1s): ${vwap_1s:<8.2f} | "
              f"RSI: {rsi:<4.1f} | Trend: {pct_change:>+5.2f}% | TPS: {tps:.0f}")

        # Pipe the math straight to the cache
        push_to_redis(symbol, last_price, vwap_1s, vwap_5s, rsi, pct_change, tps)

    except Exception as e:
        print(f"Failed to process {symbol} batch: {e}")

def main():
    signal.signal(signal.SIGINT, signal_handler)
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])
    
    print(f"Listening to Kafka topic '{TOPIC}'... Waiting for trades.")
    
    try:
        while running:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            symbol = data['symbol']
            
            buffers[symbol].append(data)
            message_counts[symbol] += 1
            
            if message_counts[symbol] % 10 == 0:
                process_batch(symbol, buffers[symbol])
                consumer.commit(asynchronous=True)
                
    except Exception as e:
        print(f"Fatal consumer crash: {e}")
    finally:
        print("Closing Kafka connection...")
        consumer.close()

if __name__ == "__main__":
    main()