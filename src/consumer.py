import json
import signal
from collections import deque, defaultdict


import polars as pl
from confluent_kafka import Consumer, KafkaException, KafkaError

# Config
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cryptopulse-fast',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}
TOPIC = "crypto_trades"

# In-memory SWs (max 300 trades per symbol prevent memory leaks)
buffers = defaultdict(lambda: deque(maxlen=300))
message_counts = defaultdict(int)

running = True

def signal_handler(sig, frame):
    """Ctrl+C basically"""
    global running
    print("\n Shutting down")
    running = False

# RSI helper
def calculate_rsi(prices, period=14):
    """Calculate RSI using exp moving averages"""

    if len(prices) <= period:
        return 50.0 # neutral fallback if lacking data
  

    deltas = prices.diff()
    gains = deltas.clip(lower_bound=0)
    losses = -deltas.clip(upper_bound=0)
    avg_gain = gains.slice(1, period).mean()
    avg_loss = losses.slice(1, period).mean()
    
    # Wilder's smoothing
    # TODO: To be verified with MF and epsilome
    
    avg_gain = gains.ewm_mean(alpha=1/period, adjust=False, min_periods=period)
    avg_loss = losses.ewm_mean(alpha=1/period, adjust=False, min_periods=period)

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))


    latest_rsi = rsi.drop_nulls().tail(1)
    return latest_rsi.item() if not latest_rsi.is_empty() else 50.0




def process_batch(symbol, buffer):
    """Process a batch of trades for a symbol"""
    
    try:
        df = pl.DataFrame(list(buffer))
        
        # Standardize timestamp
        df = df.with_columns(
            pl.col("timestamp").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")
        )
        
        # Calculate 1 sec and 5 sec VWAP dynamically
        
        vwap_df = (
            df.sort("timestamp")
            .group_by_dynamic("timestamp", every="1s")
            .agg([
                (pl.col("price") * pl.col("quantity")).sum().alias("price_volume"),
                pl.col("quantity").sum().alias("total_volume")
            ])
            .with_columns((pl.col("price_volume") / pl.col("total_volume")).alias("vwap"))
        )
        
        # Extract latest metrics
        last_price = df["price"].tail(1).item()
        vwap_1s = vwap_df["vwap"].tail(1).item() if not vwap_df.is_empty() else last_price
        
        rsi = calculate_rsi(df["price"])
        
        # Calculate approximate Trades Per Second (TPS)
        time_span_s = (df["ingested_at"].max() - df["ingested_at"].min()) / 1000
        tps = len(df) / time_span_s if time_span_s > 0 else 0
        
        # Calculate price movement
        first_price = df["price"].head(1).item()
        pct_change = ((last_price - first_price) / first_price) * 100
        
        print(f"[{symbol:<5}] Price: ${last_price:<8.2f} | VWAP(1s): ${vwap_1s:<8.2f} | "
              f"RSI: {rsi:<4.1f} | Trend: {pct_change:>+5.2f}% | TPS: {tps:.0f}")
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
            
            # Unpack the live trade
            data = json.loads(msg.value().decode('utf-8'))
            symbol = data['symbol']
            
            buffers[symbol].append(data)
            message_counts[symbol] += 1
            
            # Process metrics every 10 trades per symbol to avoid CPU choke
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