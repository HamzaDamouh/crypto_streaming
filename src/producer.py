import json
import time
import signal
import sys
import websocket
from confluent_kafka import Producer
from datetime import datetime

# Configuration
KAFKA_TOPIC = "crypto_trades"
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'linger.ms': 5,
    'compression.type': 'snappy',
    'enable.idempotence': True
}
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade"

# Global stats
stats = {
    'total_received': 0,
    'published': 0,
    'failed': 0,
    'start_time': 0,
    'end_time': 0
}

producer = None
ws = None

def delivery_report(err, msg):
    if err is not None:
        stats['failed'] += 1
        print(f"Message delivery failed: {err}")
    else:
        stats['published'] += 1

def on_message(ws, message):
    if stats['start_time'] == 0:
        stats['start_time'] = time.time()
        
    stats['total_received'] += 1
    current_time_ms = int(time.time() * 1000)
    
    try:
        data = json.loads(message)
        # Binance combined stream format: {"stream": "...", "data": {...}}
        if 'data' not in data:
            return
            
        payload = data['data']
        
        # e: event type, E: event time, s: symbol, t: trade id, p: price, q: quantity, T: trade time
        parsed_msg = {
            'symbol': payload['s'],
            'price': float(payload['p']),
            'quantity': float(payload['q']),
            'trade_id': payload['t'],
            'timestamp': payload['E'], # Binance event time ms as requested
            'ingested_at': current_time_ms
        }
        
        # Publish to Kafka
        if producer:
            producer.produce(
                KAFKA_TOPIC,
                key=parsed_msg['symbol'],
                value=json.dumps(parsed_msg),
                callback=delivery_report
            )
            producer.poll(0)
        
        # Logging every 50 messages
        if stats['total_received'] % 50 == 0:
            latency = parsed_msg['ingested_at'] - parsed_msg['timestamp']
            elapsed = time.time() - stats['start_time']
            tps = stats['total_received'] / elapsed if elapsed > 0 else 0
            print(f"Symbol: {parsed_msg['symbol']}, Price: {parsed_msg['price']}, TPS: {tps:.2f}, Latency: {latency}ms, Published: {stats['published']}")
            
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    print("WebSocket connection opened")
    stats['start_time'] = time.time()

def signal_handler(sig, frame):
    print("\nShutting down...")
    stats['end_time'] = time.time()
    if ws:
        ws.close()
    if producer:
        producer.flush()
    
    start = stats['start_time']
    if start == 0: start = time.time()
    
    elapsed = stats['end_time'] - start
    avg_tps = stats['total_received'] / elapsed if elapsed > 0 else 0
    
    print("\nFinal Stats:")
    print(f"Total Received: {stats['total_received']}")
    print(f"Total Published: {stats['published']}")
    print(f"Total Failed: {stats['failed']}")
    print(f"Runtime: {elapsed:.2f} seconds")
    print(f"Avg TPS: {avg_tps:.2f}")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        producer = Producer(KAFKA_CONF)
        print(f"Connecting to {BINANCE_WS_URL}...")
        
        # websocket.enableTrace(True)
        ws = websocket.WebSocketApp(
            BINANCE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        
        ws.run_forever(reconnect=5)
    except Exception as e:
        print(f"Main execution error: {e}")
