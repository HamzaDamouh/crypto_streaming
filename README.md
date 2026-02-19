# 🚀 CryptoPulse

**Real-time cryptocurrency trade streaming pipeline.**

## Architecture

```
Binance WebSocket → Kafka → ┬─ Fast Consumer → Redis  (real-time)
                             └─ Slow Consumer → Parquet (batch)
```

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Create virtual environment & install deps
python -m venv .venv
source .venv/bin/activate      # Linux / macOS
.venv\Scripts\activate         # Windows
pip install -r requirements.txt

# 3. Verify installation
python -c "import polars, confluent_kafka, redis; print('OK')"
```

## Stack

| Component        | Technology       |
|------------------|-----------------|
| Data source      | Binance WebSocket |
| Message broker   | Apache Kafka     |
| Real-time store  | Redis            |
| Batch storage    | Parquet / PyArrow |
| DataFrame engine | Polars           |

## Project Structure

```
cryptopulse/
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── producer.py
│   ├── consumer_fast.py
│   └── consumer_slow.py
└── README.md
```
