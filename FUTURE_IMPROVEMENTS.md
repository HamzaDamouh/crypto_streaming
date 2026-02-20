# Future Improvements / Brain Dump

The pipeline works fine for a PoC, but if I actually wanted to put this in production or use it for real algotrading, here are a few things I'd fix when I have the time. Feel free to grab these if you want to extend the project.

1. **Drop `print`, use `loguru`**
   Right now I'm just printing stuff to stdout to verify it works. In a real system, we need proper structured JSON logs. `loguru` is already in the requirements.txt, I just haven't hooked it up yet. It makes piping to ELK or Datadog trivial.

2. **Schema Validation with Pydantic**
   The consumer parses JSON and hopes for the best. Binance changes their WS payload structure sometimes, or drops fields if it's a weird tick. We need a strict `Pydantic` model at the boundaries so it fails loudly and cleanly if the schema drifts, instead of causing a KeyError deep inside the Polars aggregation.

3. **Proper Metrics Export**
   I'm calculating TPS and latency directly in the python scripts but it's isolated to the terminal. We should probably expose a `/metrics` Prometheus endpoint from both the producer and consumer (maybe using `prometheus_client`). Then Grafana can scrape it and we can actually alert on consumer lag or drops in ingestion throughput.

4. **Reviewing the VWAP windowing strategy**
   The current Polars sliding window implementation (`group_by_dynamic`) is basically a rolling flush. For a true HFT context where microseconds matter, we'd probably want to migrate this logic to a dedicated stream processing engine (like Flink) or write a custom cython ring-buffer if we're keeping it in Python. Polars is insanely fast but instantiating DataFrames in a tight loop has a bit of overhead. 

5. **Kafka Exactly-Once Semantics (EOS)**
   Right now the setup is just "at least once". If the consumer crashes halfway through a batch before committing offsets, we replay and double-count a few trades in the VWAP calculation on restart. Not a huge deal for a visual dashboard, but fatal if this was feeding a live execution bot. We'd need to look into transactional producers/consumers and idempotent writes.
