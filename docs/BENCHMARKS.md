# Columnar Lakehouse & PyArrow Serialization Benchmark

*Evaluated on Darwin (arm64), Python 3.14.6 on 2026-11-18 13:36:12 UTC.*

## 1. Measured Serialization & Analytical Performance

| Metric | Measured Value | Architecture Layer | Notes |
| :--- | :--- | :--- | :--- |
| **PyArrow Parquet Serialization** | **226,415 records/sec** | Micro-Batch Buffer | In-process columnar vectorization & Snappy compression |
| **Serialization Latency (50k rows)** | **220.83 ms** | Micro-Batch Buffer | 10 batches of 5,000 records flushed to date partitions |
| **DuckDB Summary Aggregation** | **5.92 ms** | Analytical Query Layer | Scans Parquet directory metadata and computes metrics |
| **DuckDB Group-By Aggregation** | **2.89 ms** | Analytical Query Layer | Merchant volume ranking across 50,000 records |
| **DuckDB Anomaly Scan** | **3.45 ms** | Analytical Query Layer | Threshold filter scanning financial spikes |

## 2. Kafka Streams Java Topology Verification

- **Testing Harness:** Executed via `TopologyTestDriver` on OpenJDK 21 without a live cluster broker dependency.
- **Unit Suite:** 4 stream processing and invariant tests running in **1.65 seconds**.
- **State Store:** RocksDB backed stateful window aggregation (1-minute tumbling windows).
- **Statistical Anomaly Detection:** Rolling Z-score evaluated over windowed transactions. Any transaction exceeding 3 standard deviations ($Z \ge 3.0$) from the window mean is tagged with `anomaly_flag = true`.
