import time
from engine.lake_sink import ParquetBatchWriter
from engine.lake_query import LakeQueryEngine


def benchmark():
    print("=" * 65)
    print("KSTREAM-LAKE: PARQUET SINK & DUCKDB OLAP BENCHMARK")
    print("=" * 65)

    n_records = 50_000
    batch_size = 5_000
    writer = ParquetBatchWriter(base_directory="data/parquet_bench", batch_size=batch_size)

    # 1. Ingestion & Columnar Serialization Throughput
    print(f"\n[1/3] Micro-batching & Writing {n_records:,} events to Parquet...")
    t0 = time.perf_counter()
    for i in range(n_records):
        writer.append({
            "transaction_id": f"tx-{i:07d}",
            "user_id": f"usr-{i % 1000:04d}",
            "merchant_id": f"m-{i % 50:02d}",
            "amount": float((i % 200) + 10.5),
            "currency": "EUR",
            "country": "IE" if i % 2 == 0 else "DE",
            "timestamp": 1710720000000 + (i * 100)
        })
    writer.flush()
    write_ms = (time.perf_counter() - t0) * 1000
    throughput = n_records / (write_ms / 1000)
    print(f"  ✓ Flushed {n_records:,} records in {write_ms:.2f} ms (~{throughput:,.0f} records/sec)")

    # 2. Serverless OLAP Query Benchmark
    print("\n[2/3] Benchmarking DuckDB Analytical Query Latency...")
    engine = LakeQueryEngine(data_directory="data/parquet_bench")

    # Aggregation Query
    t0 = time.perf_counter()
    summary = engine.get_summary_metrics()
    agg_ms = (time.perf_counter() - t0) * 1000

    # Group By Query
    t0 = time.perf_counter()
    top_merchants = engine.get_top_merchants(limit=5)
    gb_ms = (time.perf_counter() - t0) * 1000

    print(f"  ✓ Aggregation Query Latency: {agg_ms:.2f} ms")
    print(f"  ✓ Top-Merchant GroupBy Query Latency: {gb_ms:.2f} ms")

    # 3. Anomaly Scan
    t0 = time.perf_counter()
    anomalies = engine.detect_high_velocity_spikes(threshold_amount=195.0)
    scan_ms = (time.perf_counter() - t0) * 1000
    print(f"  ✓ Anomaly Scan Query Latency: {scan_ms:.2f} ms ({len(anomalies)} candidates found)")

    print("\n" + "-" * 65)
    print("BENCHMARK SUMMARY:")
    print("-" * 65)
    print(f"Total Transactions Processed : {summary['total_transactions']:,}")
    print(f"Unique User Accounts Tracked : {summary['unique_users']:,}")
    print(f"Total Financial Volume       : €{summary['total_volume']:,.2f}")
    print(f"Average Ingestion Throughput : ~{throughput:,.0f} events/sec")
    print(f"OLAP Metric Query Time       : {agg_ms:.2f} ms")
    print("-" * 65)


if __name__ == "__main__":
    benchmark()
