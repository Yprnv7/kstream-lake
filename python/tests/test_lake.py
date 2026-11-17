import shutil
import pytest
from pathlib import Path
from engine.lake_sink import ParquetBatchWriter
from engine.lake_query import LakeQueryEngine


@pytest.fixture
def temp_lake_dir(tmp_path):
    data_dir = tmp_path / "lake_data"
    yield str(data_dir)
    if data_dir.exists():
        shutil.rmtree(data_dir)


def test_parquet_batch_writer_and_partitioning(temp_lake_dir):
    writer = ParquetBatchWriter(base_directory=temp_lake_dir, batch_size=2)

    e1 = {
        "transaction_id": "tx-1",
        "user_id": "usr-1",
        "merchant_id": "m-1",
        "amount": 100.0,
        "currency": "EUR",
        "country": "IE",
        "timestamp": 1710720000000 # 2024-03-18
    }
    e2 = {
        "transaction_id": "tx-2",
        "user_id": "usr-2",
        "merchant_id": "m-2",
        "amount": 250.0,
        "currency": "EUR",
        "country": "IE",
        "timestamp": 1710720000000
    }

    flushed1 = writer.append(e1)
    assert flushed1 == 0

    flushed2 = writer.append(e2)
    assert flushed2 == 2

    # Check partition directory exists
    p_files = list(Path(temp_lake_dir).rglob("*.parquet"))
    assert len(p_files) == 1


def test_duckdb_lake_query_engine(temp_lake_dir):
    writer = ParquetBatchWriter(base_directory=temp_lake_dir, batch_size=10)

    for i in range(10):
        writer.append({
            "transaction_id": f"tx-{i}",
            "user_id": f"usr-{i % 3}",
            "merchant_id": "m-tech",
            "amount": float((i + 1) * 100),
            "currency": "EUR",
            "country": "IE",
            "timestamp": 1710720000000 + (i * 1000)
        })
    writer.flush()

    engine = LakeQueryEngine(data_directory=temp_lake_dir)
    metrics = engine.get_summary_metrics()

    assert metrics["total_transactions"] == 10
    assert metrics["unique_users"] == 3
    assert metrics["total_volume"] == 5500.0
    assert metrics["max_transaction"] == 1000.0

    merchants = engine.get_top_merchants(limit=1)
    assert len(merchants) == 1
    assert merchants[0]["merchant_id"] == "m-tech"
    assert merchants[0]["transaction_count"] == 10
