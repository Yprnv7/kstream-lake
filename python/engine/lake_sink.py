import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetBatchWriter:
    """
    Micro-batch Parquet Lakehouse Sink.
    Buffers incoming Kafka clean stream records and flushes to Snappy-compressed
    partitioned Apache Parquet files by date (year=YYYY/month=MM/day=DD).
    """

    SCHEMA = pa.schema([
        ("transaction_id", pa.string()),
        ("user_id", pa.string()),
        ("merchant_id", pa.string()),
        ("amount", pa.float64()),
        ("currency", pa.string()),
        ("country", pa.string()),
        ("timestamp", pa.int64()),
        ("ingested_at", pa.timestamp("ms")),
    ])

    def __init__(self, base_directory: str = "data/parquet", batch_size: int = 1000):
        self.base_dir = Path(base_directory)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self._buffer: List[Dict[str, Any]] = []

    def append(self, event: Dict[str, Any]) -> int:
        """Appends a validated event. Flushes if batch_size reached."""
        self._buffer.append(event)
        if len(self._buffer) >= self.batch_size:
            return self.flush()
        return 0

    def flush(self) -> int:
        """Writes buffered events into partitioned Parquet files."""
        if not self._buffer:
            return 0

        # Group records by partition (date)
        partitioned: Dict[str, List[Dict[str, Any]]] = {}
        now = datetime.now(timezone.utc)

        for rec in self._buffer:
            ts = rec.get("timestamp", int(now.timestamp() * 1000))
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            partition_key = f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
            partitioned.setdefault(partition_key, []).append(rec)

        written_count = 0
        for partition, records in partitioned.items():
            target_dir = self.base_dir / partition
            target_dir.mkdir(parents=True, exist_ok=True)

            table_dict = {
                "transaction_id": [r.get("transaction_id") for r in records],
                "user_id": [r.get("user_id") for r in records],
                "merchant_id": [r.get("merchant_id") for r in records],
                "amount": [float(r.get("amount", 0.0)) for r in records],
                "currency": [r.get("currency") for r in records],
                "country": [r.get("country") for r in records],
                "timestamp": [int(r.get("timestamp", 0)) for r in records],
                "ingested_at": [now for _ in records],
            }

            table = pa.Table.from_pydict(table_dict, schema=self.SCHEMA)
            file_name = f"part-{int(time.time() * 1000)}-{os.getpid()}.parquet"
            out_path = target_dir / file_name

            pq.write_table(table, out_path, compression="snappy")
            written_count += len(records)

        self._buffer.clear()
        return written_count
