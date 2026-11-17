from pathlib import Path
from typing import List, Dict, Any, Optional
import duckdb


class LakeQueryEngine:
    """
    Serverless OLAP analytical query interface over Parquet lakehouse storage.
    Enables instant SQL querying without needing heavy cluster infrastructure.
    """

    def __init__(self, data_directory: str = "data/parquet"):
        self.data_dir = Path(data_directory)
        self.con = duckdb.connect(database=":memory:")
        self._ensure_views()

    def _ensure_views(self):
        parquet_pattern = str(self.data_dir / "**/*.parquet")
        # Register view if files exist
        if any(self.data_dir.rglob("*.parquet")):
            self.con.execute(f"""
                CREATE OR REPLACE VIEW transactions AS 
                SELECT * FROM read_parquet('{parquet_pattern}', hive_partitioning=1);
            """)

    def execute_sql(self, query: str) -> List[Dict[str, Any]]:
        self._ensure_views()
        rel = self.con.sql(query)
        columns = rel.columns
        rows = rel.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def get_summary_metrics(self) -> Dict[str, Any]:
        self._ensure_views()
        result = self.execute_sql("""
            SELECT 
                COUNT(*) as total_transactions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(SUM(amount), 2) as total_volume,
                ROUND(AVG(amount), 2) as avg_transaction_size,
                ROUND(MAX(amount), 2) as max_transaction
            FROM transactions
        """)
        return result[0] if result else {}

    def get_top_merchants(self, limit: int = 5) -> List[Dict[str, Any]]:
        return self.execute_sql(f"""
            SELECT 
                merchant_id, 
                COUNT(*) as transaction_count, 
                ROUND(SUM(amount), 2) as total_volume
            FROM transactions
            GROUP BY merchant_id
            ORDER BY total_volume DESC
            LIMIT {limit}
        """)

    def detect_high_velocity_spikes(self, threshold_amount: float = 5000.0) -> List[Dict[str, Any]]:
        return self.execute_sql(f"""
            SELECT 
                user_id, 
                COUNT(*) as count, 
                SUM(amount) as total_spent, 
                MAX(amount) as largest_tx
            FROM transactions
            WHERE amount >= {threshold_amount}
            GROUP BY user_id
            ORDER BY total_spent DESC
        """)
