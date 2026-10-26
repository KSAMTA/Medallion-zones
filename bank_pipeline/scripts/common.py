"""
Shared constants for the Bank Dataset medallion pipeline.

Engine: `deltalake` (delta-rs, the Rust-native Delta Lake implementation)
via pandas/pyarrow. It reads/writes the real Delta Lake table format
(Parquet + a `_delta_log` transaction log with ACID guarantees, schema
enforcement, MERGE/upsert, time travel) -- fully compatible with Spark,
DuckDB, Polars, etc. This avoids needing a JVM + Maven download to run
the project; see README.md for the equivalent PySpark code if you're
deploying this on a real Spark cluster.
"""
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SOURCE_DIR = os.path.join(PROJECT_ROOT, "data", "source")
BRONZE_DIR = os.path.join(PROJECT_ROOT, "lake", "bronze")
SILVER_DIR = os.path.join(PROJECT_ROOT, "lake", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "lake", "gold")
QUARANTINE_DIR = os.path.join(SILVER_DIR, "_quarantine")

for d in (SOURCE_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR):
    os.makedirs(d, exist_ok=True)

# Source file -> Bronze table name
SOURCE_FILES = {
    "customers": "Bank_Customer_Data.csv",
    "branches": "Bank_Branch_Data.csv",
    "accounts": "Bank_Account_Data.csv",
    "loans": "Bank_Loan_Data.csv",
    "transactions": "Bank_Transacation_Data.csv",   # [sic] matches source filename
}
