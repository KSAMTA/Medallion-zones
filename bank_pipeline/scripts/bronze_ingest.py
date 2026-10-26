"""
BRONZE LAYER
------------
Land the five raw CSV extracts as-is into Delta tables: everything read as
strings, no cleaning, no dedup, no type casting beyond what's needed to
read the file. We only add lineage/audit columns (_source_file,
_ingested_at). Bronze's job is to preserve a faithful, replayable copy of
exactly what arrived from source -- including all of its quality problems,
which Silver will then handle deliberately and visibly rather than
silently.

An ingestion log makes re-runs idempotent: a source file already recorded
as ingested is skipped on the next run.
"""
import os
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from common import SOURCE_DIR, BRONZE_DIR, SOURCE_FILES

LOG_TABLE = os.path.join(BRONZE_DIR, "_ingestion_log")


def already_ingested(source_file: str) -> bool:
    if not os.path.exists(LOG_TABLE):
        return False
    log = DeltaTable(LOG_TABLE).to_pandas()
    return (log["source_file"] == source_file).any()


def log_ingestion(source_file: str, table: str, rows: int):
    entry = pd.DataFrame([{
        "source_file": source_file,
        "bronze_table": table,
        "row_count": rows,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }])
    mode = "append" if os.path.exists(LOG_TABLE) else "overwrite"
    write_deltalake(LOG_TABLE, entry, mode=mode)


def ingest_csv(table_name: str, filename: str):
    if already_ingested(filename):
        print(f"  [skip] {filename} already in bronze._ingestion_log")
        return

    path = os.path.join(SOURCE_DIR, filename)
    # dtype=str: Bronze does not interpret types. Even OPENING_BALANCE /
    # TRANSCATION_AMOUNT arrive as text here; Silver decides how to cast them.
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])

    df["_source_file"] = filename
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    # preserve original CSV row order -- needed later because the raw
    # duplicate-key "tiles" in this dataset only make sense in file order
    df["_source_row_num"] = range(1, len(df) + 1)

    bronze_path = os.path.join(BRONZE_DIR, table_name)
    write_deltalake(bronze_path, df, mode="overwrite", schema_mode="overwrite")

    log_ingestion(filename, table_name, len(df))
    print(f"  [ok]   {filename:28s} -> {table_name:14s} ({len(df)} rows)")


def main():
    print("=== BRONZE INGEST ===")
    for table_name, filename in SOURCE_FILES.items():
        ingest_csv(table_name, filename)

    print("\nBronze row counts:")
    for table_name in SOURCE_FILES:
        p = os.path.join(BRONZE_DIR, table_name)
        if os.path.exists(p):
            print(f"  {table_name:14s}: {len(DeltaTable(p).to_pandas())}")


if __name__ == "__main__":
    main()
