"""
Prints the schema (columns) and a few sample rows of every table in the
Gold star schema, so you can actually SEE the dimensional model, not just
row counts.

Run from the scripts/ folder:
    python view_gold.py
"""
import os
import pandas as pd
from deltalake import DeltaTable

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)

GOLD_DIR = os.path.join(os.path.dirname(__file__), "..", "lake", "gold")

TABLES = ["dim_customer", "dim_branch", "dim_account", "dim_date",
          "fact_transactions", "fact_loans"]

for name in TABLES:
    path = os.path.join(GOLD_DIR, name)
    df = DeltaTable(path).to_pandas()
    print("\n" + "=" * 90)
    print(f"{name}   ({len(df)} rows, {len(df.columns)} columns)")
    print("=" * 90)
    print("Columns:", list(df.columns))
    print("\nSample rows:")
    print(df.head(5))