"""
Sample analytical queries against the Gold star schema, run with DuckDB
directly over the Delta tables' Parquet files (via PyArrow datasets) --
demonstrating that any Delta-aware engine can query what the pipeline
built, independent of the tool that wrote it.
"""
import os

import duckdb
from deltalake import DeltaTable

from common import GOLD_DIR

TABLES = ["dim_customer", "dim_branch", "dim_account", "dim_date", "fact_transactions", "fact_loans"]


def register_gold_tables(con: duckdb.DuckDBPyConnection):
    for tbl in TABLES:
        ds = DeltaTable(os.path.join(GOLD_DIR, tbl)).to_pyarrow_dataset()
        con.register(tbl, ds)


def main():
    con = duckdb.connect()
    register_gold_tables(con)

    print("\n--- Net cash flow & transaction volume by month ---")
    print(con.execute("""
        SELECT d.year, d.month,
               COUNT(*) AS transactions,
               ROUND(SUM(f.signed_amount), 2) AS net_flow,
               ROUND(SUM(CASE WHEN f.is_inflow THEN f.transaction_amount ELSE 0 END), 2) AS inflow,
               ROUND(SUM(CASE WHEN NOT f.is_inflow THEN f.transaction_amount ELSE 0 END), 2) AS outflow
        FROM fact_transactions f
        JOIN dim_date d ON f.transaction_date_sk = d.date_sk
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
        LIMIT 12
    """).df().to_string(index=False))

    print("\n--- Transaction volume by media and type ---")
    print(con.execute("""
        SELECT transaction_media, transaction_type,
               COUNT(*) AS n, ROUND(SUM(transaction_amount), 2) AS total_amount,
               ROUND(AVG(transaction_amount), 2) AS avg_amount
        FROM fact_transactions
        GROUP BY 1, 2
        ORDER BY total_amount DESC
    """).df().to_string(index=False))

    print("\n--- Branch performance: transaction volume, avg opening balance, loan exposure ---")
    print(con.execute("""
        WITH txn_by_branch AS (
            SELECT a.branch_sk, COUNT(*) AS n_transactions,
                   ROUND(SUM(f.transaction_amount), 2) AS total_txn_amount
            FROM fact_transactions f
            JOIN dim_account a ON f.account_sk = a.account_sk
            GROUP BY a.branch_sk
        ),
        loans_by_branch AS (
            SELECT branch_sk, COUNT(*) AS n_loans, ROUND(SUM(loan_amount), 2) AS total_loan_amount
            FROM fact_loans
            GROUP BY branch_sk
        )
        SELECT b.branch_name, b.branch_state, b.region,
               COALESCE(t.n_transactions, 0)      AS n_transactions,
               COALESCE(t.total_txn_amount, 0)     AS total_txn_amount,
               COALESCE(l.n_loans, 0)              AS n_loans,
               COALESCE(l.total_loan_amount, 0)    AS total_loan_amount
        FROM dim_branch b
        LEFT JOIN txn_by_branch t ON b.branch_sk = t.branch_sk
        LEFT JOIN loans_by_branch l ON b.branch_sk = l.branch_sk
        ORDER BY total_txn_amount DESC
        LIMIT 10
    """).df().to_string(index=False))

    print("\n--- Customer segmentation: revenue-equivalent activity by age band ---")
    print(con.execute("""
        SELECT c.age_band,
               COUNT(DISTINCT c.customer_sk) AS n_customers,
               COUNT(f.transaction_id)       AS n_transactions,
               ROUND(SUM(f.transaction_amount), 2) AS total_amount,
               ROUND(AVG(f.transaction_amount), 2) AS avg_txn_amount
        FROM dim_customer c
        JOIN dim_account a ON c.customer_sk = a.customer_sk
        JOIN fact_transactions f ON a.account_sk = f.account_sk
        GROUP BY c.age_band
        ORDER BY c.age_band
    """).df().to_string(index=False))

    print("\n--- Account type mix: balances and status ---")
    print(con.execute("""
        SELECT account_type, account_status,
               COUNT(*) AS n_accounts,
               ROUND(AVG(opening_balance), 2) AS avg_opening_balance,
               ROUND(SUM(opening_balance), 2) AS total_opening_balance
        FROM dim_account
        GROUP BY 1, 2
        ORDER BY account_type, account_status
    """).df().to_string(index=False))

    print("\n--- Top 10 customers by total loan exposure ---")
    print(con.execute("""
        SELECT c.customer_id, c.first_name, c.last_name, c.city, c.occupation,
               COUNT(l.loan_id) AS n_loans, ROUND(SUM(l.loan_amount), 2) AS total_loan_amount
        FROM fact_loans l
        JOIN dim_customer c ON l.customer_sk = c.customer_sk
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY total_loan_amount DESC
        LIMIT 10
    """).df().to_string(index=False))

    print("\n--- Gold layer table sizes ---")
    for tbl in TABLES:
        n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:18s}: {n} rows")


if __name__ == "__main__":
    main()
