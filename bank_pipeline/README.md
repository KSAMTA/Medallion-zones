# Bank Dataset: Bronze → Silver → Gold Medallion Pipeline

An end-to-end medallion architecture built on **Delta Lake**, using the
[Kaggle "Bank Dataset"](https://www.kaggle.com/datasets/salikhussaini49/bank-dataset)
(5 CSVs: customers, branches, accounts, loans, transactions).

```
data/source/   raw CSVs (as downloaded from Kaggle)
lake/bronze/   raw, append-only Delta tables (1:1 with source, + lineage cols)
lake/silver/   cleaned, validated, conformed Delta tables (+ a quarantine area)
lake/gold/     dimensional star schema: 4 dimensions + 2 facts
scripts/       all pipeline code
```

## Quick start

```bash
pip install deltalake pandas pyarrow duckdb
cd scripts
python3 run_pipeline.py        # runs Bronze -> Silver -> Gold -> sample queries
```

Or run each stage individually: `bronze_ingest.py`, `silver_transform.py`,
`gold_star_schema.py`, `query_gold.py`.

## Engine choice: `deltalake` (delta-rs), not PySpark

This project uses the `deltalake` Python package — Delta Lake's native
Rust engine — via pandas/pyarrow, instead of PySpark. It reads and writes
the **exact same Delta Lake table format** (Parquet data files + a
`_delta_log` transaction log with ACID transactions, schema enforcement,
MERGE/upsert support, and time travel), so every table this project
produces is fully compatible with Spark, DuckDB, Polars, or any other
Delta-aware engine. This choice just avoids needing a JVM + Maven artifact
download to run the demo. If you're deploying on a real Spark cluster,
swap in `spark.read.format("delta")...` / `spark.write.format("delta")...`
— the transformation logic (pandas operations) maps directly onto
PySpark DataFrame operations.

## The central data-quality problem (and why it drives the design)

Profiling Bronze immediately surfaces this:

| table        | rows  | unique natural key | repeat factor |
|--------------|-------|---------------------|----------------|
| customers    | 3,000 | 1,000 CUSTOMER_ID   | 3x |
| branches     |   150 |    50 BRANCH_ID      | 3x |
| accounts     | 3,000 | 1,000 ACCOUNT_ID     | 3x |
| loans        | 1,500 |   500 LOAN_ID        | 3x |
| transactions |15,000 | 5,000 TRANSCATION_ID | 3x |

Every declared "ID" column in the raw CSVs repeats 2–3 times, **with
different attribute values each time**, and there is no timestamp
anywhere to say which occurrence is current or correct. Row-level
inspection shows this is a synthetic-data generation artifact (the file
looks like the same ID pool "tiled" 2–3 times with freshly randomized
attributes each pass), not a real upstream system's edit history.

This forces a judgment call, and **dimensions and facts get opposite
treatments** in Silver:

- **Dimensions (customers, branches, accounts):** the repeated rows are
  genuinely conflicting *versions of the same entity* — e.g. `CUSTOMER_ID
  C00001` appears with three different names/DOBs/phone numbers. With no
  way to determine which is authoritative, Silver **deterministically
  keeps the first-encountered row per key** (by original CSV row order)
  as the golden record, and routes every later conflicting occurrence to
  `lake/silver/_quarantine/<table>` with an explicit reason — rather than
  silently discarding 2/3 of the data or overwriting it arbitrarily. This
  is a **documented limitation**, not a hidden one; if the real source
  system exposes an update timestamp, this logic should be replaced with
  proper "keep latest" or SCD2 versioning.

- **Facts (loans, transactions):** row-level checks show the repeated IDs
  are *not* versions of the same event — `TRANSCATION_ID T00001` links to
  three different accounts, dates, types, and amounts. These are three
  distinct real transactions that happen to share a recycled ID string.
  Dropping them would silently delete 2/3 of all transactions. Instead,
  Silver **mints a new surrogate key** for every fact row (`transaction_id`,
  `loan_id`) and keeps the original value as `source_transaction_id` /
  `source_loan_id` — a degenerate, non-unique attribute kept for
  traceability — while retaining every row.

Other Silver-layer cleaning: type casting, text normalization
(trim/title-case), phone-number digit normalization with a
`phone_number_valid` flag, required-field checks, referential-integrity
checks against the cleaned dimensions, and valid-range/category checks
(positive amounts, known transaction media/types) — all failures are
quarantined with a `_dq_reason`, never silently dropped.

## Gold star schema

```
                    dim_date
                       |
   dim_customer   dim_account   dim_branch
        \              |            /
         \             |           /
       fact_loans  fact_transactions
```

- **dim_customer** — 1 row/customer + computed `age` and `age_band`
  (age is computed relative to the latest transaction date in the data,
  not wall-clock "today", so the star schema is reproducible on rebuild)
- **dim_branch** — 1 row/branch + a derived US Census `region`
- **dim_account** — 1 row/account; FKs to `dim_customer`, `dim_branch`
- **dim_date** — standard calendar dimension spanning the data's date range
- **fact_transactions** — grain: one row per transaction; FKs to
  `dim_account` and `dim_date`; includes `signed_amount` (Deposit = +,
  everything else = −, see assumption note in `gold_star_schema.py` about
  Transfer direction) and an `is_inflow` flag
- **fact_loans** — grain: one row per loan; FKs to `dim_customer` and
  `dim_branch`. No date FK: the source data has no loan-issue date

All facts join dimensions on integer surrogate keys (`*_sk`), not natural
keys — the standard Kimball pattern, and especially important here since
natural keys in this dataset are demonstrably unreliable.

## Sample results (`query_gold.py`)

Included queries: monthly net cash flow, transaction volume by
media/type, branch performance (transaction + loan exposure), customer
segmentation by age band, account type/status mix, and top customers by
loan exposure — each run via DuckDB directly against the Gold Delta
tables (zero-copy through PyArrow, no Spark involved), proving the tables
are engine-agnostic.

## Re-running / idempotency

- `bronze_ingest.py` tracks ingested files in `lake/bronze/_ingestion_log`;
  re-running skips files already loaded.
- `silver_transform.py` and `gold_star_schema.py` are deterministic
  full-rebuild scripts over Bronze/Silver respectively — safe to re-run
  any time Bronze changes.

## Known limitations (by design, documented rather than hidden)

- Dimension conflict resolution is "first occurrence wins" — arbitrary in
  the absence of a real timestamp; revisit if the source system ever adds one.
- Transfer transactions are modeled as an outflow from the account on
  record; the source has no counterparty-account field to model true
  double-entry transfers.
- `region` for Puerto Rico is labeled `"Territory"` rather than forced
  into a Census region, since it isn't one.
