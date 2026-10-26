"""
GOLD LAYER -- star schema
-------------------------
    dim_customer   (1 row per customer, incl. computed age / age_band)
    dim_branch     (1 row per branch, incl. derived US region)
    dim_account    (1 row per account; FKs to customer & branch)
    dim_date       (standard calendar dimension)

    fact_transactions   grain: one row per transaction
                        FKs: account_sk, transaction_date_sk
    fact_loans          grain: one row per loan
                        FKs: customer_sk, branch_sk
                        (no date dimension: the source data has no loan date)

All facts join to dimensions via integer surrogate keys (sk), not the
business/natural keys -- the standard Kimball pattern.

Reference date: since this is a static historical extract (not a live
feed), "today" for age calculations is pinned to the latest transaction
date in the data, so the star schema's numbers are reproducible on every
rebuild rather than silently drifting with wall-clock time.
"""
import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from common import SILVER_DIR, GOLD_DIR


def read_silver(name: str) -> pd.DataFrame:
    return DeltaTable(os.path.join(SILVER_DIR, name)).to_pandas()


def write_gold(df: pd.DataFrame, table_name: str):
    write_deltalake(os.path.join(GOLD_DIR, table_name), df, mode="overwrite")


# ---------------------------------------------------------------------------
# DIM_CUSTOMER
# ---------------------------------------------------------------------------
def build_dim_customer(as_of: pd.Timestamp):
    print("dim_customer:")
    cust = read_silver("customers").reset_index(drop=True)
    cust["customer_sk"] = cust.index + 1

    dob = pd.to_datetime(cust["dob"])
    age_years = (as_of - dob).dt.days // 365
    cust["age"] = age_years
    cust["age_band"] = pd.cut(
        age_years, bins=[-1, 24, 34, 44, 54, 64, 200],
        labels=["Under 25", "25-34", "35-44", "45-54", "55-64", "65+"],
    ).astype(str)

    out = cust[["customer_sk", "customer_id", "first_name", "last_name",
               "city", "phone_number", "phone_number_valid", "occupation",
               "dob", "age", "age_band"]]
    write_gold(out, "dim_customer")
    print(f"    {len(out)} rows")


# ---------------------------------------------------------------------------
# DIM_BRANCH
# ---------------------------------------------------------------------------
def build_dim_branch():
    print("dim_branch:")
    br = read_silver("branches").reset_index(drop=True)
    br["branch_sk"] = br.index + 1
    out = br[["branch_sk", "branch_id", "branch_name", "branch_state", "region"]]
    write_gold(out, "dim_branch")
    print(f"    {len(out)} rows")


# ---------------------------------------------------------------------------
# DIM_ACCOUNT
# ---------------------------------------------------------------------------
def build_dim_account():
    print("dim_account:")
    acc = read_silver("accounts").reset_index(drop=True)
    dim_customer = DeltaTable(os.path.join(GOLD_DIR, "dim_customer")).to_pandas()
    dim_branch = DeltaTable(os.path.join(GOLD_DIR, "dim_branch")).to_pandas()

    acc = acc.merge(dim_customer[["customer_sk", "customer_id"]], on="customer_id", how="left")
    acc = acc.merge(dim_branch[["branch_sk", "branch_id"]], on="branch_id", how="left")
    acc["account_sk"] = acc.index + 1
    acc["account_open_date_sk"] = pd.to_datetime(acc["account_open_date"]).dt.strftime("%Y%m%d").astype(int)

    unresolved = acc["customer_sk"].isna().sum() + acc["branch_sk"].isna().sum()
    out = acc[["account_sk", "account_id", "customer_sk", "branch_sk",
              "opening_balance", "account_open_date", "account_open_date_sk",
              "account_type", "account_status"]]
    write_gold(out, "dim_account")
    print(f"    {len(out)} rows" + (f"  [WARNING: {unresolved} unresolved FK(s)]" if unresolved else ""))


# ---------------------------------------------------------------------------
# DIM_DATE
# ---------------------------------------------------------------------------
def build_dim_date(min_date, max_date):
    print("dim_date:")
    start = (pd.Timestamp(min_date) - pd.Timedelta(days=7)).normalize()
    end = (pd.Timestamp(max_date) + pd.Timedelta(days=7)).normalize()
    dates = pd.date_range(start, end, freq="D")
    dim = pd.DataFrame({
        "date_sk": dates.strftime("%Y%m%d").astype(int),
        "date": dates.date.astype(str),
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "day": dates.day,
        "day_of_week": dates.strftime("%A"),
        "is_weekend": dates.dayofweek.isin([5, 6]),
    })
    write_gold(dim, "dim_date")
    print(f"    {len(dim)} rows [{start.date()}..{end.date()}]")


# ---------------------------------------------------------------------------
# FACT_TRANSACTIONS  -- grain: one row per transaction
# ---------------------------------------------------------------------------
def build_fact_transactions():
    print("fact_transactions:")
    txn = read_silver("transactions")
    dim_account = DeltaTable(os.path.join(GOLD_DIR, "dim_account")).to_pandas()

    fact = txn.merge(dim_account[["account_sk", "account_id"]], on="account_id", how="left")
    fact["transaction_date_sk"] = pd.to_datetime(fact["transaction_date"]).dt.strftime("%Y%m%d").astype(int)

    # Business rule / assumption (documented): Deposit is money entering the
    # account (+); Withdrawal, Purchase and Transfer are modeled as money
    # leaving the account (-). The source has no counterparty-account field,
    # so a Transfer's direction can't be derived and is treated as outflow.
    fact["is_inflow"] = fact["transaction_type"] == "Deposit"
    fact["signed_amount"] = fact["transaction_amount"].where(
        fact["is_inflow"], -fact["transaction_amount"])

    unresolved = fact["account_sk"].isna().sum()
    out = fact[["transaction_id", "source_transaction_id", "account_sk",
               "transaction_date_sk", "transaction_media", "transaction_type",
               "transaction_amount", "signed_amount", "is_inflow", "transaction_date"]]
    write_gold(out, "fact_transactions")
    print(f"    {len(out)} rows" + (f"  [WARNING: {unresolved} unresolved FK(s)]" if unresolved else ""))


# ---------------------------------------------------------------------------
# FACT_LOANS  -- grain: one row per loan (no date dimension: source has no loan date)
# ---------------------------------------------------------------------------
def build_fact_loans():
    print("fact_loans:")
    loans = read_silver("loans")
    dim_customer = DeltaTable(os.path.join(GOLD_DIR, "dim_customer")).to_pandas()
    dim_branch = DeltaTable(os.path.join(GOLD_DIR, "dim_branch")).to_pandas()

    fact = loans.merge(dim_customer[["customer_sk", "customer_id"]], on="customer_id", how="left")
    fact = fact.merge(dim_branch[["branch_sk", "branch_id"]], on="branch_id", how="left")

    unresolved = fact["customer_sk"].isna().sum() + fact["branch_sk"].isna().sum()
    out = fact[["loan_id", "source_loan_id", "customer_sk", "branch_sk", "loan_amount"]]
    write_gold(out, "fact_loans")
    print(f"    {len(out)} rows" + (f"  [WARNING: {unresolved} unresolved FK(s)]" if unresolved else ""))


def main():
    print("=== GOLD: BUILD STAR SCHEMA ===")
    txn = read_silver("transactions")
    acc = read_silver("accounts")
    as_of = pd.to_datetime(txn["transaction_date"]).max()
    min_date = min(pd.to_datetime(txn["transaction_date"]).min(), pd.to_datetime(acc["account_open_date"]).min())
    max_date = max(pd.to_datetime(txn["transaction_date"]).max(), pd.to_datetime(acc["account_open_date"]).max())
    print(f"(reference 'as of' date for age calculations: {as_of.date()})")

    build_dim_customer(as_of)
    build_dim_branch()
    build_dim_account()          # depends on dim_customer, dim_branch
    build_dim_date(min_date, max_date)
    build_fact_transactions()    # depends on dim_account
    build_fact_loans()           # depends on dim_customer, dim_branch

    print("\nGold row counts:")
    for tbl in ["dim_customer", "dim_branch", "dim_account", "dim_date", "fact_transactions", "fact_loans"]:
        p = os.path.join(GOLD_DIR, tbl)
        if os.path.exists(p):
            print(f"  {tbl:18s}: {len(DeltaTable(p).to_pandas())}")


if __name__ == "__main__":
    main()
