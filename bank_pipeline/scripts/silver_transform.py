"""
SILVER LAYER
------------
This dataset has one dominant data-quality problem, discovered by
profiling Bronze: every table's declared natural key is NOT actually
unique.

    customers:     3000 rows,  1000 unique CUSTOMER_ID   (3x)
    branches:       150 rows,    50 unique BRANCH_ID      (3x)
    accounts:      3000 rows,  1000 unique ACCOUNT_ID     (3x)
    loans:         1500 rows,   500 unique LOAN_ID        (3x)
    transactions: 15000 rows,  5000 unique TRANSCATION_ID (3x)

Each ID appears 2-3 times with COMPLETELY DIFFERENT attribute values each
time, and there is no timestamp column anywhere to say which occurrence is
current or correct. This is a synthetic-data generation artifact (the
source file is effectively the same ID pool "tiled" 2-3x with fresh random
attributes each pass), not a real upstream system's update history.

That forces a judgment call, and dimensions vs. facts need OPPOSITE
treatments:

  DIMENSIONS (customers, branches, accounts) -- the repeated rows are
  genuinely conflicting *versions of the same entity* (same CUSTOMER_ID,
  different name/DOB/phone each time). Silver cannot know which is
  "true", so it deterministically keeps the first-encountered row per
  natural key as the golden record and routes every later conflicting
  occurrence to a quarantine table for visibility -- rather than silently
  discarding 2/3 of the data or arbitrarily overwriting it. This is a
  documented limitation, not a hidden one.

  FACTS (loans, transactions) -- the repeated "ID" rows are NOT versions
  of the same event: row-checks show TRANSCATION_ID T00001 links to three
  different accounts, dates, types and amounts, i.e. these are three
  distinct real transactions that happen to share a recycled ID string.
  Dropping them would silently delete 2/3 of all transactions. Instead,
  Silver mints a new surrogate key for every fact row and keeps the
  original ID as `source_transaction_id` / `source_loan_id` (a degenerate
  attribute for traceability), and keeps every row.

Every table is also type-cast, text-normalized, and validated (required
fields, referential integrity against the cleaned dimensions, valid
ranges/categories), with failures quarantined rather than dropped.
"""
import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from common import BRONZE_DIR, SILVER_DIR, QUARANTINE_DIR


def read_bronze(table_name: str) -> pd.DataFrame:
    df = DeltaTable(os.path.join(BRONZE_DIR, table_name)).to_pandas()
    return df.sort_values("_source_row_num").reset_index(drop=True)


def write_silver(df: pd.DataFrame, table_name: str):
    write_deltalake(os.path.join(SILVER_DIR, table_name), df, mode="overwrite")


def quarantine(df: pd.DataFrame, table_name: str, reason: str):
    if df.empty:
        return
    df = df.copy()
    df["_dq_reason"] = reason
    path = os.path.join(QUARANTINE_DIR, table_name)
    mode = "append" if os.path.exists(path) else "overwrite"
    write_deltalake(path, df, mode=mode, schema_mode="merge")
    print(f"    quarantined {len(df)} row(s): {reason}")


def split_golden_vs_conflicting(df: pd.DataFrame, key: str):
    """First occurrence per natural key (by original source row order) is
    the golden record; every later occurrence is a conflicting duplicate."""
    df = df.copy()
    df["_version_no"] = df.groupby(key).cumcount() + 1
    golden = df[df["_version_no"] == 1].drop(columns="_version_no")
    conflicting = df[df["_version_no"] > 1].drop(columns="_version_no")
    return golden, conflicting


# ---------------------------------------------------------------------------
# CUSTOMERS  (dimension -- dedupe golden vs conflicting)
# ---------------------------------------------------------------------------
def transform_customers():
    print("Customers:")
    df = read_bronze("customers")

    bad = df[df["CUSTOMER_ID"].isna() | (df["CUSTOMER_ID"] == "")]
    df = df[~df.index.isin(bad.index)]
    quarantine(bad, "customers", "missing customer_id")

    golden, conflicting = split_golden_vs_conflicting(df, "CUSTOMER_ID")
    quarantine(conflicting, "customers", "duplicate customer_id with conflicting attributes "
                                          "(source anomaly) - later occurrence demoted")

    golden = golden.rename(columns={
        "CUSTOMER_ID": "customer_id", "First_Name": "first_name", "Last_Name": "last_name",
        "City": "city", "Phone_Number": "phone_number_raw", "Occupation": "occupation", "DOB": "dob",
    })
    for c in ["first_name", "last_name", "city", "occupation"]:
        golden[c] = golden[c].astype(str).str.strip().str.title()

    phone_digits = golden["phone_number_raw"].astype(str).str.replace(r"\D", "", regex=True)
    golden["phone_number"] = phone_digits
    golden["phone_number_valid"] = phone_digits.str.len() == 10

    golden["dob"] = pd.to_datetime(golden["dob"], errors="coerce").dt.date
    bad_dob = golden[golden["dob"].isna()]
    golden = golden[~golden.index.isin(bad_dob.index)]
    quarantine(bad_dob, "customers", "unparseable DOB")

    out = golden[["customer_id", "first_name", "last_name", "city",
                  "phone_number", "phone_number_valid", "occupation", "dob"]]
    write_silver(out, "customers")
    print(f"    silver.customers -> {len(out)} rows ({df['CUSTOMER_ID'].nunique()} unique IDs in bronze)")


# ---------------------------------------------------------------------------
# BRANCHES  (dimension -- dedupe golden vs conflicting)
# ---------------------------------------------------------------------------
def transform_branches():
    print("Branches:")
    df = read_bronze("branches")

    golden, conflicting = split_golden_vs_conflicting(df, "BRANCH_ID")
    quarantine(conflicting, "branches", "duplicate branch_id with conflicting attributes "
                                         "(source anomaly) - later occurrence demoted")

    golden = golden.rename(columns={
        "BRANCH_ID": "branch_id", "BRANCH_NAME": "branch_name", "BRANCH_STATE": "branch_state",
    })
    golden["branch_name"] = golden["branch_name"].astype(str).str.strip()
    golden["branch_state"] = golden["branch_state"].astype(str).str.strip().str.title()
    golden["region"] = golden["branch_state"].map(US_STATE_REGION).fillna("Unknown")

    out = golden[["branch_id", "branch_name", "branch_state", "region"]]
    write_silver(out, "branches")
    print(f"    silver.branches -> {len(out)} rows ({df['BRANCH_ID'].nunique()} unique IDs in bronze)")


US_STATE_REGION = {
    **{s: "Northeast" for s in ["Connecticut", "Maine", "Massachusetts", "New Hampshire", "Rhode Island",
                                  "Vermont", "New Jersey", "New York", "Pennsylvania"]},
    **{s: "Midwest" for s in ["Illinois", "Indiana", "Michigan", "Ohio", "Wisconsin", "Iowa", "Kansas",
                                "Minnesota", "Missouri", "Nebraska", "North Dakota", "South Dakota"]},
    **{s: "South" for s in ["Delaware", "Florida", "Georgia", "Maryland", "North Carolina", "South Carolina",
                              "Virginia", "West Virginia", "Alabama", "Kentucky", "Mississippi", "Tennessee",
                              "Arkansas", "Louisiana", "Oklahoma", "Texas", "District Of Columbia"]},
    **{s: "West" for s in ["Arizona", "Colorado", "Idaho", "Montana", "Nevada", "New Mexico", "Utah",
                             "Wyoming", "Alaska", "California", "Hawaii", "Oregon", "Washington"]},
    "Puerto Rico": "Territory",
}


# ---------------------------------------------------------------------------
# ACCOUNTS  (dimension -- dedupe golden vs conflicting, then validate FKs)
# ---------------------------------------------------------------------------
def transform_accounts():
    print("Accounts:")
    df = read_bronze("accounts")

    golden, conflicting = split_golden_vs_conflicting(df, "ACCOUNT_ID")
    quarantine(conflicting, "accounts", "duplicate account_id with conflicting attributes "
                                         "(source anomaly) - later occurrence demoted")

    golden = golden.rename(columns={
        "ACCOUNT_ID": "account_id", "CUSTOMER_ID": "customer_id", "BRANCH_ID": "branch_id",
        "OPENING_BALANCE": "opening_balance", "ACCOUNT_OPEN_DATE": "account_open_date",
        "ACCOUNT_TYPE": "account_type", "ACCOUNT_STATUS": "account_status",
    })
    golden["opening_balance"] = pd.to_numeric(golden["opening_balance"], errors="coerce")
    golden["account_open_date"] = pd.to_datetime(golden["account_open_date"], errors="coerce")
    golden["account_type"] = golden["account_type"].astype(str).str.strip().str.title()
    golden["account_status"] = golden["account_status"].astype(str).str.strip().str.title()

    customers = DeltaTable(os.path.join(SILVER_DIR, "customers")).to_pandas()
    branches = DeltaTable(os.path.join(SILVER_DIR, "branches")).to_pandas()

    reasons = pd.Series("", index=golden.index)
    reasons += golden["opening_balance"].isna().map(lambda x: "invalid_opening_balance;" if x else "") \
        if golden["opening_balance"].isna().any() else ""
    reasons += (golden["opening_balance"] <= 0).fillna(False).map(lambda x: "non_positive_opening_balance;" if x else "")
    reasons += golden["account_open_date"].isna().map(lambda x: "unparseable_open_date;" if x else "")
    reasons += (~golden["customer_id"].isin(customers["customer_id"])).map(lambda x: "unknown_customer_id;" if x else "")
    reasons += (~golden["branch_id"].isin(branches["branch_id"])).map(lambda x: "unknown_branch_id;" if x else "")

    bad_mask = reasons != ""
    bad = golden[bad_mask].copy()
    if not bad.empty:
        bad["_dq_reason"] = reasons[bad_mask]
        path = os.path.join(QUARANTINE_DIR, "accounts")
        mode = "append" if os.path.exists(path) else "overwrite"
        write_deltalake(path, bad, mode=mode, schema_mode="merge")
        print(f"    quarantined {len(bad)} row(s) failing validation")
    golden = golden[~bad_mask]

    out = golden[["account_id", "customer_id", "branch_id", "opening_balance",
                  "account_open_date", "account_type", "account_status"]]
    write_silver(out, "accounts")
    print(f"    silver.accounts -> {len(out)} rows ({df['ACCOUNT_ID'].nunique()} unique IDs in bronze)")


# ---------------------------------------------------------------------------
# LOANS  (fact -- every row is a distinct loan; mint a surrogate id)
# ---------------------------------------------------------------------------
def transform_loans():
    print("Loans:")
    df = read_bronze("loans").rename(columns={
        "LOAN_ID": "source_loan_id", "CUSTOMER_ID": "customer_id",
        "BRANCH_ID": "branch_id", "LOAN_AMOUNT": "loan_amount",
    })
    df["loan_amount"] = pd.to_numeric(df["loan_amount"], errors="coerce")

    customers = DeltaTable(os.path.join(SILVER_DIR, "customers")).to_pandas()
    branches = DeltaTable(os.path.join(SILVER_DIR, "branches")).to_pandas()

    reasons = pd.Series("", index=df.index)
    reasons += df["loan_amount"].isna().map(lambda x: "invalid_loan_amount;" if x else "")
    reasons += (df["loan_amount"] <= 0).fillna(False).map(lambda x: "non_positive_loan_amount;" if x else "")
    reasons += (~df["customer_id"].isin(customers["customer_id"])).map(lambda x: "unknown_customer_id;" if x else "")
    reasons += (~df["branch_id"].isin(branches["branch_id"])).map(lambda x: "unknown_branch_id;" if x else "")

    bad_mask = reasons != ""
    bad = df[bad_mask].copy()
    if not bad.empty:
        bad["_dq_reason"] = reasons[bad_mask]
        path = os.path.join(QUARANTINE_DIR, "loans")
        mode = "append" if os.path.exists(path) else "overwrite"
        write_deltalake(path, bad, mode=mode, schema_mode="merge")
        print(f"    quarantined {len(bad)} row(s) failing validation")
    df = df[~bad_mask].reset_index(drop=True)

    # mint a surrogate natural-ish key; source_loan_id is kept but is NOT unique
    df["loan_id"] = ["L" + str(i).zfill(6) for i in range(1, len(df) + 1)]

    out = df[["loan_id", "source_loan_id", "customer_id", "branch_id", "loan_amount"]]
    write_silver(out, "loans")
    print(f"    silver.loans -> {len(out)} rows (all kept: repeated LOAN_ID values are "
          f"distinct loans, see module docstring)")


# ---------------------------------------------------------------------------
# TRANSACTIONS  (fact -- every row is a distinct transaction; mint a surrogate id)
# ---------------------------------------------------------------------------
VALID_MEDIA = {"Credit_Card", "Debit_Card", "Check", "Cash"}
VALID_TYPES = {"Deposit", "Withdrawal", "Purchase", "Transfer"}


def transform_transactions():
    print("Transactions:")
    df = read_bronze("transactions").rename(columns={
        "TRANSCATION_ID": "source_transaction_id", "ACCOUNT_ID": "account_id",
        "TRANSCATION_DATE": "transaction_date", "TRANSCATION_MEDIA": "transaction_media",
        "TRANSCATION_TYPE": "transaction_type", "TRANSCATION_AMOUNT": "transaction_amount",
    })
    df["transaction_amount"] = pd.to_numeric(df["transaction_amount"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
    df["transaction_media"] = df["transaction_media"].astype(str).str.strip()
    df["transaction_type"] = df["transaction_type"].astype(str).str.strip()

    accounts = DeltaTable(os.path.join(SILVER_DIR, "accounts")).to_pandas()

    reasons = pd.Series("", index=df.index)
    reasons += df["transaction_amount"].isna().map(lambda x: "invalid_amount;" if x else "")
    reasons += (df["transaction_amount"] <= 0).fillna(False).map(lambda x: "non_positive_amount;" if x else "")
    reasons += df["transaction_date"].isna().map(lambda x: "unparseable_date;" if x else "")
    reasons += (~df["transaction_media"].isin(VALID_MEDIA)).map(lambda x: "invalid_media;" if x else "")
    reasons += (~df["transaction_type"].isin(VALID_TYPES)).map(lambda x: "invalid_type;" if x else "")
    reasons += (~df["account_id"].isin(accounts["account_id"])).map(lambda x: "unknown_account_id;" if x else "")

    bad_mask = reasons != ""
    bad = df[bad_mask].copy()
    if not bad.empty:
        bad["_dq_reason"] = reasons[bad_mask]
        path = os.path.join(QUARANTINE_DIR, "transactions")
        mode = "append" if os.path.exists(path) else "overwrite"
        write_deltalake(path, bad, mode=mode, schema_mode="merge")
        print(f"    quarantined {len(bad)} row(s) failing validation")
    df = df[~bad_mask].reset_index(drop=True)

    df["transaction_id"] = ["T" + str(i).zfill(6) for i in range(1, len(df) + 1)]

    out = df[["transaction_id", "source_transaction_id", "account_id", "transaction_date",
             "transaction_media", "transaction_type", "transaction_amount"]]
    write_silver(out, "transactions")
    print(f"    silver.transactions -> {len(out)} rows (all kept: repeated TRANSCATION_ID "
          f"values are distinct transactions, see module docstring)")


def main():
    print("=== SILVER TRANSFORM ===")
    # order matters: dimensions must be written before facts/children validate FKs against them
    transform_customers()
    transform_branches()
    transform_accounts()
    transform_loans()
    transform_transactions()

    print("\nSilver row counts:")
    for tbl in ["customers", "branches", "accounts", "loans", "transactions"]:
        p = os.path.join(SILVER_DIR, tbl)
        if os.path.exists(p):
            print(f"  {tbl:14s}: {len(DeltaTable(p).to_pandas())}")

    print("\nQuarantine row counts:")
    for tbl in ["customers", "branches", "accounts", "loans", "transactions"]:
        p = os.path.join(QUARANTINE_DIR, tbl)
        if os.path.exists(p):
            print(f"  {tbl:14s}: {len(DeltaTable(p).to_pandas())}")


if __name__ == "__main__":
    main()
