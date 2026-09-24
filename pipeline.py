# ============================================================
# ShopData ETL Pipeline
#
# Steps:
#   1. Extract   - read data from shopdata.db
#   2. Transform - clean customers and orders
#   3. Load      - save clean tables to analytics.db
# ============================================================

import sqlite3
from pathlib import Path

import pandas as pd
from prefect import flow, task, get_run_logger


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
BASE_DIR = Path(__file__).parent
SOURCE_DB = BASE_DIR / "shopdata.db"
TARGET_DB = BASE_DIR / "analytics.db"

DEFAULT_EMAIL = "unknown@domain.com"
DEFAULT_CURRENCY = "USD"


# ============================================================
# Part A: Cleaning functions
# ============================================================

def standardize_phone(phone):
    """Keep only the digits in a phone number.
    Example: "+1 (555) 123-4567" -> "15551234567"
    """
    # No phone number -> return None
    if phone is None or pd.isna(phone):
        return None

    # Loop through each character and keep only digits
    digits = ""
    for ch in str(phone):
        if ch.isdigit():
            digits = digits + ch

    # No digits left (e.g. "N/A") -> None
    if digits == "":
        return None
    return digits


def deduplicate_customers(customers):
    """Remove duplicate customers, keeping the row with the latest signup_date.
    (remove duplicates, keep the newest signup)
    """
    df = customers.copy()

    # Convert text to date
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    # Sort dates from newest to oldest
    df = df.sort_values(by=["customer_id", "signup_date"], ascending=[True, False])

    # Keep the first row of each customer_id = the newest one
    df = df.drop_duplicates(subset="customer_id", keep="first")

    df = df.reset_index(drop=True)
    return df


def fill_missing_emails(customers, default=DEFAULT_EMAIL):
    """Replace missing emails with unknown@domain.com."""
    df = customers.copy()

    # Missing values (None / NaN) -> default
    df["email"] = df["email"].fillna(default)

    # Trim spaces, then replace blank text with the default
    df["email"] = df["email"].astype(str).str.strip()
    df.loc[df["email"] == "", "email"] = default

    return df


def clean_customers(customers):
    """Apply all customer cleaning rules -> dim_customers table."""
    # 1) Remove duplicates first, so we keep the email from the newest row
    df = deduplicate_customers(customers)

    # 2) Fill missing emails
    df = fill_missing_emails(df)

    # 3) Clean every phone number
    df["phone"] = df["phone"].apply(standardize_phone)

    # 4) Convert the date back to text (YYYY-MM-DD)
    df["signup_date"] = df["signup_date"].dt.strftime("%Y-%m-%d")

    return df[["customer_id", "full_name", "email", "phone", "signup_date"]]


def filter_invalid_orders(orders):
    """Remove orders with total_amount <= 0 (system errors)."""
    df = orders.copy()

    # Convert to numbers first; values that can't be converted become NaN
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

    # Keep only amounts greater than 0 (NaN is removed too)
    df = df[df["total_amount"] > 0]

    df = df.reset_index(drop=True)
    return df


def convert_to_usd(orders, rates):
    """Convert order amounts to USD using the rate on the order_date.
    If the currency is missing or no rate is found -> treat as USD (rate = 1.0)
    """
    df = orders.copy()

    # --- Step 1: Clean the order currency ---
    df["currency"] = df["currency"].fillna(DEFAULT_CURRENCY)
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    df.loc[df["currency"] == "", "currency"] = DEFAULT_CURRENCY

    # --- Step 2: Prepare the exchange rate table ---
    rate_table = rates.copy()
    rate_table["currency"] = rate_table["currency"].str.strip().str.upper()
    # Rename date -> order_date so the join is easy
    rate_table = rate_table.rename(columns={"date": "order_date"})
    # Remove duplicate rates so orders are not duplicated after the join
    rate_table = rate_table.drop_duplicates(subset=["currency", "order_date"])
    rate_table = rate_table[["currency", "order_date", "rate_to_usd"]]

    # --- Step 3: Join on currency + order_date ---
    df = df.merge(rate_table, on=["currency", "order_date"], how="left")

    # --- Step 4: No rate found -> use 1.0 ---
    df["rate_to_usd"] = df["rate_to_usd"].fillna(1.0)

    # --- Step 5: Calculate the USD amount ---
    df["usd_amount"] = (df["total_amount"] * df["rate_to_usd"]).round(2)

    return df


def clean_orders(orders, rates):
    """Apply all order cleaning rules -> fct_orders table."""
    # filter and convert
    df = filter_invalid_orders(orders)
    df = convert_to_usd(df, rates)

    columns = ["order_id", "customer_id", "order_date", "total_amount",
               "currency", "rate_to_usd", "usd_amount", "status"]
    return df[columns]


# ============================================================
# Part B: Prefect tasks
# ============================================================

@task(retries=2, retry_delay_seconds=5)
def extract_view(db_path, view_name):
    """Extract: read a whole view from the database."""
    logger = get_run_logger()

    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    try:
        # mode=ro = read-only
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)
        conn.close()
    except Exception as e:
        logger.error(f"Failed to read view {view_name}: {e}")
        raise  # re-raise the error so Prefect can retry

    logger.info(f"Extracted {len(df)} rows from {view_name}")
    return df


@task
def transform_customers(raw_customers):
    """Transform: clean the customer data."""
    logger = get_run_logger()

    clean = clean_customers(raw_customers)

    removed = len(raw_customers) - len(clean)
    defaulted = (clean["email"] == DEFAULT_EMAIL).sum()
    logger.info(
        f"Customers: {len(raw_customers)} raw -> {len(clean)} clean "
        f"({removed} duplicates removed, {defaulted} emails defaulted)"
    )
    return clean


@task
def transform_orders(raw_orders, rates):
    """Transform: clean the order data and convert to USD."""
    logger = get_run_logger()

    clean = clean_orders(raw_orders, rates)

    removed = len(raw_orders) - len(clean)
    logger.info(
        f"Orders: {len(raw_orders)} raw -> {len(clean)} clean "
        f"({removed} non-positive amounts removed)"
    )

    # Warn if any non-USD orders had no exchange rate
    no_rate = clean[(clean["currency"] != "USD") & (clean["rate_to_usd"] == 1.0)]
    if len(no_rate) > 0:
        logger.warning(
            f"{len(no_rate)} non-USD orders had no exchange rate for their date "
            f"and were treated as USD: order_ids={no_rate['order_id'].tolist()}"
        )
    return clean


@task
def load(customers, orders, target_db):
    """Load: save to analytics.db (if it fails -> save CSV files instead)."""
    logger = get_run_logger()

    try:
        conn = sqlite3.connect(target_db)
        # if_exists="replace" overwrites the old table, so re-running gives the same result
        customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
        orders.to_sql("fct_orders", conn, if_exists="replace", index=False)
        conn.close()
        logger.info(
            f"Loaded {len(customers)} rows into dim_customers and "
            f"{len(orders)} rows into fct_orders at {target_db}"
        )
        return str(target_db)

    except Exception as e:
        # Fallback: could not write to the database -> write CSV files instead
        logger.error(f"Could not write to {target_db}: {e}. Saving CSV instead.")
        folder = Path(target_db).parent
        customers.to_csv(folder / "clean_customers.csv", index=False)
        orders.to_csv(folder / "clean_orders.csv", index=False)
        logger.info(f"Saved clean_customers.csv and clean_orders.csv to {folder}")
        return str(folder)


# ============================================================
# Part C: Prefect flow
# ============================================================

@flow(name="shopdata-etl")
def shopdata_etl(source_db=SOURCE_DB, target_db=TARGET_DB):
    logger = get_run_logger()
    logger.info(f"Starting ShopData ETL: {source_db} -> {target_db}")

    # 1. Extract
    raw_customers = extract_view(source_db, "vw_raw_customers")
    raw_orders = extract_view(source_db, "vw_raw_orders")
    rates = extract_view(source_db, "vw_exchange_rates")

    # 2. Transform
    customers = transform_customers(raw_customers)
    orders = transform_orders(raw_orders, rates)

    # 3. Load
    result = load(customers, orders, target_db)

    logger.info("ShopData ETL finished successfully")
    return result


# Only runs when you call: python pipeline.py (not when tests import this file)
if __name__ == "__main__":
    shopdata_etl()
