"""
solution_pandas.py
------------------
Solution 2 – Pandas approach.

Loads the four tables into DataFrames, performs all filtering and aggregation
in Pandas, then saves the result to a semicolon-delimited CSV file.
"""

import sqlite3
from pathlib import Path

import pandas as pd

from setup_database import DB_PATH, setup

OUTPUT_FILE = "output_pandas.csv"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_tables(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    """Load all four tables from the database into DataFrames."""
    tables = ["Customer", "Sales", "Items", "Orders"]
    return {t: pd.read_sql_query(f"SELECT * FROM {t}", conn) for t in tables}


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------

def transform(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Join, filter, and aggregate the tables to produce the final result.

    Steps
    -----
    1. Filter customers to age 18–35.
    2. Join Sales → filtered Customers.
    3. Join Orders → Sales (keep only non-NULL quantities).
    4. Join Items → Orders.
    5. Group by customer + item, sum quantities.
    6. Remove groups with zero total quantity.
    7. Cast quantity to int (no decimals).
    8. Sort and rename columns.
    """
    customer = tables["Customer"]
    sales    = tables["Sales"]
    orders   = tables["Orders"]
    items    = tables["Items"]

    # Step 1 – age filter
    cust_filtered = customer[customer["age"].between(18, 35)]

    # Step 2 – Sales ← Customer
    df = sales.merge(cust_filtered, on="customer_id")

    # Step 3 – Orders ← Sales (drop NULL quantities before aggregation)
    df = df.merge(orders, on="sales_id")
    df = df.dropna(subset=["quantity"])

    # Step 4 – Items ← Orders
    df = df.merge(items, on="item_id")

    # Step 5 – Aggregate
    df["quantity"] = pd.to_numeric(df["quantity"])
    grouped = (
        df.groupby(["customer_id", "age", "item_name"], as_index=False)["quantity"]
        .sum()
    )

    # Step 6 – Remove zero totals
    grouped = grouped[grouped["quantity"] > 0]

    # Step 7 – Integer quantities
    grouped["quantity"] = grouped["quantity"].astype(int)

    # Step 8 – Rename and sort
    result = grouped.rename(columns={
        "customer_id": "Customer",
        "age":         "Age",
        "item_name":   "Item",
        "quantity":    "Quantity",
    })
    result = result.sort_values(["Customer", "Item"]).reset_index(drop=True)

    return result


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_to_csv(df: pd.DataFrame, output_path: str = OUTPUT_FILE) -> None:
    """Write the DataFrame to a semicolon-delimited CSV file."""
    path = Path(output_path)
    try:
        df.to_csv(path, sep=";", index=False, encoding="utf-8")
        print(f"[pandas] Results saved to '{path.resolve()}'")
    except OSError as exc:
        print(f"[pandas] File I/O error: {exc}")
        raise


def run(db_path: str = DB_PATH, output_path: str = OUTPUT_FILE) -> None:
    """Main entry point for the Pandas solution."""
    try:
        conn = sqlite3.connect(db_path)
        tables = load_tables(conn)
        result = transform(tables)
        print(f"[pandas] {len(result)} row(s) retrieved.")
        save_to_csv(result, output_path)
    except sqlite3.Error as exc:
        print(f"[pandas] Database error: {exc}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup(DB_PATH)
    run()
