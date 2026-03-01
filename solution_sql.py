"""
solution_sql.py
---------------
Solution 1 - Pure SQL approach.

Extracts the total quantity of each item purchased per customer aged 18-35,
then saves the result to a semicolon-delimited CSV file.
"""

import csv
import sqlite3
from pathlib import Path

from setup_database import DB_PATH, setup

OUTPUT_FILE = "output_sql.csv"

# ---------------------------------------------------------------------------
# SQL query
# ---------------------------------------------------------------------------
QUERY = """
    SELECT
        c.customer_id          AS Customer,
        c.age                  AS Age,
        i.item_name            AS Item,
        CAST(SUM(o.quantity) AS INTEGER) AS Quantity
    FROM Customer  c
    JOIN Sales     s ON s.customer_id = c.customer_id
    JOIN Orders    o ON o.sales_id    = s.sales_id
    JOIN Items     i ON i.item_id     = o.item_id
    WHERE c.age BETWEEN 18 AND 35
      AND o.quantity IS NOT NULL        -- ignore unrecorded purchases
    GROUP BY c.customer_id, i.item_id
    HAVING SUM(o.quantity) > 0          -- omit items with zero total
    ORDER BY c.customer_id, i.item_name;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_results(conn: sqlite3.Connection) -> list[tuple]:
    """Execute the SQL query and return all rows."""
    cursor = conn.cursor()
    cursor.execute(QUERY)
    return cursor.fetchall()


def save_to_csv(rows: list[tuple], output_path: str = OUTPUT_FILE) -> None:
    """Write rows to a semicolon-delimited CSV file."""
    path = Path(output_path)
    try:
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter=";")
            writer.writerow(["Customer", "Age", "Item", "Quantity"])
            writer.writerows(rows)
        print(f"[sql] Results saved to '{path.resolve()}'")
    except OSError as exc:
        print(f"[sql] File I/O error: {exc}")
        raise


def run(db_path: str = DB_PATH, output_path: str = OUTPUT_FILE) -> None:
    """Main entry point for the SQL solution."""
    try:
        conn = sqlite3.connect(db_path)
        rows = fetch_results(conn)
        print(f"[sql] {len(rows)} row(s) retrieved.")
        save_to_csv(rows, output_path)
    except sqlite3.Error as exc:
        print(f"[sql] Database error: {exc}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Ensure the database exists and is populated before querying.
    setup(DB_PATH)
    run()
