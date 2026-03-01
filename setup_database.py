"""
setup_database.py
-----------------
Creates and seeds the SQLite3 database (company_xyz.db) with sample data
matching the ER diagram: Customer, Sales, Items, Orders.
"""

import sqlite3
import os

DB_PATH = "company_xyz.db"


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a sqlite3 connection to the given database path."""
    return sqlite3.connect(db_path)


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all four tables if they do not already exist."""
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS Customer (
            customer_id INTEGER PRIMARY KEY,
            age         INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Sales (
            sales_id    INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
        );

        CREATE TABLE IF NOT EXISTS Items (
            item_id   INTEGER PRIMARY KEY,
            item_name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS Orders (
            order_id INTEGER PRIMARY KEY,
            sales_id INTEGER NOT NULL,
            item_id  INTEGER NOT NULL,
            quantity INTEGER,          -- NULL means item was not purchased
            FOREIGN KEY (sales_id) REFERENCES Sales(sales_id),
            FOREIGN KEY (item_id)  REFERENCES Items(item_id)
        );
    """)
    conn.commit()


def seed_data(conn: sqlite3.Connection) -> None:
    """Insert sample data that covers the test-case scenario."""
    cursor = conn.cursor()

    # --- Customers (ages span inside and outside the 18-35 window) ---
    customers = [
        (1, 21),   # inside range
        (2, 23),   # inside range
        (3, 35),   # inside range (boundary)
        (4, 17),   # outside range (too young)
        (5, 36),   # outside range (too old)
        (6, 18),   # inside range (boundary)
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO Customer (customer_id, age) VALUES (?, ?)",
        customers,
    )

    # --- Items ---
    items = [(1, "x"), (2, "y"), (3, "z")]
    cursor.executemany(
        "INSERT OR IGNORE INTO Items (item_id, item_name) VALUES (?, ?)",
        items,
    )

    # --- Sales ---
    # customer_id: sales_ids
    # 1 -> 101, 102   (two trips)
    # 2 -> 103        (one trip)
    # 3 -> 104, 105   (two trips)
    # 4 -> 106        (under-age, should be excluded)
    # 5 -> 107        (over-age, should be excluded)
    # 6 -> 108        (boundary 18, should be included)
    sales = [
        (101, 1), (102, 1),
        (103, 2),
        (104, 3), (105, 3),
        (106, 4),
        (107, 5),
        (108, 6),
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO Sales (sales_id, customer_id) VALUES (?, ?)",
        sales,
    )

    # --- Orders ---
    # Clerk records ALL items per sale; NULL = not purchased.
    # Customer 1: bought x=6 (sale 101), x=4 (sale 102) → total x=10
    # Customer 2: bought x=1, y=1, z=1 once (sale 103) → total 1 each
    # Customer 3: bought z=1 (sale 104), z=1 (sale 105) → total z=2
    # Customer 4 (age 17): bought x=5 → must be excluded
    # Customer 5 (age 36): bought y=3 → must be excluded
    # Customer 6 (age 18): bought y=2, z=0(NULL) → only y should appear
    orders = [
        # sale 101 – customer 1
        (1001, 101, 1, 6),    # x=6
        (1002, 101, 2, None), # y=NULL (not bought)
        (1003, 101, 3, None), # z=NULL (not bought)
        # sale 102 – customer 1
        (1004, 102, 1, 4),    # x=4
        (1005, 102, 2, None), # y=NULL
        (1006, 102, 3, None), # z=NULL
        # sale 103 – customer 2
        (1007, 103, 1, 1),    # x=1
        (1008, 103, 2, 1),    # y=1
        (1009, 103, 3, 1),    # z=1
        # sale 104 – customer 3
        (1010, 104, 1, None), # x=NULL
        (1011, 104, 2, None), # y=NULL
        (1012, 104, 3, 1),    # z=1
        # sale 105 – customer 3
        (1013, 105, 1, None), # x=NULL
        (1014, 105, 2, None), # y=NULL
        (1015, 105, 3, 1),    # z=1
        # sale 106 – customer 4 (age 17, out of range)
        (1016, 106, 1, 5),
        (1017, 106, 2, None),
        (1018, 106, 3, None),
        # sale 107 – customer 5 (age 36, out of range)
        (1019, 107, 1, None),
        (1020, 107, 2, 3),
        (1021, 107, 3, None),
        # sale 108 – customer 6 (age 18, boundary)
        (1022, 108, 1, None), # x=NULL
        (1023, 108, 2, 2),    # y=2
        (1024, 108, 3, None), # z=NULL
    ]
    cursor.executemany(
        "INSERT OR IGNORE INTO Orders (order_id, sales_id, item_id, quantity) VALUES (?, ?, ?, ?)",
        orders,
    )

    conn.commit()


def setup(db_path: str = DB_PATH) -> None:
    """Full setup: create tables + seed data."""
    try:
        conn = get_connection(db_path)
        create_tables(conn)
        seed_data(conn)
        print(f"[setup] Database ready at '{db_path}'")
    except sqlite3.Error as exc:
        print(f"[setup] Database error: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    setup()
