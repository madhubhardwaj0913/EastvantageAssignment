# EastvantageAssignment

Here’s a more natural, typed-out version of your assignment write‑up — less formal, more like someone documenting their project for colleagues or recruiters:

---

# Eastvantage Data Engineer Assignment

## Overview
Company XYZ ran a promo sale for items **x**, **y**, and **z**.  
The task: pull out the **total quantity of each item purchased per customer aged 18–35** from a SQLite3 database, then write the results into a semicolon‑delimited CSV file.

I built two independent solutions:

| File | Approach |
|------|----------|
| `solution_sql.py` | Pure SQL — all aggregation done inside SQLite |
| `solution_pandas.py` | Pandas — load tables as DataFrames, transform in Python |

Both scripts produce the **same CSV output**.

---

## Project Layout
```
eastvantage_assignment/
├── setup_database.py   # creates & seeds company_xyz.db
├── solution_sql.py     # solution 1: pure SQL
├── solution_pandas.py  # solution 2: Pandas
├── verify_outputs.py   # runs both solutions & compares CSVs
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Prereqs
- Python 3.10+
- pip (or any virtual env manager)

---

## Setup
```bash
# clone repo
git clone <repo-url>
cd eastvantage_assignment

# create + activate venv
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# install deps
pip install -r requirements.txt
```

---

## Running

### Option A – run both + verify
```bash
python verify_outputs.py
```
This will:
1. create/seed `company_xyz.db`
2. run SQL solution → `output_sql.csv`
3. run Pandas solution → `output_pandas.csv`
4. compare both files and confirm they match

### Option B – run individually
```bash
# SQL only
python solution_sql.py

# Pandas only
python solution_pandas.py
```

---

## Expected Output
File: `output_sql.csv` / `output_pandas.csv`  
Delimiter: `;`

```
Customer;Age;Item;Quantity
1;21;x;10
2;23;x;1
2;23;y;1
2;23;z;1
3;35;z;2
6;18;y;2
```

---

## Business Rules
| Rule | Implementation |
|------|----------------|
| Age filter 18–35 inclusive | `WHERE age BETWEEN 18 AND 35` / `df["age"].between(18, 35)` |
| NULL quantity = not purchased | `WHERE quantity IS NOT NULL` / `dropna(subset=["quantity"])` |
| Omit items with total = 0 | `HAVING SUM(quantity) > 0` / filter after groupby |
| No decimals | `CAST(... AS INTEGER)` / `.astype(int)` |
| One row per customer × item | `GROUP BY customer_id, item_id` / `groupby` |

---

## Assumptions
1. `NULL` means “not purchased.” If `quantity = 0` existed, it would also be excluded.  
2. Age boundaries are inclusive (18 and 35 count).  
3. `item_name` is used in the CSV, not `item_id`.  
4. Database is seeded fresh each run with `INSERT OR IGNORE`, so re‑runs are idempotent.


