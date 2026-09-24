# ShopData ETL Pipeline

This project cleans messy order data and prepares it for a **Customer Lifetime Value (CLV)** report.

```
shopdata.db  ──►  pipeline.py (Prefect)  ──►  analytics.db  ──►  clv_report.sql
 raw data          extract → clean → load       clean data         CLV per customer
```

| File | What it does |
|------|--------------|
| `exploration.sql` | SQL queries that find problems in the raw data |
| `pipeline.py` | The ETL pipeline (Prefect flow) |
| `tests/test_pipeline.py` | Unit tests for the cleaning rules |
| `clv_report.sql` | SQL query for the CLV report |
| `requirements.txt` | Python packages to install |

---

## Quick start

You need **Python 3.12+** and **SQLite 3**.

```bash
# 1. Set up
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Run the tests
python -m pytest -v

# 3. Run the pipeline (creates analytics.db)
python pipeline.py

# 4. See the CLV report
sqlite3 -header -column analytics.db < clv_report.sql
```

To see the data problems yourself: `sqlite3 shopdata.db < exploration.sql`

---

## 1. What is wrong with the raw data?

I ran `exploration.sql` and found these problems:

### Customers (`vw_raw_customers`)

| Problem | Example |
|---------|---------|
| **Duplicate customers** | Customers 1 and 2 each appear twice |
| **Missing emails** | 2 rows have no email |
| **Messy phone numbers** | `+1 (555) 123-4567`, `(555) 333 4444`, `Ext 444`, `1-800-555-DINO` |

### Orders (`vw_raw_orders`)

| Problem | Example |
|---------|---------|
| **Negative or zero amounts** | Orders 103 (-50), 113 (-100), 114 (0) |
| **Missing currency** | Orders 107 and 116 |
| **No exchange rate for the order date** | Rates only exist for 1–5 May, but orders go up to 14 May (orders 110, 111, 115, 118, 120) |
| **Orders from an unknown customer** | Orders 106 and 118 belong to customer 99, who does not exist |
| **Missing order date** | Order 117 |
| **Mixed statuses** | Some orders are `CANCELLED` or `PENDING` |

---

## 2. How the pipeline cleans the data

The pipeline has 3 steps: **Extract → Transform → Load**.

### Extract
Read the 3 views from `shopdata.db` (read-only, so the source is never changed).

### Transform

**Customers**

| Rule | Before | After |
|------|--------|-------|
| Keep only the newest record per customer | Alice ×2 | Alice ×1 (signup 2023-06-01) |
| Keep only digits in phone numbers | `+1 (555) 123-4567` | `15551234567` |
| Fill missing emails | *(empty)* | `unknown@domain.com` |

**Orders**

| Rule | Before | After |
|------|--------|-------|
| Remove orders with amount ≤ 0 | 20 orders | 17 orders |
| Missing currency means USD | *(empty)* | `USD` |
| Convert to USD using the rate on the order date | 300 EUR on 2023-05-02 | 336.00 USD (rate 1.12) |
| No rate for that date means USD (rate 1.0) | 89 EUR on 2023-05-07 | 89.00 USD |

### Load
Save 2 tables into `analytics.db`:

| Table | Rows | Description |
|-------|------|-------------|
| `dim_customers` | 10 | One row per customer |
| `fct_orders` | 17 | One row per valid order, with `rate_to_usd` and `usd_amount` |

If writing the database fails, the pipeline saves `clean_customers.csv` and `clean_orders.csv` instead.

### How Prefect is used

| Feature | Where |
|---------|-------|
| `@flow` | `shopdata_etl` runs every step in order |
| `@task` | `extract_view`, `transform_customers`, `transform_orders`, `load` |
| Retries | `extract_view` retries 2 times if reading the database fails |
| Logging | Each task logs row counts; missing exchange rates are logged as a **WARNING** |
| Error handling | `load` falls back to CSV if the database write fails |

The cleaning logic is written as plain functions (no database, no Prefect), so it can be tested on its own.

---

## 3. Tests

```bash
python -m pytest -v     # 21 passed
```

The tests use small fake DataFrames, not the real database. They check:

- Phone numbers are turned into digits only
- Duplicates keep the newest record
- Missing or blank emails are filled
- Orders with amount ≤ 0 are removed
- Currency conversion uses the correct date's rate
- Missing currency or missing rate is treated as USD

---

## 4. CLV report

`clv_report.sql` shows each customer's total spending in USD, highest first.

| customer_id | full_name | total_orders_placed | lifetime_value_usd | customer_cohort |
|---|---|---|---|---|
| 3 | Charlie Brown | 1 | 25000.0 | 2023-03 |
| 1 | Alice Smith | 3 | 1686.0 | 2023-06 |
| 6 | Fiona Gallagher | 2 | 525.0 | 2023-05 |
| … | … | … | … | … |
| 10 | Jane Doe | 0 | 0.0 | 2023-09 |

- Customers with no orders are still listed, with 0.
- `customer_cohort` is the year and month they signed up.

---

## 5. Notes and assumptions

The assignment did not say how to handle these cases. I followed the rules as written and noted them here:

1. **Missing exchange rates give wrong numbers.** Order 115 is 25,000 JPY, but it is counted as 25,000 USD because there is no JPY rate for that day. That is why Charlie Brown is ranked first. In real life I would use the latest known rate instead (25,000 JPY ≈ 175 USD).
2. **Some phone numbers are not real.** `Ext 444` becomes `444`. These should be validated and flagged.
3. **Orders from customer 99** are kept in `fct_orders`, but they are not in the CLV report because that customer does not exist.
4. **Cancelled and pending orders** are counted, because the rules only remove amounts ≤ 0. The business may want only `COMPLETED` orders.

---

## Troubleshooting

**`pip install` fails while building `cryptography` (Intel Mac)**
```bash
pip install -r requirements.txt --only-binary cryptography
```

**`database is locked` error from `send_telemetry_heartbeat`**
This comes from Prefect's own usage tracking, not from the pipeline. The flow still finishes with `Completed()`. To turn it off:
```bash
prefect config set PREFECT_SERVER_ANALYTICS_ENABLED=false
```

**See runs and logs in the Prefect UI**
```bash
prefect server start          # then open http://127.0.0.1:4200
```
