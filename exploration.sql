-- =====================================================================
-- exploration.sql
-- Data quality exploration of the raw ShopData views.
-- Run: sqlite3 shopdata.db < exploration.sql
-- =====================================================================
.headers on
.mode column

-- ---------------------------------------------------------------------
-- 0. Row counts / profile
-- ---------------------------------------------------------------------
SELECT 'vw_raw_customers' AS view_name, COUNT(*) AS row_count FROM vw_raw_customers
UNION ALL SELECT 'vw_raw_orders', COUNT(*) FROM vw_raw_orders
UNION ALL SELECT 'vw_exchange_rates', COUNT(*) FROM vw_exchange_rates;

-- =====================================================================
-- CUSTOMERS
-- =====================================================================

-- 1. Duplicate customer_id values (same customer appears more than once)
SELECT customer_id,
       COUNT(*)                          AS record_count,
       GROUP_CONCAT(email, ' | ')        AS emails,
       GROUP_CONCAT(signup_date, ' | ')  AS signup_dates
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 2. Missing / blank values per column
SELECT SUM(full_name   IS NULL OR TRIM(full_name) = '') AS missing_full_name,
       SUM(email       IS NULL OR TRIM(email) = '')     AS missing_email,
       SUM(phone       IS NULL OR TRIM(phone) = '')     AS missing_phone,
       SUM(signup_date IS NULL)                         AS missing_signup_date
FROM vw_raw_customers;

-- 3. Phone numbers that are not purely numeric (inconsistent formatting)
SELECT customer_id, phone,
       CASE WHEN phone GLOB '*[A-Za-z]*' THEN 'contains letters'
            ELSE 'contains symbols/spaces' END AS issue
FROM vw_raw_customers
WHERE phone IS NOT NULL
  AND phone GLOB '*[^0-9]*';

-- 4. Emails that do not look like an email address
SELECT customer_id, email
FROM vw_raw_customers
WHERE email IS NOT NULL
  AND email NOT LIKE '%_@_%._%';

-- =====================================================================
-- ORDERS
-- =====================================================================

-- 5. Non-positive order amounts (system errors)
SELECT order_id, customer_id, total_amount, status
FROM vw_raw_orders
WHERE total_amount IS NULL OR total_amount <= 0;

-- 6. Missing currency
SELECT order_id, total_amount, currency
FROM vw_raw_orders
WHERE currency IS NULL OR TRIM(currency) = '';

-- 7. Non-USD orders with no exchange rate for their order_date
SELECT o.order_id, o.order_date, o.currency, o.total_amount
FROM vw_raw_orders o
LEFT JOIN vw_exchange_rates r
       ON r.currency = o.currency
      AND r.date     = o.order_date
WHERE o.currency IS NOT NULL
  AND o.currency <> 'USD'
  AND r.rate_to_usd IS NULL;

-- 8. Exchange-rate coverage window vs. order date range
SELECT 'exchange_rates' AS source, MIN(date) AS min_date, MAX(date) AS max_date FROM vw_exchange_rates
UNION ALL
SELECT 'orders', MIN(order_date), MAX(order_date) FROM vw_raw_orders;

-- 9. Orphan orders: customer_id not present in customers
SELECT o.order_id, o.customer_id, o.total_amount, o.currency
FROM vw_raw_orders o
LEFT JOIN (SELECT DISTINCT customer_id FROM vw_raw_customers) c
       ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;

-- 10. Missing order_date
SELECT order_id, customer_id, order_date
FROM vw_raw_orders
WHERE order_date IS NULL OR TRIM(order_date) = '';

-- 11. Status distribution (non-completed orders mixed in)
SELECT status, COUNT(*) AS order_count, ROUND(SUM(total_amount), 2) AS raw_amount
FROM vw_raw_orders
GROUP BY status
ORDER BY order_count DESC;

-- 12. Duplicate order_id values
SELECT order_id, COUNT(*) AS n
FROM vw_raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1;
