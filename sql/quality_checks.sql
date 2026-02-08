/* =========================================================
   Data Quality Checks
   ========================================================= */

-- 1) PK NULL checks
SELECT COUNT(*) AS null_customer_id
FROM dbo.customers
WHERE customer_id IS NULL;

SELECT COUNT(*) AS null_stock_code
FROM dbo.products
WHERE stock_code IS NULL;

SELECT COUNT(*) AS null_invoice_no
FROM dbo.orders
WHERE invoice_no IS NULL;

-- 2) Duplicate PK checks
SELECT customer_id, COUNT(*) AS cnt
FROM dbo.customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

SELECT stock_code, COUNT(*) AS cnt
FROM dbo.products
GROUP BY stock_code
HAVING COUNT(*) > 1;

SELECT invoice_no, COUNT(*) AS cnt
FROM dbo.orders
GROUP BY invoice_no
HAVING COUNT(*) > 1;

SELECT invoice_no, stock_code, COUNT(*) AS cnt
FROM dbo.order_items
GROUP BY invoice_no, stock_code
HAVING COUNT(*) > 1;

-- 3) FK orphan checks
SELECT COUNT(*) AS orphan_orders_customers
FROM dbo.orders o
LEFT JOIN dbo.customers c ON c.customer_id = o.customer_id
WHERE o.customer_id IS NOT NULL AND c.customer_id IS NULL;

SELECT COUNT(*) AS orphan_items_orders
FROM dbo.order_items oi
LEFT JOIN dbo.orders o ON o.invoice_no = oi.invoice_no
WHERE o.invoice_no IS NULL;

SELECT COUNT(*) AS orphan_items_products
FROM dbo.order_items oi
LEFT JOIN dbo.products p ON p.stock_code = oi.stock_code
WHERE p.stock_code IS NULL;

-- 4) Business rule checks (should be > 0)
SELECT COUNT(*) AS non_positive_qty
FROM dbo.order_items
WHERE quantity <= 0;

SELECT COUNT(*) AS non_positive_price
FROM dbo.products
WHERE unit_price <= 0;
