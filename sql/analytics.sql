/* =========================================================
   Analytics Queries 
   Tables:
     - dbo.customers(customer_id, country)
     - dbo.products(stock_code, description, unit_price)
     - dbo.orders(invoice_no, customer_id, invoice_date)
     - dbo.order_items(invoice_no, stock_code, quantity)
   ========================================================= */

-- 0) Row counts 
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM dbo.customers
UNION ALL
SELECT 'products', COUNT(*) FROM dbo.products
UNION ALL
SELECT 'orders', COUNT(*) FROM dbo.orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM dbo.order_items;
GO

-- 1) Top 10 products by quantity sold
SELECT TOP 10
    oi.stock_code,
    MAX(p.description) AS description,
    SUM(oi.quantity) AS total_qty
FROM dbo.order_items oi
JOIN dbo.products p
  ON p.stock_code = oi.stock_code
GROUP BY oi.stock_code
ORDER BY total_qty DESC;
GO

-- 2) Revenue by country
-- revenue = quantity * unit_price
SELECT TOP 20
    c.country,
    CAST(SUM(oi.quantity * p.unit_price) AS DECIMAL(18,2)) AS revenue
FROM dbo.orders o
JOIN dbo.customers c
  ON c.customer_id = o.customer_id
JOIN dbo.order_items oi
  ON oi.invoice_no = o.invoice_no
JOIN dbo.products p
  ON p.stock_code = oi.stock_code
GROUP BY c.country
ORDER BY revenue DESC;
GO

-- 3) Top 10 customers by revenue
SELECT TOP 10
    o.customer_id,
    MAX(c.country) AS country,
    CAST(SUM(oi.quantity * p.unit_price) AS DECIMAL(18,2)) AS revenue
FROM dbo.orders o
JOIN dbo.customers c
  ON c.customer_id = o.customer_id
JOIN dbo.order_items oi
  ON oi.invoice_no = o.invoice_no
JOIN dbo.products p
  ON p.stock_code = oi.stock_code
GROUP BY o.customer_id
ORDER BY revenue DESC;
GO

-- 4) Monthly revenue trend
SELECT
    DATEFROMPARTS(YEAR(o.invoice_date), MONTH(o.invoice_date), 1) AS month_start,
    CAST(SUM(oi.quantity * p.unit_price) AS DECIMAL(18,2)) AS revenue
FROM dbo.orders o
JOIN dbo.order_items oi
  ON oi.invoice_no = o.invoice_no
JOIN dbo.products p
  ON p.stock_code = oi.stock_code
WHERE o.invoice_date IS NOT NULL
GROUP BY DATEFROMPARTS(YEAR(o.invoice_date), MONTH(o.invoice_date), 1)
ORDER BY month_start;
GO

-- 5) Monthly number of orders 
SELECT
    DATEFROMPARTS(YEAR(invoice_date), MONTH(invoice_date), 1) AS month_start,
    COUNT(*) AS orders_count
FROM dbo.orders
WHERE invoice_date IS NOT NULL
GROUP BY DATEFROMPARTS(YEAR(invoice_date), MONTH(invoice_date), 1)
ORDER BY month_start;
GO

-- 6) Average order value by month
-- AOV = total revenue in month / number of distinct invoices in month
WITH monthly AS (
    SELECT
        DATEFROMPARTS(YEAR(o.invoice_date), MONTH(o.invoice_date), 1) AS month_start,
        o.invoice_no,
        SUM(oi.quantity * p.unit_price) AS order_revenue
    FROM dbo.orders o
    JOIN dbo.order_items oi ON oi.invoice_no = o.invoice_no
    JOIN dbo.products p ON p.stock_code = oi.stock_code
    WHERE o.invoice_date IS NOT NULL
    GROUP BY DATEFROMPARTS(YEAR(o.invoice_date), MONTH(o.invoice_date), 1), o.invoice_no
)
SELECT
    month_start,
    CAST(AVG(order_revenue) AS DECIMAL(18,2)) AS avg_order_value,
    CAST(SUM(order_revenue) AS DECIMAL(18,2)) AS revenue,
    COUNT(*) AS orders_in_month
FROM monthly
GROUP BY month_start
ORDER BY month_start;
GO

-- 7) Top 3 products per country by revenue 
WITH country_product AS (
    SELECT
        c.country,
        oi.stock_code,
        MAX(p.description) AS description,
        SUM(oi.quantity * p.unit_price) AS revenue
    FROM dbo.orders o
    JOIN dbo.customers c ON c.customer_id = o.customer_id
    JOIN dbo.order_items oi ON oi.invoice_no = o.invoice_no
    JOIN dbo.products p ON p.stock_code = oi.stock_code
    GROUP BY c.country, oi.stock_code
),
ranked AS (
    SELECT
        country,
        stock_code,
        description,
        CAST(revenue AS DECIMAL(18,2)) AS revenue,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY revenue DESC) AS rnk
    FROM country_product
)
SELECT
    country,
    stock_code,
    description,
    revenue,
    rnk
FROM ranked
WHERE rnk <= 3
ORDER BY country, rnk, revenue DESC;
GO

-- 8) Customers with most orders 
SELECT TOP 20
    o.customer_id,
    MAX(c.country) AS country,
    COUNT(DISTINCT o.invoice_no) AS orders_count
FROM dbo.orders o
JOIN dbo.customers c ON c.customer_id = o.customer_id
GROUP BY o.customer_id
ORDER BY orders_count DESC;
GO
