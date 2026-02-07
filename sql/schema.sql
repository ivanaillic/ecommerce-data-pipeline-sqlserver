/* =========================================
   Ecommerce Data Pipeline - SQL Server Schema
   ========================================= */

-- 1) Create database if it doesn't exist
IF DB_ID(N'EcommerceDW') IS NULL
BEGIN
    CREATE DATABASE EcommerceDW;
END;
GO

USE EcommerceDW;
GO

/* =========================================
   2) Tables (create if not exists)
   ========================================= */

-- Customers
IF OBJECT_ID(N'dbo.customers', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.customers (
        customer_id INT NOT NULL,
        country NVARCHAR(100) NULL,
        CONSTRAINT PK_customers PRIMARY KEY (customer_id)
    );
END;
GO

-- Products
IF OBJECT_ID(N'dbo.products', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.products (
        stock_code NVARCHAR(50) NOT NULL,
        description NVARCHAR(255) NULL,
        unit_price DECIMAL(10, 2) NULL,
        CONSTRAINT PK_products PRIMARY KEY (stock_code)
    );
END;
GO

-- Orders (header)
IF OBJECT_ID(N'dbo.orders', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.orders (
        invoice_no NVARCHAR(50) NOT NULL,
        customer_id INT NULL,
        invoice_date DATETIME2(0) NULL,
        CONSTRAINT PK_orders PRIMARY KEY (invoice_no)
    );
END;
GO

-- Order Items (lines)
IF OBJECT_ID(N'dbo.order_items', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.order_items (
        invoice_no NVARCHAR(50) NOT NULL,
        stock_code NVARCHAR(50) NOT NULL,
        quantity INT NULL,
        CONSTRAINT PK_order_items PRIMARY KEY (invoice_no, stock_code)
    );
END;
GO

/* =========================================
   3) Foreign Keys (add if not exists)
   ========================================= */

-- orders.customer_id -> customers.customer_id
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = N'FK_orders_customers'
)
BEGIN
    ALTER TABLE dbo.orders
    ADD CONSTRAINT FK_orders_customers
        FOREIGN KEY (customer_id)
        REFERENCES dbo.customers (customer_id);
END;
GO

-- order_items.invoice_no -> orders.invoice_no
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = N'FK_order_items_orders'
)
BEGIN
    ALTER TABLE dbo.order_items
    ADD CONSTRAINT FK_order_items_orders
        FOREIGN KEY (invoice_no)
        REFERENCES dbo.orders (invoice_no);
END;
GO

-- order_items.stock_code -> products.stock_code
IF NOT EXISTS (
    SELECT 1
    FROM sys.foreign_keys
    WHERE name = N'FK_order_items_products'
)
BEGIN
    ALTER TABLE dbo.order_items
    ADD CONSTRAINT FK_order_items_products
        FOREIGN KEY (stock_code)
        REFERENCES dbo.products (stock_code);
END;
GO

/* =========================================
   4) Indexes (add if not exists)
   ========================================= */

-- Index on orders.customer_id (helps joins)
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_orders_customer_id'
      AND object_id = OBJECT_ID(N'dbo.orders')
)
BEGIN
    CREATE INDEX IX_orders_customer_id
    ON dbo.orders (customer_id);
END;
GO

-- Index on order_items.stock_code (helps product joins/aggregations)
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'IX_order_items_stock_code'
      AND object_id = OBJECT_ID(N'dbo.order_items')
)
BEGIN
    CREATE INDEX IX_order_items_stock_code
    ON dbo.order_items (stock_code);
END;
GO
