# Data Dictionary

## Raw Dataset: online_retail.csv

| Column Name | Data Type | Description | Notes |
|-------------|-----------|------------|-------|
| InvoiceNo   | NVARCHAR  | Unique invoice number | Repeats for multiple items per order |
| StockCode   | NVARCHAR  | Product code | Identifies a specific product |
| Description | NVARCHAR  | Product name | May contain missing values |
| Quantity    | INT       | Number of units purchased | Can be negative (returns) |
| InvoiceDate | DATETIME  | Date and time of purchase | |
| UnitPrice   | DECIMAL   | Price per unit | |
| CustomerID  | INT       | Unique customer identifier | Contains NULL values |
| Country     | NVARCHAR  | Customer country | |

## Derived Tables

### customers
- customer_id (PK)
- country

### products
- stock_code (PK)
- description
- unit_price

### orders
- invoice_no (PK)
- customer_id (FK)
- invoice_date

### order_items
- invoice_no (FK)
- stock_code (FK)
- quantity
