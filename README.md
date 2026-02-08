# Ecommerce Data Pipeline (Python & SQL Server)

## Project Overview
This project implements an end-to-end ETL pipeline that transforms raw e-commerce CSV data into a relational Data Warehouse in SQL Server. The goal is to demonstrate a practical data engineering workflow - from ingestion and cleaning to relational modeling and SQL-based analytics.

## Tech Stack
* **Language:** Python 3.14.0
* **Data Processing:** Pandas
* **Database:** Microsoft SQL Server
* **Connectivity:** SQLAlchemy, PyODBC
* **Environment Management:** python-dotenv
* **Version Control:** Git

## Data Model
The flat CSV dataset is transformed into a relational model structured as Fact and Dimension tables to reduce redundancy and ensure referential integrity.

### Dimension Tables
* `dbo.customers`: Unique customer records and geographic data.
* `dbo.products`: Product catalog with descriptions and unit prices.

### Fact Tables
* `dbo.orders`: Invoice header level data (date, customer link).
* `dbo.order_items`: Line-item level data (quantity, product link, per-item price).

## ETL Workflow

### 1. Extract
* Reads raw CSV data from the `data/raw/` directory.
* Logs dataset shape and validates required columns to ensure data contract compliance.

### 2. Transform (Business Logic)
* **Cleaning:** Removes null CustomerID values and filters cancelled invoices (negative quantities/prices).
* **Standardization:** Formats date strings into datetime objects and casts types for consistent joins.
* **Modeling:** Separates flat data into normalized tables and ensures uniqueness in dimension records.

### 3. Load (SQL Server)
* **Schema Enforcement:** Validates and creates tables, primary keys, and foreign keys if they do not exist.
* **Incremental Loading:** Implements a **Watermark strategy** (`MAX(invoice_date)`) to load only new records.
* **Performance:** Uses `fast_executemany` for optimized bulk insert performance.
* **Idempotency:** The pipeline is designed to be safely re-run without creating duplicate records or corrupting data.

## SQL Analytics & Data Quality
The project includes specialized SQL scripts located in the `/sql` directory:

* **Business Analysis:** Monthly revenue trends, Top 10 customers, and Top 3 products per country using Window Functions (`DENSE_RANK`).
* **Data Quality Checks:** Scripts for duplicate primary key detection, foreign key validation, and null/negative value checks.

## Project Structure
```text
ecommerce_data_pipeline/
├── data/                # Raw and processed CSV files
├── extract/             # Ingestion scripts
├── transform/           # Pandas transformation logic
├── load/                # SQL Server loading logic
├── sql/                 # Schema, Analytics, Quality checks
├── .env                 # Database configuration
└── README.md            # Project documentation