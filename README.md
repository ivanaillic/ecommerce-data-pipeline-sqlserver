# E-commerce Data Pipeline

## Project Overview
This project implements an end-to-end ETL pipeline for an e-commerce dataset using Python and SQL Server.  
The pipeline extracts raw CSV data, performs data cleaning and transformations, and loads the processed data into a relational database for analytical querying.

The goal of this project is to simulate a real-world data engineering workflow, including relational modeling, data validation, and incremental loading strategies.


## Architecture

Raw CSV Files  
→ Python (Extract & Transform)  
→ SQL Server (Relational Model)  
→ SQL Analytics  


## Tech Stack

- Python (pandas, sqlalchemy)
- SQL Server
- pyodbc
- Git

## Project Structure
ecommerce_data_pipeline_sqlserver/
├── data/
│ ├── raw/
│ └── processed/
├── extract/
│ └── extract_csv.py
├── transform/
│ └── transform_orders.py
├── load/
│ └── load_sqlserver.py
├── sql/
│ └── schema.sql
├── config/
│ └── db_config.yaml
├── logs/
├── README.md
├── requirements.txt
└── main.py

## ETL Flow

1. **Extract**  
   - Load raw CSV files into pandas DataFrames  
   - Validate structure and basic data quality  

2. **Transform**  
   - Clean missing values and duplicates  
   - Cast data types  
   - Apply business logic transformations  
   - Prepare data for relational modeling  

3. **Load**  
   - Insert cleaned data into SQL Server tables  
   - Maintain relational integrity (PK/FK)  

4. **Analytics**  
   - Execute SQL queries for business insights  
   - Example analyses: top customers, monthly revenue, best-selling products  

## Future Improvements

- Incremental loading (watermark strategy)
- API ingestion support
- Logging and monitoring enhancements
- Pipeline orchestration (Airflow)
- Docker containerization

