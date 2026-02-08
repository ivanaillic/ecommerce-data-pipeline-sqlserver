import logging
from pathlib import Path
from typing import Dict

import pandas as pd

PROCESSED_DIR = Path("data/processed")
LOG_PATH = Path("logs/pipeline.log")


def setup_logger() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df


def transform(raw_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
      - customers(customer_id, country)
      - products(stock_code, description, unit_price)
      - orders(invoice_no, customer_id, invoice_date)
      - order_items(invoice_no, stock_code, quantity)
    """
    df = standardize_columns(raw_df)

    required = {"InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # 1) Drop null customers 
    df = df.dropna(subset=["CustomerID"]).copy()

    # 2) Parse types
    df["CustomerID"] = df["CustomerID"].astype(int)

    # dayfirst=True 
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["InvoiceDate"]).copy()

    # 3) Remove cancellations 
    df["InvoiceNo"] = df["InvoiceNo"].astype(str)
    df = df[~df["InvoiceNo"].str.startswith("C")].copy()

    # 4) Remove invalid rows 
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)].copy()

    if "Description" in df.columns:
        df["Description"] = df["Description"].astype("string").str.strip()

    # ------------------------
    # TABLES
    # ------------------------

    # customers
    df_customers = (
    df[["CustomerID", "Country", "InvoiceDate"]]
    .sort_values(["CustomerID", "InvoiceDate"])
    .groupby("CustomerID", as_index=False)
    .agg(country=("Country", "last"))
    .rename(columns={"CustomerID": "customer_id"})
    .sort_values("customer_id")
    .reset_index(drop=True)
    )


    # products
    df_products_tmp = df[["StockCode", "Description", "UnitPrice", "InvoiceDate"]].copy()
    df_products_tmp = df_products_tmp.sort_values(["StockCode", "InvoiceDate"])

    df_products = (
        df_products_tmp.groupby("StockCode", as_index=False)
        .agg(
            description=("Description", "last"),
            unit_price=("UnitPrice", "last"),
        )
        .rename(columns={"StockCode": "stock_code"})
        .reset_index(drop=True)
    )

    # orders (header)
    df_orders = (
    df[["InvoiceNo", "CustomerID", "InvoiceDate"]]
    .sort_values(["InvoiceNo", "InvoiceDate"])
    .groupby("InvoiceNo", as_index=False)
    .agg(
        customer_id=("CustomerID", "first"),
        invoice_date=("InvoiceDate", "min"),
    )
    .rename(columns={"InvoiceNo": "invoice_no"})
    .sort_values("invoice_no")
    .reset_index(drop=True)
    )


    # order_items (lines)
    df_items = df[["InvoiceNo", "StockCode", "Quantity"]].rename(
        columns={"InvoiceNo": "invoice_no", "StockCode": "stock_code", "Quantity": "quantity"}
    )

    df_order_items = (
        df_items.groupby(["invoice_no", "stock_code"], as_index=False)
        .agg(quantity=("quantity", "sum"))
        .sort_values(["invoice_no", "stock_code"])
        .reset_index(drop=True)
    )

    return {
        "customers": df_customers,
        "products": df_products,
        "orders": df_orders,
        "order_items": df_order_items,
    }


def run_quality_checks(tables: Dict[str, pd.DataFrame]) -> None:
    customers = tables["customers"]
    products = tables["products"]
    orders = tables["orders"]
    items = tables["order_items"]

    # PK checks
    assert customers["customer_id"].notna().all(), "customers.customer_id contains NULL"
    assert customers["customer_id"].is_unique, "customers.customer_id is not unique"

    assert products["stock_code"].notna().all(), "products.stock_code contains NULL"
    assert products["stock_code"].is_unique, "products.stock_code is not unique"

    assert orders["invoice_no"].notna().all(), "orders.invoice_no contains NULL"
    assert orders["invoice_no"].is_unique, "orders.invoice_no is not unique"

    # Composite PK check
    assert items[["invoice_no", "stock_code"]].notna().all().all(), "order_items keys contain NULL"
    assert items.duplicated(["invoice_no", "stock_code"]).sum() == 0, "Duplicate (invoice_no, stock_code) in order_items"

    # FK checks 
    assert orders["customer_id"].isin(customers["customer_id"]).all(), "orders has customer_id not in customers"
    assert items["invoice_no"].isin(orders["invoice_no"]).all(), "order_items has invoice_no not in orders"
    assert items["stock_code"].isin(products["stock_code"]).all(), "order_items has stock_code not in products"

    logging.info("Data quality checks passed.")


def save_processed(tables: Dict[str, pd.DataFrame]) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        out_path = PROCESSED_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        logging.info("Saved %s -> %s (rows=%s)", name, out_path.as_posix(), len(df))


if __name__ == "__main__":
    setup_logger()

    raw = pd.read_csv("data/raw/online_retail.csv", encoding="ISO-8859-1")
    tables = transform(raw)

    for name, tdf in tables.items():
        logging.info("%s shape: %s", name, tdf.shape)
        logging.info("%s head:\n%s", name, tdf.head(3))

    run_quality_checks(tables)
    save_processed(tables)
