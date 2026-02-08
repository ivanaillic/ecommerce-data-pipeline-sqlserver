import os
import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

LOG_PATH = Path("logs/pipeline.log")
SCHEMA_PATH = Path("sql/schema.sql")
PROCESSED_DIR = Path("data/processed")


def setup_logger() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
    )


def build_engine():
    load_dotenv()
    server = os.getenv("SQL_SERVER", "localhost")
    database = os.getenv("SQL_DATABASE")
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    if not database:
        raise ValueError("Missing SQL_DATABASE in .env")

    conn_str = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}"
        f"&trusted_connection=yes&Encrypt=yes&TrustServerCertificate=yes"
    )
    return create_engine(conn_str, fast_executemany=True)


def split_batches(sql_text: str):
    batches, cur = [], []
    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(cur).strip()
            if batch:
                batches.append(batch)
            cur = []
        else:
            cur.append(line)
    last = "\n".join(cur).strip()
    if last:
        batches.append(last)
    return batches


def ensure_schema(engine) -> None:
    if not SCHEMA_PATH.exists():
        logging.warning("Schema file missing: %s", SCHEMA_PATH)
        return

    sql_text = SCHEMA_PATH.read_text(encoding="utf-8")
    with engine.begin() as conn:
        for batch in split_batches(sql_text):
            conn.execute(text(batch))
    logging.info("Schema ensured.")


def get_watermark(engine) -> pd.Timestamp:
    with engine.connect() as conn:
        max_dt = conn.execute(text("SELECT MAX(invoice_date) FROM dbo.orders")).scalar()
    return pd.to_datetime(max_dt) if max_dt else pd.Timestamp("1900-01-01")


def upsert_dimension_not_exists(engine, table: str, csv_file: str, key_col: str) -> None:
    """
    NOTE: For big dims, staging+MERGE is better; this is ok for small demo datasets.
    """
    df = pd.read_csv(PROCESSED_DIR / csv_file)

    with engine.begin() as conn:
        existing = pd.read_sql(text(f"SELECT {key_col} FROM dbo.{table}"), conn)[key_col]
        df_new = df[~df[key_col].isin(existing)]

        if df_new.empty:
            logging.info("No new rows for %s.", table)
            return

        df_new.to_sql(table, conn, if_exists="append", index=False)
        logging.info("Inserted %s new rows into %s.", len(df_new), table)


def load_orders_incremental(engine) -> None:
    df = pd.read_csv(PROCESSED_DIR / "orders.csv")
    df["invoice_date"] = pd.to_datetime(df["invoice_date"])

    wm = get_watermark(engine)
    df_new = df[df["invoice_date"] > wm]

    if df_new.empty:
        logging.info("No new orders after %s.", wm)
        return

    df_new.to_sql("orders", engine, if_exists="append", index=False)
    logging.info("Inserted %s new orders.", len(df_new))


def load_order_items_no_duplicates(engine) -> None:
    df = pd.read_csv(PROCESSED_DIR / "order_items.csv")

    with engine.connect() as conn:
        existing = pd.read_sql(
            text("SELECT invoice_no, stock_code FROM dbo.order_items"),
            conn
        )

    if existing.empty:
        df.to_sql("order_items", engine, if_exists="append", index=False, chunksize=5000)
        logging.info("Inserted %s order items (table was empty).", len(df))
        return

    df["invoice_no"] = df["invoice_no"].astype(str)
    df["stock_code"] = df["stock_code"].astype(str)
    existing["invoice_no"] = existing["invoice_no"].astype(str)
    existing["stock_code"] = existing["stock_code"].astype(str)

    df_new = df.merge(existing, on=["invoice_no", "stock_code"], how="left", indicator=True)
    df_new = df_new[df_new["_merge"] == "left_only"].drop(columns=["_merge"])

    if df_new.empty:
        logging.info("No new order items.")
        return

    df_new.to_sql("order_items", engine, if_exists="append", index=False, chunksize=5000)
    logging.info("Inserted %s new order items.", len(df_new))


if __name__ == "__main__":
    setup_logger()
    engine = build_engine()

    ensure_schema(engine)

    logging.info("Starting incremental load...")
    upsert_dimension_not_exists(engine, "customers", "customers.csv", "customer_id")
    upsert_dimension_not_exists(engine, "products", "products.csv", "stock_code")

    load_orders_incremental(engine)
    load_order_items_no_duplicates(engine)
    logging.info("Done.")
