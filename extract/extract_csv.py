import logging
from pathlib import Path

import pandas as pd

RAW_PATH = Path("data/raw/online_retail.csv")
LOG_PATH = Path("logs/pipeline.log")


def _setup_logger() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def extract_raw() -> pd.DataFrame:
    """
    Extract raw CSV into a DataFrame.
    """
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Raw file not found: {RAW_PATH.resolve()}")

    df = pd.read_csv(RAW_PATH, encoding="ISO-8859-1")
    return df


if __name__ == "__main__":
    _setup_logger()
    logging.info("Starting extract step...")
    df = extract_raw()
    logging.info("Extract completed.")
    logging.info("Shape: %s", df.shape)
    logging.info("Columns: %s", df.columns.tolist())
    logging.info("Null counts:\n%s", df.isna().sum().sort_values(ascending=False).head(10))

