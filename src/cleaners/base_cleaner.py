# src/cleaners/base_cleaner.py
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def profile_dataframe(df: pd.DataFrame, label: str):
    logger.info(f"\n--- {label} PROFILE ---")
    logger.info(f"Rows: {len(df)}")
    logger.info(f"Columns: {len(df.columns)}")
    logger.info("\nMissing values per column:")
    logger.info(df.isna().sum())
    logger.info("\nData types:")
    logger.info(df.dtypes)
