"""
Ecommerce Data Cleaning Pipeline.

This module loads the raw ecommerce dataset, performs validation,
normalization, and feature transformation, and exports a cleaned,
ML-ready dataset.

Uses shared utilities from base_cleaner.py.
"""
from pathlib import Path
import pandas as pd

from src.cleaners.base_cleaner import (
    normalize_columns,
    profile_dataframe
)

RAW_DATA_PATH = Path("data/raw/ecommerce_transactions.csv")
PROCESSED_DATA_PATH = Path("data/processed/ecommerce_transactions_cleaned_base.csv")


def clean_ecommerce_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw Ecommerce dataset.

    Steps:
    - Normalizes column names to snake_case
    - Parses transaction_date to datetime
    - Validates age range and purchase_amount thresholds
    - Removes rows with invalid age or non-positive purchase_amount

    Returns:
        pd.DataFrame: Cleaned dataset ready for ML.
    """


    # Normalize column names
    df = normalize_columns(df)

    # Drop completely empty rows
    df = df.dropna(how="all")

    # Example assumptions — adjust to real column names
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # Remove rows where critical fields are missing
    critical_fields = ["order_date", "price"]
    existing_critical = [c for c in critical_fields if c in df.columns]
    df = df.dropna(subset=existing_critical)

    if "transaction_date" in df.columns:
        df["transaction_date"] = pd.to_datetime(
            df["transaction_date"],
            errors="coerce"
        )

    # Age sanity validation
    if "age" in df.columns:
        before = len(df)
        df = df[df["age"].between(10, 100)]
        print(f"Removed {before - len(df)} rows due to invalid age")

    # Purchase amount validation
    if "purchase_amount" in df.columns:
        before = len(df)
        df = df[df["purchase_amount"] > 0]
        print(f"Removed {before - len(df)} rows due to invalid purchase amount")

    return df





def main():
    print("Loading ecommerce dataset...")
    df = pd.read_csv(RAW_DATA_PATH)

    profile_dataframe(df, "RAW DATA")

    df_cleaned = clean_ecommerce_data(df)

    profile_dataframe(df_cleaned, "CLEANED DATA")

    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_cleaned.to_csv(PROCESSED_DATA_PATH, index=False)

    print(f"\nCleaned data saved to: {PROCESSED_DATA_PATH}")



if __name__ == "__main__":
    main()
