"""
Telco Customer Churn Data Cleaning Pipeline.

This module loads the raw telco churn dataset, performs validation,
normalization, and feature transformation, and exports a cleaned,
ML-ready dataset.

Uses shared utilities from base_cleaner.py.
"""

from pathlib import Path
import pandas as pd
import numpy as np
from src.cleaners.base_cleaner import normalize_columns, profile_dataframe

RAW_DATA_PATH = Path("data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv")
PROCESSED_DATA_PATH = Path("data/processed/telco_customer_churn_cleaned_base.csv")


def clean_telco_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw Telco churn dataset.

    Steps:
    - Normalizes column names
    - Coerces totalcharges to numeric and removes invalid entries
    - Removes invalid rows
    - Encodes categorical Yes/No features
    - Encodes service-related categorical features into binary indicators

    Returns:
        pd.DataFrame: Cleaned dataset ready for ML.
    """
    df = normalize_columns(df)

    # Convert empty strings to NaN
    df = df.replace(r"^\s*$", np.nan, regex=True)

    # Fix TotalCharges numeric casting
    if "totalcharges" in df.columns:
        df["totalcharges"] = pd.to_numeric(df["totalcharges"], errors="coerce")

    # Drop rows with invalid totalcharges
    before = len(df)
    df = df.dropna(subset=["totalcharges"])
    print(f"Removed {before - len(df)} rows due to invalid totalcharges")

    # Yes/No columns to 0/1
    yes_no_columns = [
        "partner", "dependents", "phoneservice",
        "paperlessbilling", "churn"
    ]
    for col in yes_no_columns:
        if col in df.columns:
            df[col] = df[col].map({"Yes": 1, "No": 0})

    # Service columns with "No internet/phone service" normalization
    service_columns = [
        "multiplelines", "onlinesecurity", "onlinebackup",
        "deviceprotection", "techsupport",
        "streamingtv", "streamingmovies"
    ]
    for col in service_columns:
        if col in df.columns:
            df[col] = df[col].replace({
                "No internet service": "No",
                "No phone service": "No"
            })
            df[col] = df[col].map({"Yes": 1, "No": 0})

    # SeniorCitizen to boolean
    if "seniorcitizen" in df.columns:
        df["seniorcitizen"] = df["seniorcitizen"].astype(bool)

    return df


def main():
    print("Loading telco churn dataset...")
    df = pd.read_csv(RAW_DATA_PATH)

    profile_dataframe(df, "RAW DATA")

    df_cleaned = clean_telco_data(df)

    profile_dataframe(df_cleaned, "CLEANED DATA")

    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_cleaned.to_csv(PROCESSED_DATA_PATH, index=False)

    print(f"\nCleaned data saved to: {PROCESSED_DATA_PATH}")


if __name__ == "__main__":
    main()
