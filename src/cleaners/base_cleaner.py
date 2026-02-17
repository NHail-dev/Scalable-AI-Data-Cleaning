# src/cleaners/base_cleaner.py

import pandas as pd


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def profile_dataframe(df: pd.DataFrame, label: str):
    print(f"\n--- {label} PROFILE ---")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print("\nMissing values per column:")
    print(df.isna().sum())
    print("\nData types:")
    print(df.dtypes)
