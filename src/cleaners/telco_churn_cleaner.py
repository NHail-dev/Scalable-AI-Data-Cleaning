"""
Telco Customer Churn Data Cleaning Pipeline.

This module loads the raw telco churn dataset, performs validation,
normalization, and feature transformation, and exports a cleaned,
ML-ready dataset.

Uses shared utilities from base_cleaner.py.
"""
import logging
import time
from pathlib import Path
import pandas as pd
import numpy as np


from src.cleaners.base_cleaner import (
    normalize_columns,
    profile_dataframe
)

logger = logging.getLogger(__name__)

RAW_DATA_PATH = Path("data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv")
PROCESSED_DATA_PATH = Path("data/processed/telco_customer_churn_cleaned_base.csv")


def clean_telco_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw Telco churn dataset.

	Transformations:
	- Standardizes column names to lowercase
	- Coerces `TotalCharges` to numeric and drops non-convertible rows
	- Applies domain-based row filtering
	- Encodes binary Yes/No features to numeric indicators
	- Converts service-related categorical features to binary representations

	Returns:
    pd.DataFrame: Cleaned dataset ready for downstream modeling.
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
    logger.info(f"Removed {before - len(df)} rows due to invalid totalcharges")

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
	"""
	Orchestrates the full Telco Customer Churn data cleaning pipeline.
	
	Pipeline stages:
	- Load raw telco churn dataset
	- Profile raw dataset for structural validation
	- Apply domain-specific cleaning transformations
	(type coercion, invalid value handling, encoding)
	- Profile cleaned dataset
	- Persist cleaned dataset to processed directory
	- Log execution metrics
	"""
	logger.info("Loading telco churn dataset...")
	df = pd.read_csv(RAW_DATA_PATH)
	
	# Profile raw dataset before transformation
	profile_dataframe(df, "RAW DATA")
	
	# Start execution timer for pipeline performance measurement
	start_time = time.time()
	
	# Apply telco-specific cleaning logic
	df_cleaned = clean_telco_data(df)
	
	# Profile dataset after cleaning to validate transformations
	profile_dataframe(df_cleaned, "CLEANED DATA")
	
	# Ensure processed data directory exists
	PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
	
	# Persist cleaned dataset
	df_cleaned.to_csv(PROCESSED_DATA_PATH, index=False)
	
	logger.info(f"\nCleaned data saved to: {PROCESSED_DATA_PATH}")
	
	# Compute total pipeline execution time
	end_time = time.time()
	logger.info(f"Pipeline completed in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
	# Configure root logger for console output.
	# Logging is initialized only at entry point to avoid overriding
	# configuration when this module is imported elsewhere.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    main()
