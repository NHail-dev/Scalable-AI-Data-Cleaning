"""
Ecommerce Data Cleaning Pipeline.

This module loads the raw ecommerce dataset, performs validation,
normalization, and feature transformation, and exports a cleaned,
ML-ready dataset.

Uses shared utilities from base_cleaner.py.
"""
import logging
import time
from pathlib import Path
import pandas as pd



from src.cleaners.base_cleaner import (
    normalize_columns,
    profile_dataframe
)

logger = logging.getLogger(__name__)

RAW_DATA_PATH = Path("data/raw/ecommerce_transactions.csv")
PROCESSED_DATA_PATH = Path("data/processed/ecommerce_transactions_cleaned_base.csv")


def clean_ecommerce_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the raw Ecommerce transactions dataset.

    Transformations:
    - Standardizes column names to snake_case
    - Validates Age and Purchase Amount values
    - Ensures Transaction_Date is properly formatted
    - Applies data integrity checks for duplicates or missing entries
    - Prepares dataset for downstream ML tasks

    Returns:
        pd.DataFrame: Cleaned dataset ready for analysis or modeling.
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

    # Age sanity validation & Purchase amount validation
    
    initial_len = len(df)

    mask = pd.Series(True, index=df.index)

    if "age" in df.columns:
    	mask &= df["age"].between(10, 100)

    if "purchase_amount" in df.columns:
    	mask &= df["purchase_amount"] > 0

    df = df[mask]

    logger.info(f"Removed {initial_len - len(df)} rows due to domain validation rules")

    return df





def main():
	
	"""
	Orchestrates the Ecommerce data cleaning pipeline.
	
	Pipeline stages:
	- Load raw Ecommerce dataset
	- Profile raw data for structure and integrity
	- Apply domain-specific cleaning transformations
	- Profile cleaned data to validate results
	- Persist cleaned dataset to processed directory
	- Log execution time and pipeline metadata
	"""
	# Start execution timer for pipeline performance measurement
	start_time = time.perf_counter()
	
	logger.info("Loading ecommerce dataset...")
	df = pd.read_csv(RAW_DATA_PATH)
	
	# Profile raw dataset before transformation
	profile_dataframe(df, "RAW DATA", enabled=False)
	
	# Apply ecommerce-specific cleaning logic
	df_cleaned = clean_ecommerce_data(df)
	
	# Profile dataset after cleaning to validate transformations
	profile_dataframe(df_cleaned, "CLEANED DATA")
	
	# Ensure processed data directory exists
	PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
	
	# Persist cleaned dataset
	df_cleaned.to_csv(PROCESSED_DATA_PATH, index=False)
	
	logger.info(f"Cleaned data saved to: {PROCESSED_DATA_PATH}")
	
	# Compute total pipeline execution time
	end_time = time.perf_counter()
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
  
  
   
