# src/loaders/csv_loader.py

import pandas as pd
from pathlib import Path


def load_csv(path: str) -> pd.DataFrame:
    """
    Load a CSV file into a pandas DataFrame with basic safety checks.
    """
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(file_path)

    return df
