import tracemalloc
import time
import pandas as pd
from pathlib import Path

from src.cleaners.telco_churn_cleaner import clean_telco_data
from src.utils.benchmark import benchmark

RAW_DATA_PATH = Path("data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv")


@benchmark
def run_benchmark(simulate_large=True, multiplier=1):


# Load raw dataset
    df = pd.read_csv(RAW_DATA_PATH)

    # Optionally expand dataset to stress-test scaling
    if simulate_large:
        df = pd.concat([df] * multiplier, ignore_index=True)

    rows = len(df)

    # Start memory tracking
    tracemalloc.start()

    # Measure cleaning execution time
    start = time.perf_counter()
    df_cleaned = clean_telco_data(df)
    duration = time.perf_counter() - start

    # Capture peak memory usage during cleaning
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak / (1024 * 1024)

    print(f"Rows processed: {rows}")
    print(f"Cleaning time: {duration:.4f} sec")
    print(f"Throughput: {rows/duration:,.0f} rows/sec")
    print(f"Peak memory: {peak_mb:.2f} MB")

    return df_cleaned
    
    

if __name__ == "__main__":
    df_cleaned = run_benchmark(simulate_large=False)

    print(df_cleaned.head())
    print(df_cleaned.shape)

    print("\n--- Baseline ---")
    run_benchmark(simulate_large=False)

    print("\n--- x10 ---")
    run_benchmark(simulate_large=True, multiplier=10)

    print("\n--- x20 ---")
    run_benchmark(simulate_large=True, multiplier=20)
