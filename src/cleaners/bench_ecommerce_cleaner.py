"""
Benchmark Harness for Ecommerce Cleaning Pipeline.

This module measures the performance characteristics of the
`clean_ecommerce_data` function, including:

- Runtime (seconds)
- Throughput (rows per second)
- Peak memory usage (MB)
- Linear scaling behavior

The benchmark isolates cleaning logic only (excludes file I/O).
"""


import tracemalloc
import time
import pandas as pd
from pathlib import Path

from src.cleaners.ecommerce_cleaner import clean_ecommerce_data
from src.utils.benchmark import benchmark

RAW_DATA_PATH = Path("data/raw/ecommerce_transactions.csv")

@benchmark
def run_benchmark(simulate_large=True, multiplier=1):

	"""
	Executes performance benchmark for the ecommerce cleaning pipeline.

	Parameters:
		simulate_large (bool):
			If True, duplicates the dataset to simulate larger workloads.
		multiplier (int):
			Factor by which the dataset is expanded.

	Returns:
		pd.DataFrame:
			Cleaned dataframe (returned for inspection).
	 """   

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
	df_cleaned = clean_ecommerce_data(df)
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
