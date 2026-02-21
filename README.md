# Scalable AI Data Cleaning Framework

A modular, benchmarked, and performance-optimized data cleaning framework for structured datasets.

This project demonstrates production-oriented data preprocessing pipelines with measurable performance, memory profiling, and clean architecture design.

---

## Overview

Real-world datasets are rarely clean. They contain:

- Inconsistent column naming
- Invalid numeric fields
- Mixed categorical formats
- Missing values
- Binary features stored as strings
- No performance guarantees

This framework provides:

- Deterministic cleaning pipelines
- Modular architecture
- Logging integration
- Throughput benchmarking
- Memory profiling
- Linear scalability validation

---

## Project Structure

```
src/
  cleaners/
    base_cleaner.py
    ecommerce_cleaner.py
    telco_churn_cleaner.py

  benchmarks/
    bench_ecommerce_cleaner.py
    bench_telco_churn_cleaner.py
```

### Cleaners
Contain transformation logic and reusable cleaning pipelines.

### Benchmarks
Measure:
- Execution time
- Rows per second
- Peak memory usage
- Scaling behavior (x10, x20 simulations)

---

## Features

- Column normalization
- Type coercion
- Missing value handling
- Binary encoding (`uint8` optimized)
- Categorical normalization
- Structured logging
- Memory tracking via `tracemalloc`
- Throughput measurement (rows/sec)
- Linear scaling validation

---

## Benchmark Results

### Ecommerce Dataset

| Rows       | Time (sec) | Throughput         |
|------------|------------|--------------------|
| 50,000     | ~0.03      | ~1.5M rows/sec     |
| 500,000    | ~0.27      | ~1.8M rows/sec     |
| 1,000,000  | ~0.50      | ~1.9M rows/sec     |

### Telco Churn Dataset

| Rows       | Time (sec) | Throughput         |
|------------|------------|--------------------|
| 7,043      | ~0.11      | ~62k rows/sec      |
| 70,430     | ~0.80      | ~88k rows/sec      |
| 140,860    | ~1.55      | ~90k rows/sec      |

### Observations

- O(n) scaling behavior
- Linear memory growth
- No chained assignment warnings
- Optimized dtype usage (`uint8` for binary columns)

---

## Engineering Decisions

### 1. Vectorized Transformations
All operations use pandas vectorization. No row-wise loops.

### 2. Avoided Full-DataFrame Regex Scanning
Eliminated expensive global regex replacements.

### 3. Explicit Copy After Filtering
Prevented chained assignment issues by using `.copy()` after row filtering.

### 4. Memory Optimization
Binary columns cast to `uint8` to reduce memory footprint.

### 5. Separation of Concerns
- Cleaners contain business logic.
- Benchmarks contain performance instrumentation.

---

## Installation

Clone the repository:

```bash
git clone <your-repo-url>
cd Scalable-AI-Data-Cleaning
```

Create virtual environment:

```bash
python -m venv venv
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Usage

Run benchmark modules:

```bash
python -m src.benchmarks.bench_ecommerce_cleaner
python -m src.benchmarks.bench_telco_churn_cleaner
```

Each benchmark outputs:

- Rows processed
- Execution time
- Throughput (rows/sec)
- Peak memory usage

---

## Roadmap

This project is evolving toward a broader AI-driven data preparation framework.

Planned extensions:

### Structured Data
- Advanced messy CSV cleaning
- Email normalization & validation
- Schema validation layer
- Automated anomaly detection

### Unstructured Text
- OCR text extraction from images
- Post-OCR cleanup
- Text normalization

### Image Processing
- Noise reduction
- Deskewing
- Thresholding
- Preprocessing for OCR pipelines

### AI Integration
- LLM-assisted cleaning suggestions
- Automatic rule generation
- Data quality reporting
- Intelligent anomaly detection

---

## Target Use Cases

- Large-scale CSV cleaning
- Data preprocessing for ML pipelines
- Data migration cleanup
- Client-side data quality improvement
- Freelance data engineering projects

---

## Author

Data engineering and AI preprocessing pipeline development.  
Focused on scalable, reliable, and production-ready data transformation systems.
