import time

import pandas as pd
import polars as pl
from memory_profiler import memory_usage


def load_with_pandas(file_path):
    """Load data with pandas and measure time and memory."""
    start_time = time.time()
    df = pd.read_csv(file_path, delimiter="\t")
    end_time = time.time()

    print(f"Pandas loading time: {end_time - start_time:.2f} seconds")
    print("DataFrame Info:")
    df.info(memory_usage="deep")
    return df


def optimize_dtypes(df):
    """Optimize data types for memory efficiency."""
    print("\n--- Optimizing data types ---")
    start_time = time.time()

    # Optimize numeric columns
    for col in df.select_dtypes(include=["int64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")

    # Optimize string columns
    for col in df.select_dtypes(include=["object"]).columns:
        if df[col].nunique() / len(df[col]) < 0.5:  # Heuristic for using category
            df[col] = df[col].astype("category")

    end_time = time.time()
    print(f"Optimization time: {end_time - start_time:.2f} seconds")
    print("\nOptimized DataFrame Info:")
    df.info(memory_usage="deep")
    return df


def load_in_chunks(file_path, chunk_size=100_000):
    """Load data in chunks and optimize each chunk."""
    print(f"\n--- Loading data in chunks of size {chunk_size} ---")
    start_time = time.time()

    chunks = []
    with pd.read_csv(file_path, delimiter="\t", chunksize=chunk_size) as reader:
        for chunk in reader:
            optimized_chunk = optimize_dtypes(chunk)
            chunks.append(optimized_chunk)

    df_final = pd.concat(chunks, ignore_index=True)
    end_time = time.time()

    print(f"\nTotal time for chunked loading: {end_time - start_time:.2f} seconds")
    print("\nFinal DataFrame Info (from chunks):")
    df_final.info(memory_usage="deep")
    return df_final


def load_with_polars(file_path):
    """Load data with Polars and measure time and memory."""
    print("\n--- Loading data with Polars ---")
    start_time = time.time()

    # Use scan_csv for lazy evaluation and then collect
    df = pl.scan_csv(file_path, separator="\t").collect()

    end_time = time.time()
    print(f"Polars loading time: {end_time - start_time:.2f} seconds")
    print("\nPolars DataFrame Info:")
    print(df)
    print(f"\nEstimated memory usage: {df.estimated_size('mb'):.2f} MB")
    return df


if __name__ == "__main__":
    file_path = "test_data.tsv"

    print("--- Running Pandas Benchmark (Baseline) ---")
    mem_usage_pandas = memory_usage((load_with_pandas, (file_path,)), interval=0.1)
    print(f"Peak memory usage (Pandas): {max(mem_usage_pandas):.2f} MiB")
    print("---------------------------------")

    print("\n--- Running Polars Benchmark ---")
    mem_usage_polars = memory_usage((load_with_polars, (file_path,)), interval=0.1)
    print(f"\nPeak memory usage (Polars): {max(mem_usage_polars):.2f} MiB")
    print("---------------------------------")
