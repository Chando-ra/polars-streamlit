import time

import polars as pl


def analyze_data(file_path):
    """
    Polarsの遅延評価とストリーミングエンジンを利用して、
    大規模データを高速かつ省メモリで分析する。
    """
    print("--- Polarsによるデータ分析を開始 ---")
    start_time = time.time()

    # CSVファイルから遅延フレームを作成
    lazy_df = pl.scan_csv(file_path, separator="\t")

    # 1. 全ての数値列の合計を計算
    schema = lazy_df.collect_schema()
    numeric_cols = [col for col in schema.names() if col.startswith("numeric_")]
    numeric_sum_expr = [pl.sum(col) for col in numeric_cols]

    # 2. ピボットテーブルのような集計処理
    pivot_agg_expr = lazy_df.group_by("string_col_0").agg(
        pl.sum("numeric_col_0").alias("sum_numeric_0"),
        pl.mean("numeric_col_1").alias("mean_numeric_1"),
    )

    # 2つのクエリを同時に実行
    results = pl.collect_all(
        [lazy_df.select(numeric_sum_expr), pivot_agg_expr], engine="streaming"
    )
    numeric_sums = results[0]
    pivot_results = results[1]

    end_time = time.time()

    duration = end_time - start_time
    print(f"\n分析完了: {duration:.2f} 秒")
    print("\n--- 数値列の合計 ---")
    print(numeric_sums)
    print("\n--- ピボット集計結果 (上位5件) ---")
    print(pivot_results.head())
    print(f"\nピボット集計の総行数: {len(pivot_results)}")


if __name__ == "__main__":
    file_path = "test_data.tsv"
    analyze_data(file_path)
