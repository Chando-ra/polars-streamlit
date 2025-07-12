import shutil
import tarfile
import tempfile
import time

import polars as pl


def analyze_data_polars(archive_path):
    """
    tar.gzアーカイブを展開し、Polarsの遅延評価とストリーミングエンジンを
    利用して大規模データを高速かつ省メモリで分析する。
    """
    print(f"--- Polarsによるデータ分析を開始 ({archive_path}) ---")
    start_time = time.time()

    # 一時ディレクトリを作成
    temp_dir = tempfile.mkdtemp()
    print(f"一時ディレクトリを作成: {temp_dir}")

    try:
        # tar.gzファイルを一時ディレクトリに展開
        print("アーカイブを展開中...")
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(path=temp_dir)
        print("展開完了。")

        # 展開されたすべてのTSVファイルをスキャン
        tsv_files_pattern = f"{temp_dir}/*.tsv"
        lazy_df = pl.scan_csv(tsv_files_pattern, separator="\t")

        # ピボットテーブルのような集計処理を定義
        # string_col_0でグループ化し、numeric_col_0の合計とnumeric_col_1の平均を計算
        pivot_agg_expr = lazy_df.group_by("string_col_0").agg(
            pl.sum("numeric_col_0").alias("sum_numeric_0"),
            pl.mean("numeric_col_1").alias("mean_numeric_1"),
        )

        # ストリーミングモードでクエリを実行
        print("集計処理を実行中...")
        pivot_results = pivot_agg_expr.collect(streaming=True)
        print("集計完了。")

        end_time = time.time()
        duration = end_time - start_time

        print(f"\n分析完了: {duration:.2f} 秒")
        print("\n--- ピボット集計結果 (上位5件) ---")
        print(pivot_results.head())
        print(f"\nピボット集計の総行数: {len(pivot_results)}")

    finally:
        # 一時ディレクトリをクリーンアップ
        print(f"一時ディレクトリを削除: {temp_dir}")
        shutil.rmtree(temp_dir)


if __name__ == "__main__":
    archive_path = "test_data.tar.gz"
    analyze_data_polars(archive_path)
