import polars as pl

# Parquetファイルのパス
parquet_path = "output/test_data.parquet"

# 1. scan_parquetで遅延フレームを作成
# これにより、ファイル全体をメモリに読み込むことなく操作を定義できる
lazy_df = pl.scan_parquet(parquet_path)

# 2. 処理内容を定義（この時点ではデータは読み込まれない）
#    - score_levelが "high" の行をフィルタリング
#    - 特定の列のみを選択
# polarsはこれらの操作を最適化し、必要なデータのみをディスクから読み込む（Predicate/Projection Pushdown）
filtered_lazy_df = lazy_df.filter(pl.col("score_level") == "high").select(
    ["EVENT_TIME", "SCORE", "score_level"]
)

# 3. .collect()で実際にデータを読み込み、処理を実行
#    ここで初めてディスクI/Oと計算が発生する
df = filtered_lazy_df.collect()

# 4. 結果の表示
print("Filtered DataFrame:")
print(df)

# 参考：フィルタリングせずに特定の列だけを読み込む場合
print("\nSelected columns without filtering:")
selected_df = pl.scan_parquet(parquet_path).select(["EVENT_TIME", "SCORE"]).collect()
print(selected_df.head())
