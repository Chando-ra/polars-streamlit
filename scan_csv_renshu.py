import polars as pl


def main():
    """
    TSVファイルをストリーミングで読み込み、group_byで事前集計後にpivotを適用して
    月別のクロス集計表を作成する。
    """
    try:
        lazy_df = pl.scan_csv("test_data_500.tsv", separator="\t", try_parse_dates=True)
    except FileNotFoundError:
        print("エラー: test_data_500.tsv が見つかりません。")
        print("まず create_test_data.py を実行してテストデータを作成してください。")
        return

    # 閾値の設定
    t1, t2 = 500, 1500

    # カテゴリ分類と月情報の抽出
    lf_processed = lazy_df.with_columns(
        pl.when(pl.col("SCORE") < t1)
        .then(pl.lit("low"))
        .when(pl.col("SCORE") < t2)
        .then(pl.lit("mid"))
        .otherwise(pl.lit("high"))
        .alias("score_level"),
        pl.col("EVENT_DATE").dt.truncate("1mo").alias("event_month"),
    )

    # LazyFrameのままgroup_byでデータを集約し、サイズを小さくする
    aggregated_lf = lf_processed.group_by("event_month", "is_fraud", "score_level").agg(
        [
            pl.len().alias("record_count"),
            pl.sum("EVENT_VALUE").alias("event_value_sum"),
            pl.mean("SCORE").alias("score_mean"),
        ]
    )

    # 集約後の小さなDataFrameに対してpivotを実行
    aggregated_df = aggregated_lf.collect()

    # 1. レコード数のピボット
    pivot_count = aggregated_df.pivot(
        index="event_month", on=["is_fraud", "score_level"], values="record_count"
    )
    # 2. EVENT_VALUE合計のピボット
    pivot_sum = aggregated_df.pivot(
        index="event_month",
        on=["is_fraud", "score_level"],
        values="event_value_sum",
    )
    # 3. SCORE平均のピボット
    pivot_mean = aggregated_df.pivot(
        index="event_month", on=["is_fraud", "score_level"], values="score_mean"
    )

    # 3つのピボットテーブルを結合し、列名をリネームして分かりやすくする
    result_df = (
        pivot_count.join(pivot_sum, on="event_month", suffix="_sum")
        .join(pivot_mean, on="event_month", suffix="_mean")
        .sort("event_month")
    )

    print("pivotを使用した月別のクロス集計結果:")
    # Polarsの表示設定を調整して、より多くの列を表示
    with pl.Config(tbl_cols=20):
        print(result_df)


if __name__ == "__main__":
    main()
