import os

import pandas as pd


def analyze_hourly_rules(input_path, output_path):
    """
    時間単位で取引量、各ルールのヒット数、ヒット率を集計し、
    カラムを2段にしてCSVに出力する。
    """
    print(f"Reading data from {input_path}...")
    df = pd.read_csv(input_path, sep="\t")

    # --- 1. データ準備 ---
    print("Preparing data...")
    # EVENT_TIMEを日時型に変換
    df["EVENT_TIME"] = pd.to_datetime(df["EVENT_TIME"])
    # 時間単位に丸めた列を作成 (Warning修正: 'H' -> 'h')
    df["time_hour"] = df["EVENT_TIME"].dt.floor("h")

    # ルールカラムのリストを取得 (ルール1, ルール2, ...)
    rule_columns = [col for col in df.columns if col.startswith("ルール")]

    # --- 2. グループ化と基本集計 ---
    print("Grouping and aggregating data...")
    # 時間単位でグループ化
    grouped = df.groupby("time_hour")

    # ヒット数を集計
    agg_sum = grouped[rule_columns].sum()
    # 取引量（カウント）を集計
    agg_count = grouped.size().to_frame("取引量")

    # ヒット数と取引量を結合
    agg_df = pd.concat([agg_count, agg_sum], axis=1)

    # --- 3. ヒット率の計算 (PerformanceWarning修正) ---
    print("Calculating hit rates...")
    # ヒット数のDataFrame
    hits_df = agg_df[rule_columns]
    # ヒット率のDataFrameを計算
    rates_df = hits_df.div(agg_df["取引量"], axis=0)

    # --- 4. カラムの多段化 ---
    print("Restructuring columns to MultiIndex...")
    # ヒット数とヒット率のDataFrameにMultiIndexを設定
    hits_df.columns = pd.MultiIndex.from_product([rule_columns, ["ヒット数"]])
    rates_df.columns = pd.MultiIndex.from_product([rule_columns, ["ヒット率"]])

    # 取引量、ヒット数、ヒット率を結合
    final_df = pd.concat([agg_df[["取引量"]], hits_df, rates_df], axis=1)

    # '取引量'カラムもMultiIndexに変換 (TypeError修正)
    final_df.columns = pd.MultiIndex.from_tuples(
        [(col, "") if isinstance(col, str) else col for col in final_df.columns]
    )

    # カラムをソートして見やすくする
    final_df = final_df.sort_index(axis=1)

    # --- 5. 結果の出力 ---
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Saving results to {output_path}...")
    final_df.to_csv(output_path, sep="\t")
    print("Analysis complete.")


if __name__ == "__main__":
    input_file = "prepared_data/expanded_data_pandas.tsv"
    output_file = "analysis_results/rule_summary_by_hour.csv"
    analyze_hourly_rules(input_file, output_file)
