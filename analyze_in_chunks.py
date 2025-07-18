import os

import pandas as pd
from tqdm import tqdm


def analyze_in_chunks(input_path, output_path, chunksize=100_000):
    """
    巨大なTSVファイルをチャンクで読み込み、メモリ効率よく時間単位の集計を行う。
    """
    print("Initializing total summary dataframe...")
    total_summary_df = pd.DataFrame()

    # ファイルの総行数を概算してtqdmのプログレスバーに使用（正確でなくても良い）
    # ここでは固定値を使用するが、必要なら事前に数えることも可能
    total_rows = 1_000_000

    print(f"Processing {input_path} in chunks of {chunksize} rows...")
    with pd.read_csv(
        input_path, sep="\t", chunksize=chunksize, keep_default_na=False
    ) as reader:
        for chunk in tqdm(reader, total=-(total_rows // -chunksize)):  # プログレスバー
            # --- チャンク内での処理 ---
            # 1. hit_ruleを展開
            hit_rule_dummies = chunk["hit_rule"].str.get_dummies(sep=" ")

            # 2. 時間情報の変換
            chunk["EVENT_TIME"] = pd.to_datetime(chunk["EVENT_TIME"])
            chunk["time_hour"] = chunk["EVENT_TIME"].dt.floor("h")

            # 3. 必要な列を結合
            chunk_processed = pd.concat(
                [chunk[["time_hour"]], hit_rule_dummies], axis=1
            )

            # 4. チャンク内集計
            # 取引量もここで計算
            chunk_summary = chunk_processed.groupby("time_hour").sum()
            chunk_summary["取引量"] = chunk_processed.groupby("time_hour").size()

            # 5. 全体集計に加算
            total_summary_df = total_summary_df.add(chunk_summary, fill_value=0)

    print("Finalizing calculations...")
    # --- 最終処理 ---
    # データ型を整数に変換
    total_summary_df = total_summary_df.astype(int)

    # ルールカラムのリストを取得
    rule_columns = [col for col in total_summary_df.columns if col.startswith("ルール")]

    # ヒット率の計算
    hits_df = total_summary_df[rule_columns]
    rates_df = hits_df.div(total_summary_df["取引量"], axis=0)

    # カラムの多段化
    hits_df.columns = pd.MultiIndex.from_product([rule_columns, ["ヒット数"]])
    rates_df.columns = pd.MultiIndex.from_product([rule_columns, ["ヒット率"]])

    # 最終的なDataFrameを作成
    final_df = pd.concat([total_summary_df[["取引量"]], hits_df, rates_df], axis=1)
    final_df.columns = pd.MultiIndex.from_tuples(
        [(col, "") if isinstance(col, str) else col for col in final_df.columns]
    )
    final_df = final_df.sort_index(axis=1)

    # --- 結果の出力 ---
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Saving results to {output_path}...")
    final_df.to_csv(output_path, sep="\t")
    print("Analysis complete.")


if __name__ == "__main__":
    input_file = "input_data/test_data.tsv"
    output_file = "analysis_results/chunk_summary_by_hour.csv"
    analyze_in_chunks(input_file, output_file)
