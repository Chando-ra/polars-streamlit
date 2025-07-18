import pandas as pd


def expand_hit_rules_pandas(input_path, output_path):
    """
    TSVファイルを読み込み、hit_ruleカラムを展開してフラグ列を追加し、新しいファイルに保存する（pandas版）。
    """
    # ルールリストの定義（get_dummiesは自動で列を生成するが、完全性のために定義しておく）
    # rule_list = [f"ルール{i}" for i in range(1, 201)]

    # データを読み込む
    print(f"Reading data from {input_path} with pandas...")
    df = pd.read_csv(input_path, sep="\t", keep_default_na=False)

    print("Expanding hit_rule column with pandas...")

    # hit_ruleカラムをダミー変数に展開
    # 空文字列や欠損値は自動的に無視される
    hit_rule_dummies = df["hit_rule"].str.get_dummies(sep=" ")

    # 元のDataFrameと結合
    df = pd.concat([df, hit_rule_dummies], axis=1)

    # 結果を保存
    print(f"Saving expanded data to {output_path}...")
    df.to_csv(output_path, sep="\t", index=False)
    print("Processing complete.")


if __name__ == "__main__":
    input_file = "input_data/test_data.tsv"
    output_file = "prepared_data/expanded_data_pandas.tsv"  # 出力ファイル名を変更
    expand_hit_rules_pandas(input_file, output_file)
