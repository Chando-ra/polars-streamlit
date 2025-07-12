import polars as pl


def make_parquet(file_path, output_path):
    """
    指定されたTSVファイルをParquet形式に変換する。
    """
    print(f"TSVファイルをParquet形式に変換: {file_path} -> {output_path}")

    # TSVファイルを読み込み、DataFrameを作成
    df = pl.read_csv(file_path, separator="\t")

    # Parquet形式で保存
    df.write_parquet(output_path)

    print(f"変換完了: {output_path}")


if __name__ == "__main__":
    input_file = "test_data.tsv"
    output_file = "test_data.parquet"
    make_parquet(input_file, output_file)
    print(f"Parquetファイルを作成: {output_file}")
    print("Parquetファイルの作成が完了しました。")
