import polars as pl


def read_parquet(file_path):
    """
    指定されたParquetファイルを読み込み、DataFrameを返す。
    """
    print(f"Parquetファイルを読み込み: {file_path}")

    # Parquetファイルを読み込み
    df = pl.read_parquet(file_path)

    print("データの読み込みが完了しました。")

    return df


if __name__ == "__main__":
    input_file = "test_data.parquet"
    df = read_parquet(input_file)
    print(f"読み込んだDataFrameの行数: {len(df)}")
    print("最初の5行:")
    print(df.head())
