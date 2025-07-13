import os
import tarfile
from pathlib import Path

import polars as pl


def preprocess_data(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    データフレームに前処理を適用する。
    - score_level: SCOREに基づいてカテゴリ分類
    - event_month: EVENT_DATEを月で切り捨て
    """
    t1, t2 = 500, 1500
    return lazy_df.with_columns(
        pl.when(pl.col("SCORE") < t1)
        .then(pl.lit("low"))
        .when(pl.col("SCORE") < t2)
        .then(pl.lit("mid"))
        .otherwise(pl.lit("high"))
        .alias("score_level"),
        pl.col("EVENT_DATE").dt.truncate("1mo").alias("event_month"),
    )


def process_file(file_path: Path, prepared_dir: Path):
    """
    単一のファイル（.tsvまたは.txt）を処理し、Parquet形式で保存する。
    """
    output_filename = prepared_dir / f"{file_path.stem}.parquet"
    if output_filename.exists():
        print(f"スキップ: {output_filename} は既に存在します。")
        return

    print(f"処理中: {file_path}")
    try:
        # .tsvと.txtの両方に対応するため、separatorを推測させる
        lazy_df = pl.scan_csv(
            file_path, separator="\t", try_parse_dates=True, has_header=True
        )

        # 前処理の適用
        processed_lf = preprocess_data(lazy_df)

        # 結果をParquetファイルとして保存
        processed_lf.sink_parquet(output_filename)
        print(f"保存完了: {output_filename}")

    except Exception as e:
        print(f"エラー: {file_path} の処理中にエラーが発生しました: {e}")


def process_tar_gz(file_path: Path, prepared_dir: Path, temp_dir: Path):
    """
    tar.gzファイルを展開し、中のファイルを処理する。
    """
    print(f"展開中: {file_path}")
    with tarfile.open(file_path, "r:gz") as tar:
        # 安全な展開先の確認
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        tar.extractall(path=temp_dir)

        for member in tar.getmembers():
            if member.isfile() and (
                member.name.endswith(".tsv") or member.name.endswith(".txt")
            ):
                extracted_file_path = temp_dir / member.name
                process_file(extracted_file_path, prepared_dir)


def main():
    """
    input_dataディレクトリを探索し、ファイルを処理する。
    """
    input_dir = Path("input_data")
    prepared_dir = Path("prepared_data")
    temp_dir = Path("temp_extracted_data")

    # 出力ディレクトリと一時ディレクトリの作成
    prepared_dir.mkdir(exist_ok=True)
    temp_dir.mkdir(exist_ok=True)

    if not input_dir.exists():
        print(f"エラー: {input_dir} ディレクトリが見つかりません。")
        return

    print("データ処理を開始します...")
    for root, _, files in os.walk(input_dir):
        for file in files:
            file_path = Path(root) / file
            if file.endswith((".tsv", ".txt")):
                process_file(file_path, prepared_dir)
            elif file.endswith(".tar.gz"):
                process_tar_gz(file_path, prepared_dir, temp_dir)

    # 一時ディレクトリのクリーンアップ
    import shutil

    shutil.rmtree(temp_dir)
    print("一時ファイルをクリーンアップしました。")
    print("データ処理が完了しました。")


if __name__ == "__main__":
    main()
