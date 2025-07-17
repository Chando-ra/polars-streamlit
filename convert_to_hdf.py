import csv
import os
import tarfile
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import typer

app = typer.Typer(help="tar.gz内のTSVファイルを前処理し、HDF5形式に変換するCLIツール。")


def preprocess_pandas(df: pd.DataFrame, score_thresholds: List[int]) -> pd.DataFrame:
    """
    Pandas DataFrameに対して、data_loader.pyのpreprocess相当の処理を適用する。
    """
    # 1. 全列が欠損値の行を削除
    df.dropna(how="all", inplace=True)

    # 2. 欠損値処理
    # SCORE, EVENT_VALUE: 0で補完
    if "SCORE" in df.columns:
        df["SCORE"] = df["SCORE"].fillna(0)
    if "EVENT_VALUE" in df.columns:
        df["EVENT_VALUE"] = df["EVENT_VALUE"].fillna(0)

    # is_fraud: Falseで補完し、bool型に変換
    if "is_fraud" in df.columns:
        # 文字列の"False"がbool()でTrueになるのを防ぐため、明示的にマッピング
        map_dict = {
            "True": True,
            "False": False,
            "1": True,
            "0": False,
            1: True,
            0: False,
            True: True,
            False: False,
        }
        df["is_fraud"] = df["is_fraud"].map(map_dict)
        df["is_fraud"] = df["is_fraud"].fillna(False).astype(bool)

    # 文字列型の列を"unknown"で補完
    string_cols = df.select_dtypes(include=["object"]).columns
    for col in string_cols:
        if col not in ["is_fraud", "EVENT_TIME"]:
            df[col] = df[col].fillna("unknown")

    # EVENT_TIME: 日付型に変換し、前後の値で補完
    if "EVENT_TIME" in df.columns:
        df["EVENT_TIME"] = pd.to_datetime(df["EVENT_TIME"], errors="coerce")
        df["EVENT_TIME"] = df["EVENT_TIME"].ffill()
        df["EVENT_TIME"] = df["EVENT_TIME"].bfill()

    # 3. 特徴量生成
    t1, t2 = sorted(score_thresholds)
    if "SCORE" in df.columns:
        conditions = [
            df["SCORE"] < t1,
            (df["SCORE"] >= t1) & (df["SCORE"] < t2),
        ]
        choices = ["low", "mid"]
        df["score_level"] = np.select(conditions, choices, default="high")

    if "EVENT_TIME" in df.columns and pd.api.types.is_datetime64_any_dtype(
        df["EVENT_TIME"]
    ):
        df["event_month"] = df["EVENT_TIME"].dt.to_period("M").dt.to_timestamp()

    return df


def convert_tar_to_hdf(file_path: Path, output_dir: Path, score_thresholds: List[int]):
    """
    単一のtar.gzファイルをHDF5に変換する。
    """
    typer.echo(f"処理中: {file_path}")
    output_path = output_dir / f"{file_path.name.removesuffix('.tar.gz')}.h5"

    if output_path.exists():
        typer.echo(f"スキップ: {output_path} は既に存在します。")
        return

    try:
        with tarfile.open(file_path, "r:gz") as tar:
            tsv_files = [
                member
                for member in tar.getmembers()
                if member.isfile() and member.name.endswith((".tsv", ".txt"))
            ]
            if not tsv_files:
                typer.echo(
                    f"警告: {file_path} 内に処理対象のファイルが見つかりません。"
                )
                return

            # 最初のファイルからヘッダーを読み込み、dtypeを決定
            first_member = tsv_files[0]
            with tar.extractfile(first_member) as f:
                header_line = f.readline().decode("utf-8").strip()
                header = header_line.split("\t")

            dtypes = {}
            for col in header:
                if col in ["SCORE", "EVENT_VALUE"] or col.startswith("numeric_col_"):
                    dtypes[col] = "float64"  # 欠損値NaNのため
                elif col == "is_fraud":
                    dtypes[col] = "object"  # True/False/NaN
                else:
                    dtypes[col] = "str"

            with pd.HDFStore(
                output_path, mode="w", complevel=9, complib="blosc"
            ) as store:
                for member in tsv_files:
                    typer.echo(f"  -> 追加中: {member.name}")
                    file_obj = tar.extractfile(member)
                    if file_obj:
                        df_pandas = pd.read_csv(
                            file_obj,
                            sep="\t",
                            header=0,
                            dtype=dtypes,
                            quoting=csv.QUOTE_NONE,
                            engine="python",
                        )

                        processed_df = preprocess_pandas(df_pandas, score_thresholds)

                        min_itemsize = {}
                        string_like_cols = processed_df.select_dtypes(
                            include=["object", "string"]
                        ).columns
                        for c in string_like_cols:
                            dtype = processed_df[c].dtype
                            if pd.api.types.is_string_dtype(
                                dtype
                            ) or pd.api.types.is_object_dtype(dtype):
                                max_len = processed_df[c].astype(str).str.len().max()
                                if pd.notna(max_len):
                                    min_itemsize[c] = int(max_len)

                        store.append(
                            "data",
                            processed_df,
                            format="table",
                            data_columns=True,
                            min_itemsize=min_itemsize,
                        )
        typer.echo(f"HDF5ファイルとして保存完了: {output_path}")

    except Exception as e:
        typer.secho(
            f"エラー: {file_path} のHDF5保存中にエラーが発生しました: {e}",
            fg=typer.colors.RED,
        )


@app.command()
def main(
    input_dir: Path = typer.Option(
        ...,
        "--input",
        "-i",
        help="入力データディレクトリのパス (tar.gzファイルを含む)",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    output_dir: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="HDF5ファイルを保存するディレクトリのパス",
    ),
    score_t1: int = typer.Option(500, help="SCOREの閾値1（low <-> mid）"),
    score_t2: int = typer.Option(1500, help="SCOREの閾値2（mid <-> high）"),
):
    """
    入力ディレクトリ内のtar.gzファイルを検索し、HDF5形式に変換・保存します。
    """
    output_dir.mkdir(exist_ok=True)
    score_thresholds = [score_t1, score_t2]

    typer.echo(f"入力ディレクトリ: {input_dir}")
    typer.echo(f"出力ディレクトリ: {output_dir}")

    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".tar.gz"):
                file_path = Path(root) / file
                convert_tar_to_hdf(file_path, output_dir, score_thresholds)

    typer.secho("すべての処理が完了しました。", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
