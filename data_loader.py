import os
import shutil
import tarfile
from pathlib import Path
from typing import Generator, List

import polars as pl
import typer


class DataLoader:
    """
    データファイルを処理し、前処理を適用してParquet形式で保存するクラス。
    """

    def __init__(
        self,
        input_dir: Path,
        output_dir: Path,
        score_thresholds: List[int],
        temp_dir: Path = Path("temp_extracted_data"),
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.score_thresholds = sorted(score_thresholds)
        self.temp_dir = temp_dir

        self.output_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)

    def find_files(self) -> Generator[Path, None, None]:
        """
        入力ディレクトリから対象ファイル（.tsv, .txt, .tar.gz）を再帰的に探索する。
        """
        if not self.input_dir.exists():
            raise FileNotFoundError(
                f"入力ディレクトリが見つかりません: {self.input_dir}"
            )

        for root, _, files in os.walk(self.input_dir):
            for file in files:
                if file.endswith((".tsv", ".txt", ".tar.gz")):
                    yield Path(root) / file

    def preprocess(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        欠損値処理、カテゴリ分類、日付情報の追加などの前処理を適用する。
        """
        # 1. 全列が欠損値の行を削除
        lf = lf.filter(~pl.all_horizontal(pl.all().is_null()))

        # 2. 欠損値処理
        lf = lf.with_columns(
            # SCORE, EVENT_VALUE: 0で補完
            pl.col("SCORE").fill_null(0),
            pl.col("EVENT_VALUE").fill_null(0),
            # 文字列型(pl.Utf8)の列を"unknown"で補完
            pl.col(pl.Utf8).fill_null("unknown"),
            # is_fraud: Falseで補完
            pl.col("is_fraud").fill_null(False),
            # EVENT_TIME: 前後の値で補完
            pl.col("EVENT_TIME").forward_fill(),
        )
        # numeric_col_* は null のまま

        # 3. 元の前処理
        t1, t2 = self.score_thresholds
        return lf.with_columns(
            pl.when(pl.col("SCORE") < t1)
            .then(pl.lit("low"))
            .when(pl.col("SCORE") < t2)
            .then(pl.lit("mid"))
            .otherwise(pl.lit("high"))
            .alias("score_level"),
            pl.col("EVENT_TIME").dt.truncate("1mo").alias("event_month"),
        )

    def process_file(self, file_path: Path, output_dir: Path | None = None):
        """
        単一のデータファイルを処理し、Parquetとして保存する。
        """
        if output_dir is None:
            output_dir = self.output_dir
        output_dir.mkdir(exist_ok=True)

        output_path = output_dir / f"{file_path.stem}.parquet"
        if output_path.exists():
            typer.echo(f"スキップ: {output_path} は既に存在します。")
            return

        typer.echo(f"処理中: {file_path}")
        try:
            lf = pl.scan_csv(
                file_path,
                separator="\t",
                try_parse_dates=True,
                has_header=True,
                quote_char=None,
                ignore_errors=True,
            )
            processed_lf = self.preprocess(lf)
            processed_lf.sink_parquet(output_path)
            typer.echo(f"保存完了: {output_path}")
        except Exception as e:
            typer.secho(
                f"エラー: {file_path} の処理中にエラーが発生しました: {e}",
                fg=typer.colors.RED,
            )

    def process_tar_gz(self, file_path: Path):
        """
        tar.gzファイルを展開し、内部のTSVファイルを一つにまとめてParquetとして保存する。
        """
        output_path = (
            self.output_dir / f"{file_path.name.removesuffix('.tar.gz')}.parquet"
        )
        if output_path.exists():
            typer.echo(f"スキップ: {output_path} は既に存在します。")
            return

        typer.echo(f"処理中（アーカイブ）: {file_path}")

        try:
            with tarfile.open(file_path, "r:gz") as tar:
                # アーカイブ内のTSVファイルパスをすべて取得
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

                # ファイルを展開せずに直接読み込む
                lf_list = []
                for member in tsv_files:
                    # tar.extractfileはBytesIOのようなオブジェクトを返す
                    file_obj = tar.extractfile(member)
                    if file_obj:
                        lf = pl.scan_csv(
                            file_obj,
                            separator="\t",
                            try_parse_dates=True,
                            has_header=True,
                            quote_char=None,
                            ignore_errors=True,
                        )
                        lf_list.append(lf)

                # LazyFrameを結合
                combined_lf = pl.concat(lf_list)
                processed_lf = self.preprocess(combined_lf)
                processed_lf.sink_parquet(output_path)
                typer.echo(f"保存完了: {output_path}")

        except Exception as e:
            typer.secho(
                f"エラー: {file_path} の処理中にエラーが発生しました: {e}",
                fg=typer.colors.RED,
            )

    def run(self):
        """
        データ処理パイプラインを実行する。
        """
        typer.echo("データ処理を開始します...")
        try:
            for file_path in self.find_files():
                if file_path.suffix in [".tsv", ".txt"]:
                    self.process_file(file_path)
                elif file_path.suffix == ".gz" and file_path.name.endswith(".tar.gz"):
                    self.process_tar_gz(file_path)
        finally:
            # 一時ディレクトリのクリーンアップ
            shutil.rmtree(self.temp_dir)
            typer.echo("一時ファイルをクリーンアップしました。")

        typer.secho("データ処理が完了しました。", fg=typer.colors.GREEN)


app = typer.Typer(help="データローダー・前処理パイプライン")


@app.command()
def main(
    input_dir: Path = typer.Option(
        "input_data",
        "--input",
        "-i",
        help="入力データディレクトリのパス",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    output_dir: Path = typer.Option(
        "prepared_data",
        "--output",
        "-o",
        help="前処理済みデータを保存するディレクトリのパス",
    ),
    score_t1: int = typer.Option(500, help="SCOREの閾値1（low <-> mid）"),
    score_t2: int = typer.Option(1500, help="SCOREの閾値2（mid <-> high）"),
):
    """
    データローダーを実行し、ファイルを前処理してParquet形式で保存します。
    """
    loader = DataLoader(
        input_dir=input_dir,
        output_dir=output_dir,
        score_thresholds=[score_t1, score_t2],
    )
    loader.run()


if __name__ == "__main__":
    app()
