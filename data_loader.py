import os
import shutil
import tarfile
from pathlib import Path
from typing import Generator, List

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
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
        partitioned: bool,
        to_duckdb: bool,
        duckdb_path: Path,
        temp_dir: Path = Path("temp_extracted_data"),
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.score_thresholds = sorted(score_thresholds)
        self.partitioned = partitioned
        self.to_duckdb = to_duckdb
        self.duckdb_path = duckdb_path
        self.temp_dir = temp_dir

        self.output_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        if self.to_duckdb:
            self.duckdb_path.parent.mkdir(exist_ok=True)

    def find_files(self) -> Generator[Path, None, None]:
        """
        入力ディレクトリから対象ファイル（.tsv, .txt, .tar.gz）を再帰的に探索する。
        .venvなどの不要なディレクトリは除外する。
        """
        if not self.input_dir.exists():
            raise FileNotFoundError(
                f"入力ディレクトリが見つかりません: {self.input_dir}"
            )

        exclude_dirs = {".venv", "__pycache__", ".git", "tests"}

        for root, dirs, files in os.walk(self.input_dir):
            # 除外リストに含まれるディレクトリを探索対象から削除
            dirs[:] = [d for d in dirs if d not in exclude_dirs]

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
        パーティション分割が有効な場合は、サブディレクトリに分割して保存する。
        """
        if output_dir is None:
            output_dir = self.output_dir
        output_dir.mkdir(exist_ok=True)

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

            if self.partitioned:
                output_partition_dir = output_dir / file_path.stem
                if output_partition_dir.exists():
                    typer.echo(f"スキップ: {output_partition_dir} は既に存在します。")
                    return
                typer.echo(f"パーティション分割して保存: {output_partition_dir}")
                df = processed_lf.collect()
                df.write_parquet(
                    output_partition_dir,
                    partition_by=["event_month", "score_level"],
                    use_pyarrow=True,
                )
                typer.echo(f"保存完了: {output_partition_dir}")
            else:
                output_path = output_dir / f"{file_path.stem}.parquet"
                if output_path.exists():
                    typer.echo(f"スキップ: {output_path} は既に存在します。")
                    return
                typer.echo(f"単一ファイルとして保存: {output_path}")
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
        パーティション分割が有効な場合は、サブディレクトリに分割して保存する。
        """
        typer.echo(f"処理中（アーカイブ）: {file_path}")

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

                lf_list = []
                for member in tsv_files:
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

                if not lf_list:
                    typer.echo(
                        f"警告: {file_path} 内に有効なデータがありませんでした。"
                    )
                    return

                combined_lf = pl.concat(lf_list)
                processed_lf = self.preprocess(combined_lf)

                # このメソッドはパーティション分割専用とする
                output_partition_dir = (
                    self.output_dir / f"{file_path.name.removesuffix('.tar.gz')}"
                )
                if output_partition_dir.exists():
                    typer.echo(f"スキップ: {output_partition_dir} は既に存在します。")
                    return
                typer.echo(f"パーティション分割して保存: {output_partition_dir}")
                df = processed_lf.collect()
                df.write_parquet(
                    output_partition_dir,
                    partition_by=["event_month", "score_level"],
                    use_pyarrow=True,
                )
                typer.echo(f"保存完了: {output_partition_dir}")

        except Exception as e:
            typer.secho(
                f"エラー: {file_path} の処理中にエラーが発生しました: {e}",
                fg=typer.colors.RED,
            )

    def process_tar_gz_in_chunks(self, file_path: Path):
        """
        tar.gzファイルをチャンク処理し、内部のTSVファイルを単一のParquetに追記保存する。
        """
        typer.echo(f"処理中（チャンク処理）: {file_path}")
        output_path = (
            self.output_dir / f"{file_path.name.removesuffix('.tar.gz')}.parquet"
        )
        if output_path.exists():
            typer.echo(f"スキップ: {output_path} は既に存在します。")
            return

        writer = None
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

                for member in tsv_files:
                    # ExFileObjectを一時ファイルとして保存
                    temp_file_path = self.temp_dir / Path(member.name).name
                    with (
                        tar.extractfile(member) as src,
                        open(temp_file_path, "wb") as dest,
                    ):
                        dest.write(src.read())

                    # 一時ファイルからストリーミング処理
                    try:
                        reader = pl.read_csv_batched(
                            temp_file_path,
                            separator="\t",
                            try_parse_dates=True,
                            has_header=True,
                            quote_char=None,
                            ignore_errors=True,
                            batch_size=500_000,  # メモリに応じて調整
                        )
                        while True:
                            batches = reader.next_batches(1)
                            if not batches:
                                break
                            batch = batches[0]
                            # polarsのDataFrameに変換し、前処理を適用
                            processed_df = self.preprocess(batch.lazy()).collect()

                            # Arrowテーブルに変換
                            arrow_table = processed_df.to_arrow()

                            if writer is None:
                                # 最初のバッチでスキーマを決定し、Writerを初期化
                                writer = pq.ParquetWriter(
                                    output_path, arrow_table.schema
                                )

                            writer.write_table(arrow_table)
                    finally:
                        # 一時ファイルを削除
                        os.remove(temp_file_path)

        except Exception as e:
            typer.secho(
                f"エラー: {file_path} の処理中にエラーが発生しました: {e}",
                fg=typer.colors.RED,
            )
        finally:
            if writer:
                writer.close()
                typer.echo(f"保存完了: {output_path}")

    def process_tar_gz_to_duckdb(self, file_path: Path):
        """
        tar.gzファイルを展開し、内部のTSVファイルをDuckDBのテーブルに保存する。
        """
        typer.echo(f"DuckDBへ保存中: {file_path}")
        table_name = file_path.name.removesuffix(".tar.gz").replace("-", "_")

        try:
            with duckdb.connect(str(self.duckdb_path)) as con:
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

                    # 最初のファイルでテーブルを作成
                    first_member = tsv_files[0]
                    with tar.extractfile(first_member) as file_obj:
                        if file_obj:
                            lf = pl.scan_csv(
                                file_obj,
                                separator="\t",
                                try_parse_dates=True,
                                has_header=True,
                                quote_char=None,
                                ignore_errors=True,
                            )
                            processed_df = self.preprocess(lf).collect()
                            # テーブルが存在しない場合のみ作成
                            con.execute(
                                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM processed_df"
                            )
                            typer.echo(
                                f"テーブル '{table_name}' を作成し、最初のデータを挿入しました。"
                            )

                    # 残りのファイルを追記
                    for member in tsv_files[1:]:
                        with tar.extractfile(member) as file_obj:
                            if file_obj:
                                lf = pl.scan_csv(
                                    file_obj,
                                    separator="\t",
                                    try_parse_dates=True,
                                    has_header=True,
                                    quote_char=None,
                                    ignore_errors=True,
                                )
                                processed_df = self.preprocess(lf).collect()
                                con.execute(
                                    f"INSERT INTO {table_name} SELECT * FROM processed_df"
                                )
                typer.echo(f"テーブル '{table_name}' へのデータ追加が完了しました。")

        except Exception as e:
            typer.secho(
                f"エラー: {file_path} のDuckDBへの保存中にエラーが発生しました: {e}",
                fg=typer.colors.RED,
            )

    def run(self):
        """
        データ処理パイプラインを実行する。
        """
        typer.echo("データ処理を開始します...")
        try:
            for file_path in self.find_files():
                if self.to_duckdb:
                    if file_path.suffix == ".gz" and file_path.name.endswith(".tar.gz"):
                        self.process_tar_gz_to_duckdb(file_path)
                    else:
                        typer.echo(
                            f"スキップ（DuckDBモード）: {file_path} は.tar.gz形式ではありません。"
                        )
                elif file_path.suffix in [".tsv", ".txt"]:
                    self.process_file(file_path)
                elif file_path.suffix == ".gz" and file_path.name.endswith(".tar.gz"):
                    if self.partitioned:
                        self.process_tar_gz(file_path)
                    else:
                        self.process_tar_gz_in_chunks(file_path)
        finally:
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
    partitioned: bool = typer.Option(
        False,
        "--partitioned",
        "-p",
        help="データをパーティション分割して保存するかどうか。デフォルトは単一ファイル。",
    ),
    to_duckdb: bool = typer.Option(
        False,
        "--to-duckdb",
        help="データをDuckDBに保存するかどうか。",
    ),
    duckdb_path: Path = typer.Option(
        "output/data.duckdb",
        "--duckdb-path",
        help="DuckDBデータベースファイルのパス。",
    ),
):
    """
    データローダーを実行し、ファイルを前処理してParquetまたはDuckDB形式で保存します。
    """
    loader = DataLoader(
        input_dir=input_dir,
        output_dir=output_dir,
        score_thresholds=[score_t1, score_t2],
        partitioned=partitioned,
        to_duckdb=to_duckdb,
        duckdb_path=duckdb_path,
    )
    loader.run()


if __name__ == "__main__":
    app()
