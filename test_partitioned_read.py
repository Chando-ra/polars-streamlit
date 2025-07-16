from pathlib import Path

import pandas as pd
import typer


def main(
    dataset_path: Path = typer.Argument(
        ...,
        help="パーティション分割されたParquetデータセットのルートディレクトリパス（例: prepared_data/test_data_1）",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    event_month: str = typer.Option(
        "2023-01-01",
        "--month",
        "-m",
        help="読み込むデータのevent_month（YYYY-MM-DD形式）",
    ),
    score_level: str = typer.Option(
        "high", "--level", "-l", help="読み込むデータのscore_level（low, mid, high）"
    ),
):
    """
    パーティション分割されたParquetデータセットから、
    指定した条件（フィルタ）に一致する部分だけをPandasで読み込むテスト。
    """
    typer.secho(
        f"データセット '{dataset_path}' の読み込みを試みます...", fg=typer.colors.CYAN
    )

    # 読み込むパーティションを指定するためのフィルタを作成
    # DNF (Disjunctive Normal Form) filters: [[(key, op, value), ...], ...]
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    filters = [
        ("event_month", "=", pd.Timestamp(event_month)),
        ("score_level", "=", score_level),
    ]

    try:
        # read_parquetにfiltersを渡すことで、指定したパーティションのみが読み込まれる
        df = pd.read_parquet(dataset_path, filters=filters, engine="pyarrow")

        typer.secho(
            "指定されたパーティションの読み込みに成功しました！", fg=typer.colors.GREEN
        )
        print("\n" + "=" * 50)
        print("読み込み条件（フィルタ）:")
        for key, op, value in filters:
            print(f"  - {key} {op} {value}")
        print("=" * 50)

        print("\n読み込まれたDataFrameの情報:")
        df.info()

        if not df.empty:
            print("\nデータの一部（先頭5行）:")
            print(df.head())
        else:
            typer.secho(
                "データは空です。指定した条件に一致するパーティションが存在しない可能性があります。",
                fg=typer.colors.YELLOW,
            )

    except Exception as e:
        typer.secho(f"エラーが発生しました: {e}", fg=typer.colors.RED)
        print("\n【確認してください】")
        print(f"1. 指定したパス '{dataset_path}' は正しいですか？")
        print("2. データは `python data_loader.py --partitioned` で生成されましたか？")
        print(
            f"3. 指定したフィルタ条件（event_month='{event_month}', score_level='{score_level}'）に一致するデータが存在しますか？"
        )
        print(
            "   - ディレクトリ構造を確認してください。例: prepared_data/test_data_1/event_month=2023-01-01/score_level=high/"
        )


if __name__ == "__main__":
    typer.run(main)
