import sys
import tarfile
from pathlib import Path

# プロジェクトルートをsys.pathに追加
sys.path.append(str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from convert_to_hdf import convert_tar_to_hdf


@pytest.fixture
def temp_dirs(tmp_path: Path) -> tuple[Path, Path]:
    """
    テスト用の入力・出力ディレクトリを作成するフィクスチャ。
    """
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    return input_dir, output_dir


def create_test_tar_gz(tar_path: Path, file_name: str, data: pd.DataFrame) -> None:
    """
    テスト用のTSVファイルを含むtar.gzアーカイブを作成する。
    """
    # DataFrameをTSV形式の文字列に変換
    tsv_content = data.to_csv(sep="\t", index=False)
    # バイト列にエンコード
    tsv_bytes = tsv_content.encode("utf-8")

    with tarfile.open(tar_path, "w:gz") as tar:
        tarinfo = tarfile.TarInfo(name=file_name)
        tarinfo.size = len(tsv_bytes)
        tar.addfile(tarinfo, pd.io.common.BytesIO(tsv_bytes))


def test_convert_and_read_columns(temp_dirs):
    """
    正常系テスト: tar.gzがHDF5に変換され、
    列を指定して正しく読み込めることを確認する。
    """
    input_dir, output_dir = temp_dirs
    test_tar_file = input_dir / "test_data.tar.gz"

    # テストデータ (前処理前の状態)
    source_data = pd.DataFrame(
        {
            "SCORE": [100, 600, 1600, np.nan],
            "EVENT_VALUE": [10.0, 20.0, 30.0, 40.0],
            "is_fraud": [True, False, np.nan, False],
            "EVENT_TIME": [
                "2023-01-15T12:00:00",
                "2023-02-20T12:00:00",
                "2023-03-10T12:00:00",
                pd.NaT,
            ],
            "string_col": ["A", "B", "C", "D"],
        }
    )
    create_test_tar_gz(test_tar_file, "test.tsv", source_data)

    # HDF5ファイルへの変換を実行
    score_thresholds = [500, 1500]
    convert_tar_to_hdf(test_tar_file, output_dir, score_thresholds)

    # 結果の検証
    output_file = output_dir / "test_data.h5"
    assert output_file.exists()

    # HDF5ファイルから特定の列を指定して読み込む
    columns_to_read = ["SCORE", "score_level", "is_fraud"]
    result_df = pd.read_hdf(output_file, key="data", columns=columns_to_read)

    # 期待されるデータ (前処理後の状態)
    expected_df = pd.DataFrame(
        {
            "SCORE": [100.0, 600.0, 1600.0, 0.0],
            "score_level": ["low", "mid", "high", "low"],
            "is_fraud": [True, False, False, False],
        }
    )
    # is_fraudはbool型になっているはず
    expected_df["is_fraud"] = expected_df["is_fraud"].astype(bool)

    # 読み込んだデータフレームが期待通りか検証
    assert_frame_equal(result_df, expected_df)
