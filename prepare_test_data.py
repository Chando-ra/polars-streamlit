import os
import subprocess

from create_test_data import create_test_data


def main():
    """
    複数のテスト用TSVファイルを生成し、tar.gz形式で圧縮する。
    """
    num_files = 3
    num_rows_per_file = 500_000
    num_cols = 10
    output_dir = "test_data"
    archive_name = "test_data.tar.gz"

    # 出力ディレクトリを作成
    os.makedirs(output_dir, exist_ok=True)

    # TSVファイルを生成
    file_paths = []
    for i in range(num_files):
        file_path = os.path.join(output_dir, f"test_data_part_{i}.tsv")
        print(f"Generating {num_rows_per_file} rows in '{file_path}'...")
        create_test_data(file_path, num_rows_per_file, num_cols)
        file_paths.append(
            os.path.basename(file_path)
        )  # tarコマンド用にファイル名だけをリスト化
        print(f"'{file_path}' generation complete.")

    # tar.gzに圧縮
    print(f"Archiving files into '{archive_name}'...")
    # (cd test_data && tar -czf ../test_data.tar.gz test_data_part_0.tsv test_data_part_1.tsv ...)
    # この方法により、アーカイブ内にディレクトリ構造が含まれないようにします。
    command = ["tar", "-czf", archive_name, "-C", output_dir] + file_paths
    subprocess.run(command, check=True)
    print("Archiving complete.")

    # 元のファイルを削除
    print("Cleaning up temporary files...")
    for file_name in file_paths:
        os.remove(os.path.join(output_dir, file_name))
    os.rmdir(output_dir)
    print("Cleanup complete.")


if __name__ == "__main__":
    main()
