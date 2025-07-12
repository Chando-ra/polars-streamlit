import csv
import io
import tarfile
import time
from collections import Counter


def analyze_data_stream(archive_path):
    """
    tar.gzアーカイブをストリーミングで解凍し、中のTSVファイルを一行ずつ処理する。
    これにより、メモリ使用量を最小限に抑える。
    """
    print(f"--- ストリーミングによるデータ分析を開始 ({archive_path}) ---")
    start_time = time.time()

    numeric_sums = Counter()
    category_counts = Counter()
    total_rows = 0
    header = []

    # tar.gzファイルをストリーミングモードで開く
    with tarfile.open(archive_path, "r:gz") as tar:
        # アーカイブ内の各ファイルを処理
        for member in tar:
            if member.isfile() and member.name.endswith(".tsv"):
                print(f"Processing file: {member.name}")
                # ファイルをテキストストリームとして抽出
                f = tar.extractfile(member)
                # TextIOWrapperでエンコーディングを指定
                text_stream = io.TextIOWrapper(f, encoding="utf-8")
                reader = csv.reader(text_stream, delimiter="\t")

                # ヘッダーを読み込む（最初のファイルのみ）
                current_header = next(reader)
                if not header:
                    header = current_header
                    # ヘッダーから数値列とカテゴリ列のインデックスを取得
                    numeric_indices = [
                        i for i, col in enumerate(header) if col.startswith("numeric_")
                    ]
                    category_index = header.index("string_col_0")

                # データ行を一行ずつ処理
                for row in reader:
                    total_rows += 1
                    # 数値データの合計を計算
                    for i in numeric_indices:
                        numeric_sums[header[i]] += int(row[i])
                    # カテゴリデータの出現頻度をカウント
                    category_counts[row[category_index]] += 1

    end_time = time.time()
    duration = end_time - start_time

    print(f"\n分析完了: {duration:.2f} 秒")
    print(f"総処理行数: {total_rows:,}")

    print("\n--- 数値列の合計 ---")
    for col, total in numeric_sums.items():
        print(f"{col}: {total:,}")

    print("\n--- カテゴリ(string_col_0)の出現頻度 (上位5件) ---")
    for category, count in category_counts.most_common(5):
        print(f"{category}: {count:,}")


if __name__ == "__main__":
    archive_path = "test_data.tar.gz"
    analyze_data_stream(archive_path)
