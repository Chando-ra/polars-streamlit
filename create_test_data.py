import csv
import random
import string


def generate_random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def create_test_data(file_path, num_rows, num_cols, num_categories=1000):
    """
    数値データと文字列データを混合したテスト用のTSVファイルを生成する。
    string_col_0 はカテゴリカルデータになるようにする。
    """
    # カテゴリカルデータ用の文字列リストを事前に生成
    categories = [generate_random_string() for _ in range(num_categories)]
    categories2 = [generate_random_string() for _ in range(num_categories)]

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")

        # ヘッダーを作成
        header = []
        for i in range(num_cols):
            if i == 0:
                header.append("SCORE")
            elif i == 2:
                header.append("EVENT_VALUE")
            elif i % 2 == 0:
                header.append(f"numeric_col_{i // 2}")
            else:
                header.append(f"string_col_{i // 2}")
        writer.writerow(header)

        # データ行を生成
        for _ in range(num_rows):
            row = []
            for i in range(num_cols):
                if i == 0:
                    # SCORE: -1000から2000の整数
                    row.append(random.randint(-1000, 2000))
                elif i == 2:
                    # EVENT_VALUE: 0以上の整数
                    row.append(random.randint(0, 1_000_000))
                elif i % 2 == 0:
                    # その他の数値データ
                    row.append(random.randint(0, 1_000_000))
                else:
                    # 文字列データ
                    if i == 1:  # string_col_0 の場合
                        row.append(random.choice(categories))
                    elif i == 3:  # string_col_1 の場合
                        row.append(random.choice(categories2))
                    else:
                        row.append(generate_random_string())
            writer.writerow(row)


if __name__ == "__main__":
    file_path = "test_data_500.tsv"
    num_rows = 5_000_000
    num_cols = 10
    print(f"Generating {num_rows} rows of test data in '{file_path}'...")
    create_test_data(file_path, num_rows, num_cols)
    print("Test data generation complete.")
