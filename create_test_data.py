import csv
import datetime
import random
import string


def generate_random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def generate_random_date(start_date, end_date):
    """Generate a random date between two dates."""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return random_date.strftime("%Y-%m-%d")


def create_test_data(file_path, num_rows, num_cols, num_categories=1000):
    """
    数値、日付、カテゴリ、真偽値など、多様なデータ型を含むテスト用のTSVファイルを生成する。
    """
    categories = [generate_random_string() for _ in range(num_categories)]

    # 日付生成用の期間設定
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=365)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")

        # ヘッダーを作成
        header = ["SCORE", "string_col_0", "EVENT_VALUE", "is_fraud", "EVENT_DATE"]
        # 残りの列を追加
        for i in range(5, num_cols):
            if i % 2 != 0:
                header.append(f"string_col_{i // 2}")
            else:
                header.append(f"numeric_col_{i // 2}")
        writer.writerow(header)

        # データ行を生成
        for _ in range(num_rows):
            row = []
            for i in range(num_cols):
                if i == 0:  # SCORE
                    row.append(random.randint(-1000, 2000))
                elif i == 1:  # string_col_0
                    row.append(random.choice(categories))
                elif i == 2:  # EVENT_VALUE
                    row.append(random.randint(0, 1_000_000))
                elif i == 3:  # is_fraud
                    # 約1%の確率でTrueを生成
                    row.append(random.random() < 0.01)
                elif i == 4:  # EVENT_DATE
                    row.append(generate_random_date(start_date, end_date))
                # --- 残りの列 ---
                elif i % 2 != 0:  # string
                    row.append(generate_random_string())
                else:  # numeric
                    row.append(random.randint(0, 1_000_000))
            writer.writerow(row)


if __name__ == "__main__":
    file_path = "test_data.tsv"
    num_rows = 10_000_000
    num_cols = 10
    print(f"Generating {num_rows} rows of test data in '{file_path}'...")
    create_test_data(file_path, num_rows, num_cols)
    print("Test data generation complete.")
