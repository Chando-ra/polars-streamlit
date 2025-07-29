import csv
import datetime
import random
import string


def generate_random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def generate_random_timestamp(start_date, end_date):
    """Generate a random timestamp between two dates."""
    time_between_dates = end_date - start_date
    seconds_in_day = 24 * 60 * 60
    total_seconds = (
        time_between_dates.days * seconds_in_day + time_between_dates.seconds
    )
    random_second = random.randrange(total_seconds)
    random_date = start_date + datetime.timedelta(seconds=random_second)
    return random_date.strftime("%Y-%m-%d %H:%M:%S")


def create_test_data(file_path, num_rows, num_cols, num_categories=1000):
    """
    数値、日付、カテゴリ、真偽値など、多様なデータ型を含むテスト用のTSVファイルを生成する。
    """
    categories = [generate_random_string() for _ in range(num_categories)]
    rule_list = [f"ルール{i}" for i in range(1, 201)]

    # 日付生成用の期間設定
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="\t")

        # ヘッダーを作成
        header = ["SCORE", "string_col_0", "EVENT_VALUE", "is_fraud", "EVENT_TIME"]
        # 残りの列を追加
        for i in range(5, num_cols):
            if i % 2 != 0:
                header.append(f"string_col_{i // 2}")
            else:
                header.append(f"numeric_col_{i // 2}")
        header.append("hit_rule")
        writer.writerow(header)

        # データ行を生成
        for _ in range(num_rows):
            # is_fraudを先に決定
            is_fraud = random.random() < 0.01

            row = []
            for i in range(num_cols):
                # 5%の確率で欠損値を生成
                if random.random() < 0.05:
                    row.append("")
                    continue

                if i == 0:  # SCORE
                    # is_fraudの値に基づいてスコアの分布を明確に分離
                    if is_fraud:
                        # 不正なデータはスコアが高い (1500-2500)
                        row.append(random.randint(1500, 2500))
                    else:
                        # 正常なデータはスコアが低い (-1000 - 1499)
                        row.append(random.randint(-1000, 1499))
                elif i == 1:  # string_col_0
                    row.append(random.choice(categories))
                elif i == 2:  # EVENT_VALUE
                    row.append(random.randint(0, 1_000_000))
                elif i == 3:  # is_fraud
                    row.append(is_fraud)
                elif i == 4:  # EVENT_TIME
                    row.append(generate_random_timestamp(start_date, end_date))
                # --- 残りの列 ---
                elif i % 2 != 0:  # string
                    row.append(generate_random_string())
                else:  # numeric
                    row.append(random.randint(0, 1_000_000))

            # hit_ruleカラムの生成
            num_rules = random.randint(0, 5)  # 0から5個のルールをランダムに選択
            selected_rules = random.sample(rule_list, num_rules)
            row.append(" ".join(selected_rules))
            writer.writerow(row)


if __name__ == "__main__":
    file_path = "input_data/test_data.tsv"
    num_rows = 1_000_000  # 少し小さくして実行時間を短縮
    num_cols = 10
    print(f"Generating {num_rows} rows of test data in '{file_path}'...")
    create_test_data(file_path, num_rows, num_cols)
    print("Test data generation complete.")
