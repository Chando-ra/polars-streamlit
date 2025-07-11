import time

from memory_profiler import memory_usage

from analyze_data import analyze_with_polars_fast, analyze_with_polars_low_memory


def profile_function(func, file_path):
    """指定された関数をプロファイリングし、実行時間とメモリ使用量を返す"""

    start_time = time.time()

    # メモリ使用量を測定しながら関数を実行
    mem_usage, (duration, numeric_sums, pivot_results) = memory_usage(
        (func, (file_path,)),
        retval=True,  # 関数の戻り値も取得する
        interval=0.1,
    )

    end_time = time.time()

    print(f"\n分析完了: {end_time - start_time:.2f} 秒 (内部計測: {duration:.2f} 秒)")
    print(f"ピークメモリ使用量: {max(mem_usage):.2f} MiB")

    print("\n--- 数値列の合計 ---")
    print(numeric_sums)
    print("\n--- ピボット集計結果 (上位5件) ---")
    print(pivot_results.head())
    print(f"\nピボット集計の総行数: {len(pivot_results)}")
    print("-" * 30)


if __name__ == "__main__":
    file_path = "test_data.tsv"

    print("=" * 10 + " 高速モードのプロファイリング " + "=" * 10)
    profile_function(analyze_with_polars_fast, file_path)

    print("\n" + "=" * 10 + " 低メモリモードのプロファイリング " + "=" * 10)
    profile_function(analyze_with_polars_low_memory, file_path)
