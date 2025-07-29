from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# --- DuckDBコネクションのセットアップ ---
@st.cache_resource
def get_db_connection():
    """インメモリのDuckDBコネクションを生成・キャッシュする。"""
    return duckdb.connect(database=":memory:", read_only=False)


def load_data_into_duckdb(con, selected_files: list[Path]) -> bool:
    """
    指定されたParquetファイルをDuckDBのビューとして読み込む。
    成功すればTrue、失敗すればFalseを返す。
    """
    if not selected_files:
        st.warning("データソースを1つ以上選択してください。")
        return False

    # ファイルパスのリストをSQLで使える形式 (['path1', 'path2', ...]) に変換
    file_list_str = ", ".join([f"'{str(p)}'" for p in selected_files])

    try:
        # Parquetファイルからビューを作成
        con.execute(
            f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM read_parquet([{file_list_str}])"
        )
        return True
    except Exception as e:
        st.error(f"データの読み込み中にエラーが発生しました: {e}")
        return False


# --- メインアプリケーション ---
st.set_page_config(layout="wide")

# --- カスタムCSSでサイドバーの幅を調整 ---
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] {
        width: 450px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("取引分析・可視化ダッシュボード")

# --- サイドバー ---
st.sidebar.header("分析設定")

# 1. データソース選択
input_dir = st.sidebar.text_input("データディレクトリ", "prepared_data")
data_dir = Path(input_dir)

if data_dir.exists() and data_dir.is_dir():
    available_files = sorted(list(data_dir.glob("**/*.parquet")))
    available_filenames = [str(f.relative_to(data_dir)) for f in available_files]

    selected_filenames = st.sidebar.multiselect(
        "データソースを選択",
        options=available_filenames,
        default=available_filenames,
    )
    selected_files_paths = [data_dir / name for name in selected_filenames]
else:
    st.sidebar.warning(f"`{input_dir}` ディレクトリが見つかりません。")
    selected_files_paths = []

# DuckDBコネクションを取得し、データを読み込む
con = get_db_connection()
data_loaded = False
if selected_files_paths:
    data_loaded = load_data_into_duckdb(con, selected_files_paths)
else:
    # データが選択されていない場合はビューをクリア
    con.execute("DROP VIEW IF EXISTS source_data")


if data_loaded:
    # 2. 分析軸となる列の選択
    schema_df = con.execute("DESCRIBE source_data").fetchdf()
    all_cols = schema_df["column_name"].tolist()

    # 除外したい基本的な列
    cols_to_exclude = {"EVENT_TIME", "SCORE", "is_fraud", "is_correct_label"}
    # ユーザーが選択可能な列
    selectable_cols = [
        col
        for col in all_cols
        if col not in cols_to_exclude and not col.startswith("__")
    ]

    selected_dims = st.sidebar.multiselect(
        "分析軸を選択（複数選択可）",
        options=selectable_cols,
        default=selectable_cols[:2] if len(selectable_cols) > 1 else selectable_cols,
    )

    # 3. 選択された分析軸の値でフィルタ
    st.sidebar.header("フィルタ設定")

    # 各選択列のユニーク値を取得し、ユーザーに選択させる
    filters = {}
    for col in selected_dims:
        try:
            # DuckDBは列名にスペースや特殊文字が含まれる場合に備えてダブルクォートで囲むのが安全
            distinct_values_df = con.execute(
                f'SELECT DISTINCT "{col}" FROM source_data ORDER BY 1'
            ).fetchdf()
            distinct_values = distinct_values_df[col].tolist()

            # 手入力用の選択肢
            MANUAL_INPUT_OPTION = "（値を直接入力する）"
            options = [MANUAL_INPUT_OPTION] + distinct_values

            # 値が多すぎる場合は警告
            if len(distinct_values) > 500:
                st.sidebar.warning(
                    f"`{col}`のユニーク値が多すぎます(>500)。最初の500件のみ表示します。"
                )
                options = [MANUAL_INPUT_OPTION] + distinct_values[:500]

            selected_value = st.sidebar.selectbox(
                f"「{col}」の値を選択",
                options=options,
            )

            # 手入力が選択された場合の処理
            if selected_value == MANUAL_INPUT_OPTION:
                input_value = st.sidebar.text_input(
                    f"「{col}」の値を入力", key=f"text_input_{col}"
                )
                if input_value:
                    filters[col] = [input_value]  # フィルタはリスト形式を維持
            elif selected_value:
                filters[col] = [selected_value]  # フィルタはリスト形式を維持
        except Exception as e:
            st.sidebar.error(f"`{col}`の値取得中にエラー: {e}")

    # 4. リスク閾値とスコア調整
    st.sidebar.header("リスク閾値とスコア調整")
    threshold_low = st.sidebar.number_input("低リスクの上限スコア", value=1000, step=1)
    threshold_mid = st.sidebar.number_input("中リスクの上限スコア", value=1500, step=1)

    if threshold_low >= threshold_mid:
        st.sidebar.error("低リスクの閾値は中リスクの閾値より小さくしてください。")
        st.stop()

    score_adjustment = st.sidebar.slider(
        "スコア調整値", min_value=-1000, max_value=1000, value=0, step=1
    )

    # --- フィルタリング済みデータのビューを作成 ---
    where_clauses = []

    # 1. 列の値に基づくフィルタ
    for col, values in filters.items():
        if values:  # 選択された値がある場合のみ
            # 値をSQLクエリ用にフォーマット (SQLインジェクション対策でシングルクォートをエスケープ)
            safe_values = [str(v).replace("'", "''") for v in values]
            formatted_values = ", ".join([f"'{v}'" for v in safe_values])
            where_clauses.append(f'"{col}" IN ({formatted_values})')

    # WHERE句を組み立て
    # スコアでのフィルタリングはビュー作成時には行わず、後段の分析で利用する
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    filtered_view_query = f"""
        CREATE OR REPLACE VIEW filtered_data AS
        SELECT *
        FROM source_data
        WHERE {where_sql}
    """
    con.execute(filtered_view_query)

    # --- デバッグ情報 ---
    st.sidebar.subheader("Debug Info")
    try:
        source_count = con.execute("SELECT COUNT(*) FROM source_data").fetchone()[0]
        st.sidebar.write(f"Source records: {source_count:,}")

        st.sidebar.code(filtered_view_query, language="sql")

        filtered_count = con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
        st.sidebar.write(f"Filtered records: {filtered_count:,}")
    except Exception as e:
        st.sidebar.error(f"Debug Error: {e}")

    # --- メインコンテンツ ---
    if selected_dims:
        st.header("分析結果")

        # フィルタされたデータがない場合の表示
        filtered_count = con.execute("SELECT COUNT(*) FROM filtered_data").fetchone()[0]
        if filtered_count == 0:
            st.warning("選択された条件に一致するデータがありません。")
            st.stop()

        # --- 1. 件数サマリー ---
        st.subheader("件数サマリー")

        # is_correct_label列の存在をチェック
        has_correct_label_col = "is_correct_label" in all_cols

        # is_correct_label列がある場合のみ、サマリークエリに追加
        misclassified_query_part = (
            ", SUM(CASE WHEN is_correct_label = FALSE THEN 1 ELSE 0 END) AS misclassified_count"
            if has_correct_label_col
            else ""
        )

        summary_query = f"""
        SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count
            {misclassified_query_part}
        FROM filtered_data
        """
        summary_df = con.execute(summary_query).fetchdf()

        total_count = summary_df["total_count"][0]
        fraud_count = summary_df["fraud_count"][0]

        # 表示する列を動的に決定
        if has_correct_label_col:
            misclassified_count = summary_df["misclassified_count"][0]
            col1, col2, col3 = st.columns(3)
            col1.metric("総件数", f"{total_count:,}")
            col2.metric("不正検知件数", f"{fraud_count:,}")
            col3.metric("誤検知件数", f"{misclassified_count:,}")
        else:
            col1, col2 = st.columns(2)
            col1.metric("総件数", f"{total_count:,}")
            col2.metric("不正検知件数", f"{fraud_count:,}")

        # --- 2. 混合行列 ---
        st.subheader("混合行列")
        # is_fraud: 実際のラベル
        # 予測ラベル: (SCORE + 調整値) が中リスク上限以上ならPositive(不正疑い)
        # TP: is_fraud=True, 予測=Positive
        # FP: is_fraud=False, 予測=Positive
        # TN: is_fraud=False, 予測=Negative
        # FN: is_fraud=True, 予測=Negative
        confusion_matrix_query = f"""
        WITH adjusted_score_data AS (
            SELECT *, (SCORE + {score_adjustment}) AS adjusted_score FROM filtered_data
        )
        SELECT
            SUM(CASE WHEN is_fraud = TRUE AND adjusted_score >= {threshold_mid} THEN 1 ELSE 0 END) AS tp,
            SUM(CASE WHEN is_fraud = FALSE AND adjusted_score >= {threshold_mid} THEN 1 ELSE 0 END) AS fp,
            SUM(CASE WHEN is_fraud = FALSE AND adjusted_score < {threshold_mid} THEN 1 ELSE 0 END) AS tn,
            SUM(CASE WHEN is_fraud = TRUE AND adjusted_score < {threshold_mid} THEN 1 ELSE 0 END) AS fn
        FROM adjusted_score_data
        """
        cm_df = con.execute(confusion_matrix_query).fetchdf()
        tp = cm_df["tp"][0] or 0
        fp = cm_df["fp"][0] or 0
        tn = cm_df["tn"][0] or 0
        fn = cm_df["fn"][0] or 0

        # 可視化 (Plotlyヒートマップ)
        z = [[int(tp), int(fn)], [int(fp), int(tn)]]
        x = ["Actual: Fraud", "Actual: Not Fraud"]
        y = ["Predicted: Fraud", "Predicted: Not Fraud"]

        # ラベル（テキスト）もzと同じ構造で作成
        z_text = [[str(y) for y in x] for x in z]

        fig_cm = px.imshow(
            z,
            labels=dict(x="Actual Condition", y="Predicted Condition", color="Count"),
            x=x,
            y=y,
            text_auto=True,
            color_continuous_scale="Blues",
        )

        fig_cm.update_layout(title_text="<i><b>Confusion Matrix</b></i>")
        st.plotly_chart(fig_cm, use_container_width=False)

        # --- 2.1 評価指標の計算と表示 ---
        # Recall = TP / (TP + FN)
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        # False Positive Rate = FP / (FP + TN)
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0

        col1, col2 = st.columns(2)
        col1.metric("リコール (再現率)", f"{recall:.2%}")
        col2.metric("偽陽性率 (FPR)", f"{fpr:.2%}")

        # --- 3. スコアのヒストグラム（第二軸対応） ---
        st.subheader("調整後スコアの分布")
        hist_df = con.execute(
            f"SELECT (SCORE + {score_adjustment}) AS adjusted_score, is_fraud FROM filtered_data"
        ).fetchdf()

        # データを不正と正常に分離
        fraud_scores = hist_df[hist_df["is_fraud"] == True]["adjusted_score"]
        normal_scores = hist_df[hist_df["is_fraud"] == False]["adjusted_score"]

        # 全体のスコア範囲に基づいてビニングを決定
        min_score, max_score = (
            hist_df["adjusted_score"].min(),
            hist_df["adjusted_score"].max(),
        )
        bins = np.linspace(min_score, max_score, 101)  # 100個のビン

        # 各データセットのヒストグラムを計算
        normal_counts, bin_edges = np.histogram(normal_scores, bins=bins)
        fraud_counts, _ = np.histogram(fraud_scores, bins=bins)

        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

        # グラフを作成
        fig_hist = go.Figure()

        # 正常データのバーを追加 (プライマリY軸)
        fig_hist.add_trace(
            go.Bar(
                x=bin_centers,
                y=normal_counts,
                name="Normal",
                marker_color="blue",
                opacity=0.6,
            )
        )

        # 不正データのバーを追加 (セカンダリY軸)
        fig_hist.add_trace(
            go.Bar(
                x=bin_centers,
                y=fraud_counts,
                name="Fraud",
                marker_color="red",
                opacity=0.6,
                yaxis="y2",
            )
        )

        # レイアウトを更新
        fig_hist.update_layout(
            title_text="調整後スコアのヒストグラム（不正フラグ別）",
            xaxis_title="調整後スコア",
            yaxis_title="正常(Normal)件数",
            yaxis2=dict(title="不正(Fraud)件数", overlaying="y", side="right"),
            barmode="overlay",
            legend=dict(x=0, y=1.0),
        )

        # 判定閾値の垂直線を追加
        fig_hist.add_vline(
            x=threshold_mid,
            line_width=2,
            line_dash="dash",
            line_color="black",
            annotation_text="不正判定閾値",
            annotation_position="top right",
        )

        st.plotly_chart(fig_hist, use_container_width=True)

    else:
        st.info("サイドバーで分析軸となる列を1つ以上選択してください。")
else:
    st.info("サイドバーで表示するデータソースを選択してください。")
