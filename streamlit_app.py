from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st


# --- データ読み込みとキャッシュ ---
@st.cache_data
def load_lazy_data(selected_files: list[Path]) -> pl.LazyFrame:
    """
    指定されたParquetファイルを遅延読み込みし、結合する。
    """
    if not selected_files:
        st.warning("データソースを1つ以上選択してください。")
        return pl.LazyFrame()

    try:
        lf_list = [pl.scan_parquet(file) for file in selected_files]
        full_lf = pl.concat(lf_list)
        return full_lf
    except Exception as e:
        st.error(f"データの読み込み中にエラーが発生しました: {e}")
        return pl.LazyFrame()


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

st.title("インタラクティブ・データ分析ダッシュボード")

# --- サイドバー ---
st.sidebar.header("表示設定")

# 1. データソース選択
input_dir = st.sidebar.text_input("データディレクトリ", "prepared_data")
data_dir = Path(input_dir)

if data_dir.exists() and data_dir.is_dir():
    # .parquetファイルを再帰的に検索
    available_files = sorted(list(data_dir.glob("**/*.parquet")))
    # data_dirからの相対パスを生成
    available_filenames = [str(f.relative_to(data_dir)) for f in available_files]

    selected_filenames = st.sidebar.multiselect(
        "データソースを選択",
        options=available_filenames,
        default=available_filenames,
    )
    # 絶対パスに変換
    selected_files_paths = [data_dir / name for name in selected_filenames]
else:
    st.sidebar.warning(f"`{input_dir}` ディレクトリが見つかりません。")
    selected_files_paths = []

# データの読み込み
if selected_files_paths:
    lf = load_lazy_data(selected_files_paths)
else:
    lf = pl.LazyFrame()


if len(lf.columns) > 0:
    # 2. 集計カテゴリ列の選択
    categorical_cols = [col for col, dtype in lf.schema.items() if dtype == pl.Utf8]
    if "score_level" in categorical_cols:
        categorical_cols.remove("score_level")  # フィルタ用なので除外

    if categorical_cols:
        agg_col = st.sidebar.selectbox(
            "集計に使用するカテゴリ列",
            options=categorical_cols,
            index=categorical_cols.index("string_col_0")
            if "string_col_0" in categorical_cols
            else 0,
        )
    else:
        agg_col = None
        st.sidebar.warning("集計可能なカテゴリ列が見つかりません。")

    # 3. 上位N件の選択
    top_n = st.sidebar.number_input(
        "グラフに表示する上位カテゴリ数", min_value=1, max_value=50, value=10, step=1
    )

    # --- フィルタ設定 ---
    st.sidebar.header("フィルタ設定")
    # .collect() を使って実際の値を取得
    fraud_options = (
        lf.select("is_fraud").unique().collect().get_column("is_fraud").sort().to_list()
    )
    selected_fraud = st.sidebar.multiselect(
        "不正フラグ (is_fraud)", options=fraud_options, default=fraud_options
    )
    level_options = (
        lf.select("score_level")
        .unique()
        .collect()
        .get_column("score_level")
        .sort()
        .to_list()
    )
    selected_levels = st.sidebar.multiselect(
        "スコアレベル (score_level)", options=level_options, default=level_options
    )

    # --- データのフィルタリング ---
    filtered_lf = lf.filter(
        (pl.col("is_fraud").is_in(selected_fraud))
        & (pl.col("score_level").is_in(selected_levels))
    )

    # --- メインコンテンツ ---
    if agg_col:
        st.header(f"`{agg_col}`別 サマリー")

        # ここで計算を実行
        top_by_count = (
            filtered_lf.group_by(agg_col)
            .agg(pl.len().alias("レコード数"))
            .sort("レコード数", descending=True)
            .head(100)
            .collect()
        )
        top_by_value = (
            filtered_lf.group_by(agg_col)
            .agg(pl.sum("EVENT_VALUE").alias("EVENT_VALUE合計"))
            .sort("EVENT_VALUE合計", descending=True)
            .head(100)
            .collect()
        )

        if top_by_count.is_empty():
            st.warning("選択された条件に一致するデータがありません。")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("レコード数 トップ100")
                st.dataframe(top_by_count)
            with col2:
                st.subheader("EVENT_VALUE合計 トップ100")
                st.dataframe(top_by_value)

            st.header("月次トレンド分析")

            # --- グラフ1: レコード数 ---
            top_n_by_count_cats = top_by_count.head(top_n)[agg_col].to_list()
            df_for_count_chart = filtered_lf.with_columns(
                pl.when(pl.col(agg_col).is_in(top_n_by_count_cats))
                .then(pl.col(agg_col))
                .otherwise(pl.lit("Other"))
                .alias("category_group")
            )
            summary_by_count = (
                df_for_count_chart.group_by(["event_month", "category_group"])
                .agg(pl.len().alias("record_count"))
                .sort("event_month")
                .collect()
            )
            fig1 = px.bar(
                summary_by_count,
                x="event_month",
                y="record_count",
                color="category_group",
                title=f"月別レコード数（{agg_col} 上位{top_n}カテゴリ別内訳）",
                labels={
                    "event_month": "月",
                    "record_count": "レコード数",
                    "category_group": "カテゴリ",
                },
                category_orders={"category_group": top_n_by_count_cats + ["Other"]},
            )
            fig1.update_traces(
                hovertemplate="<b>%{x}</b><br>カテゴリ: %{fullData.name}<br>レコード数: %{y}<extra></extra>"
            )
            st.plotly_chart(fig1, use_container_width=True)

            # --- グラフ2: イベント価値合計 ---
            top_n_by_value_cats = top_by_value.head(top_n)[agg_col].to_list()
            df_for_value_chart = filtered_lf.with_columns(
                pl.when(pl.col(agg_col).is_in(top_n_by_value_cats))
                .then(pl.col(agg_col))
                .otherwise(pl.lit("Other"))
                .alias("category_group")
            )
            summary_by_value = (
                df_for_value_chart.group_by(["event_month", "category_group"])
                .agg(pl.sum("EVENT_VALUE").alias("event_value_sum"))
                .sort("event_month")
                .collect()
            )
            fig2 = px.bar(
                summary_by_value,
                x="event_month",
                y="event_value_sum",
                color="category_group",
                title=f"月別イベント価値合計（{agg_col} 上位{top_n}カテゴリ別内訳）",
                labels={
                    "event_month": "月",
                    "event_value_sum": "価値合計",
                    "category_group": "カテゴリ",
                },
                category_orders={"category_group": top_n_by_value_cats + ["Other"]},
            )
            fig2.update_traces(
                hovertemplate="<b>%{x}</b><br>カテゴリ: %{fullData.name}<br>価値合計: %{y:,.0f}<extra></extra>"
            )
            st.plotly_chart(fig2, use_container_width=True)

    else:
        st.info("サイドバーで集計する列を選択してください。")
else:
    st.info("サイドバーで表示するデータソースを選択してください。")
