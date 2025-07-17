from pathlib import Path
from typing import List

import pandas as pd
import plotly.express as px
import streamlit as st

# --- 時間集計単位の定数 ---
TIME_AGG_OPTIONS = {"月次": "M", "週次": "W", "日次": "D"}


# --- データ読み込みとキャッシュ ---
# HDF5からのデータ読み込みは高速なため、Streamlitのキャッシュは限定的に使用
def get_store(file_path: Path) -> pd.HDFStore:
    """HDFStoreオブジェクトを取得する。"""
    try:
        return pd.HDFStore(file_path, "r")
    except Exception as e:
        st.error(f"HDF5ファイルの読み込みに失敗しました: {e}")
        return None


def get_unique_values(stores: List[pd.HDFStore], column: str) -> List:
    """複数のHDF5ストアから指定された列のユニークな値を取得する。"""
    unique_values = set()
    for store in stores:
        try:
            # クエリをかけてユニークな値を取得
            s = store.select("data", columns=[column]).iloc[:, 0].unique()
            unique_values.update(s)
        except (KeyError, IndexError):
            # カラムが存在しない場合はスキップ
            pass
        except Exception as e:
            st.warning(f"列 '{column}' の値取得中にエラー: {e}")
    return sorted(list(unique_values))


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

st.title("HDF5インタラクティブ・データ分析ダッシュボード")
st.info("HDF5ファイルの 'where' 句を利用して、必要なデータのみを読み込みます。")

# --- サイドバー ---
st.sidebar.header("表示設定")

# 1. データソース選択
input_dir = st.sidebar.text_input("データディレクトリ", "hdf_data")
data_dir = Path(input_dir)

if data_dir.exists() and data_dir.is_dir():
    available_files = sorted(list(data_dir.glob("**/*.h5")))
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

# HDFStoreオブジェクトのリストを作成
stores = [get_store(p) for p in selected_files_paths if p.exists()]
stores = [s for s in stores if s]  # Noneを除外

if stores:
    # 2. 集計カテゴリ列の選択
    # 最初のストアから列名を取得（全ファイル同じスキーマと仮定）
    all_cols = stores[0].get_storer("data").table.description._v_names
    categorical_cols = [
        col for col in all_cols if "string_col" in col or col == "score_level"
    ]
    if "score_level" in categorical_cols:
        categorical_cols.remove("score_level")

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

    # 4. 時間軸の選択
    time_agg_label = st.sidebar.radio(
        "時間集計単位", options=list(TIME_AGG_OPTIONS.keys()), horizontal=True
    )
    time_agg_unit = TIME_AGG_OPTIONS[time_agg_label]

    # --- フィルタ設定 ---
    st.sidebar.header("フィルタ設定")
    fraud_options = get_unique_values(stores, "is_fraud")
    selected_fraud = st.sidebar.multiselect(
        "不正フラグ (is_fraud)", options=fraud_options, default=fraud_options
    )

    level_options = get_unique_values(stores, "score_level")
    selected_levels = st.sidebar.multiselect(
        "スコアレベル (score_level)", options=level_options, default=level_options
    )

    # --- 分析実行ボタン ---
    if st.sidebar.button("分析実行", type="primary"):
        # --- データのフィルタリングと読み込み ---
        where_conditions = []
        if selected_fraud:
            where_conditions.append(f"is_fraud in {selected_fraud}")
        if selected_levels:
            where_conditions.append(f"score_level in {selected_levels}")

        where_query = " & ".join(where_conditions) if where_conditions else None

        # 必要な列だけを読み込む
        required_cols = list(
            set([agg_col, "EVENT_VALUE", "EVENT_TIME", "is_fraud", "score_level"])
        )

        df_list = []
        try:
            with st.spinner("データをHDF5から読み込み中..."):
                for store in stores:
                    df_list.append(
                        store.select("data", where=where_query, columns=required_cols)
                    )

            if not df_list:
                st.warning("データがありません。")
                st.stop()

            filtered_df = pd.concat(df_list, ignore_index=True)

            if filtered_df.empty:
                st.warning("選択された条件に一致するデータがありません。")
                st.stop()

        except Exception as e:
            st.error(f"データ読み込み中にエラーが発生しました: {e}")
            st.stop()

        # --- メインコンテンツ ---
        if agg_col:
            st.header(f"`{agg_col}`別 サマリー")

            top_by_count = (
                filtered_df.groupby(agg_col)
                .size()
                .nlargest(100)
                .reset_index(name="レコード数")
            )
            top_by_value = (
                filtered_df.groupby(agg_col)["EVENT_VALUE"]
                .sum()
                .nlargest(100)
                .reset_index(name="EVENT_VALUE合計")
            )

            col1, col2 = st.columns(2)
            with col1:
                st.subheader("レコード数 トップ100")
                st.dataframe(top_by_count, use_container_width=True)
            with col2:
                st.subheader("EVENT_VALUE合計 トップ100")
                st.dataframe(top_by_value, use_container_width=True)

            st.header(f"{time_agg_label}トレンド分析")

            # --- 集計用ヘルパー関数 ---
            def aggregate_for_chart(
                df: pd.DataFrame,
                agg_col: str,
                top_cats: list,
                time_agg_unit: str,
                value_col: str = None,
            ) -> pd.DataFrame:
                df["category_group"] = df[agg_col].where(
                    df[agg_col].isin(top_cats), "Other"
                )
                df["time_agg"] = (
                    df["EVENT_TIME"].dt.to_period(time_agg_unit).dt.start_time
                )

                if value_col:
                    summary = df.groupby(["time_agg", "category_group"])[
                        value_col
                    ].sum()
                else:
                    summary = df.groupby(["time_agg", "category_group"]).size()

                return summary.reset_index(name="value")

            # --- グラフ1: レコード数 ---
            top_n_by_count_cats = top_by_count.head(top_n)[agg_col].to_list()
            summary_by_count = aggregate_for_chart(
                filtered_df, agg_col, top_n_by_count_cats, time_agg_unit
            )
            fig1 = px.bar(
                summary_by_count,
                x="time_agg",
                y="value",
                color="category_group",
                title=f"{time_agg_label}レコード数（{agg_col} 上位{top_n}カテゴリ別内訳）",
                labels={
                    "time_agg": time_agg_label,
                    "value": "レコード数",
                    "category_group": "カテゴリ",
                },
                category_orders={"category_group": top_n_by_count_cats + ["Other"]},
            )
            st.plotly_chart(fig1, use_container_width=True)

            # --- グラフ2: イベント価値合計 ---
            top_n_by_value_cats = top_by_value.head(top_n)[agg_col].to_list()
            summary_by_value = aggregate_for_chart(
                filtered_df,
                agg_col,
                top_n_by_value_cats,
                time_agg_unit,
                value_col="EVENT_VALUE",
            )
            fig2 = px.bar(
                summary_by_value,
                x="time_agg",
                y="value",
                color="category_group",
                title=f"{time_agg_label}イベント価値合計（{agg_col} 上位{top_n}カテゴリ別内訳）",
                labels={
                    "time_agg": time_agg_label,
                    "value": "価値合計",
                    "category_group": "カテゴリ",
                },
                category_orders={"category_group": top_n_by_value_cats + ["Other"]},
            )
            st.plotly_chart(fig2, use_container_width=True)

    else:
        st.info("サイドバーで条件を設定し、「分析実行」ボタンを押してください。")

else:
    st.info("サイドバーで表示するデータソースを選択してください。")

# --- ストアを閉じる ---
for store in stores:
    if store.is_open:
        store.close()
