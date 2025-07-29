from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# --- 時間集計単位の定数 (DuckDB形式) ---
TIME_AGG_OPTIONS = {"月次": "month", "週次": "week", "日次": "day"}


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

st.title("インタラクティブ・データ分析ダッシュボード (DuckDB版)")

# --- サイドバー ---
st.sidebar.header("表示設定")

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
    # 2. 集計カテゴリ列の選択
    schema_df = con.execute("DESCRIBE source_data").fetchdf()
    # 文字列型の列を抽出
    categorical_cols = schema_df[schema_df["column_type"] == "VARCHAR"][
        "column_name"
    ].tolist()

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

    # is_fraud フラグのフィルタ
    fraud_options_df = con.execute(
        "SELECT DISTINCT is_fraud FROM source_data ORDER BY 1"
    ).fetchdf()
    fraud_options = fraud_options_df["is_fraud"].to_list()
    selected_fraud = st.sidebar.multiselect(
        "不正フラグ (is_fraud)", options=fraud_options, default=fraud_options
    )

    # スコア閾値の入力
    st.sidebar.subheader("スコアレベル設定")
    threshold_low = st.sidebar.number_input("低リスクの上限スコア", value=1000, step=1)
    threshold_mid = st.sidebar.number_input("中リスクの上限スコア", value=1500, step=1)

    if threshold_low >= threshold_mid:
        st.sidebar.error("低リスクの閾値は中リスクの閾値より小さくしてください。")
        st.stop()

    # score_levelを動的に生成するSQLのCASE文
    score_level_case_stmt = f"""
        CASE
            WHEN SCORE <= {threshold_low} THEN 'Low'
            WHEN SCORE <= {threshold_mid} THEN 'Mid'
            ELSE 'High'
        END AS score_level
    """

    # score_levelのフィルタ
    level_options = ["Low", "Mid", "High"]
    selected_levels = st.sidebar.multiselect(
        "スコアレベル (動的)", options=level_options, default=level_options
    )

    # --- フィルタリング済みデータのビューを作成 ---
    # フィルタ条件をSQLのWHERE句に変換
    def build_in_clause(values: list) -> str:
        """SQLのIN句の文字列を安全に構築する。"""
        if not values:
            return "1=1"  # 常にTrue

        def format_value(v):
            if isinstance(v, str):
                return f"'{v}'"
            return str(v)  # booleanや数値はそのまま文字列化

        items = ", ".join([format_value(v) for v in values])
        return f"IN ({items})"

    fraud_filter = f"is_fraud {build_in_clause(selected_fraud)}"
    level_filter = f"score_level {build_in_clause(selected_levels)}"

    filtered_view_query = f"""
        CREATE OR REPLACE VIEW filtered_data AS
        SELECT *
        FROM (
            SELECT * EXCLUDE (score_level), {score_level_case_stmt}
            FROM source_data
        )
        WHERE {fraud_filter} AND {level_filter}
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
    if agg_col:
        st.header(f"`{agg_col}`別 サマリー")

        # --- ここで計算を実行 ---
        top_by_count_query = f"""
            SELECT {agg_col}, COUNT(*) AS "レコード数"
            FROM filtered_data
            GROUP BY {agg_col}
            ORDER BY "レコード数" DESC
            LIMIT 100
        """
        top_by_value_query = f"""
            SELECT {agg_col}, SUM(EVENT_VALUE) AS "EVENT_VALUE合計"
            FROM filtered_data
            GROUP BY {agg_col}
            ORDER BY "EVENT_VALUE合計" DESC
            LIMIT 100
        """
        top_by_count = con.execute(top_by_count_query).fetchdf()
        top_by_value = con.execute(top_by_value_query).fetchdf()

        if top_by_count.empty:
            st.warning("選択された条件に一致するデータがありません。")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("レコード数 トップ100")
                st.data_editor(
                    top_by_count,
                    column_config={
                        "レコード数": st.column_config.NumberColumn(format="localized")
                    },
                    use_container_width=True,
                )
            with col2:
                st.subheader("EVENT_VALUE合計 トップ100")
                st.data_editor(
                    top_by_value,
                    column_config={
                        "EVENT_VALUE合計": st.column_config.NumberColumn(
                            format="¥{value:,.0f}"
                        )
                    },
                    use_container_width=True,
                )

            st.header(f"{time_agg_label}トレンド分析")

            # --- 集計用ヘルパー関数 ---
            def aggregate_for_chart(
                agg_col: str,
                top_cats: list,
                time_agg_unit: str,
                value_col: str = None,
            ) -> pd.DataFrame:
                def build_in_clause_for_agg(values: list) -> str:
                    """集計用のIN句ビルダー"""
                    if not values:
                        return "IN ('')"  # 空のIN句
                    # カテゴリ列は常に文字列と想定
                    items = ", ".join([f"'{v}'" for v in values])
                    return f"IN ({items})"

                category_group_case = f"""
                    CASE
                        WHEN {agg_col} {build_in_clause_for_agg(top_cats)} THEN {agg_col}
                        ELSE 'Other'
                    END AS category_group
                """

                time_agg_expr = f"date_trunc('{time_agg_unit}', EVENT_TIME) AS time_agg"

                if value_col:
                    agg_expr = f"SUM({value_col}) AS value_agg"
                    group_by_cols = "time_agg, category_group"
                else:
                    agg_expr = "COUNT(*) AS value_agg"
                    group_by_cols = "time_agg, category_group"

                query = f"""
                    SELECT
                        {time_agg_expr},
                        {category_group_case},
                        {agg_expr}
                    FROM filtered_data
                    GROUP BY {group_by_cols}
                    ORDER BY time_agg
                """
                return con.execute(query).fetchdf()

            # --- グラフ1: レコード数 ---
            top_n_by_count_cats = top_by_count.head(top_n)[agg_col].to_list()
            summary_by_count = aggregate_for_chart(
                agg_col, top_n_by_count_cats, time_agg_unit
            )
            fig1 = px.bar(
                summary_by_count,
                x="time_agg",
                y="value_agg",
                color="category_group",
                title=f"{time_agg_label}レコード数（{agg_col} 上位{top_n}カテゴリ別内訳）",
                labels={
                    "time_agg": time_agg_label,
                    "value_agg": "レコード数",
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
            summary_by_value = aggregate_for_chart(
                agg_col, top_n_by_value_cats, time_agg_unit, value_col="EVENT_VALUE"
            )
            fig2 = px.bar(
                summary_by_value,
                x="time_agg",
                y="value_agg",
                color="category_group",
                title=f"{time_agg_label}イベント価値合計（{agg_col} 上位{top_n}カテゴリ別内訳）",
                labels={
                    "time_agg": time_agg_label,
                    "value_agg": "価値合計",
                    "category_group": "カテゴリ",
                },
                category_orders={"category_group": top_n_by_value_cats + ["Other"]},
            )
            fig2.update_traces(
                hovertemplate="<b>%{x}</b><br>カテゴリ: %{fullData.name}<br>価値合計: ¥%{y:,.0f}<extra></extra>"
            )
            st.plotly_chart(fig2, use_container_width=True)

            # --- 曜日・時間帯別 ヒートマップ ---
            st.header("曜日・時間帯別 アクティビティヒートマップ")

            # 曜日と時間を抽出して件数をカウントするクエリ
            heatmap_query = """
                SELECT
                    EXTRACT(isodow FROM EVENT_TIME) AS day_of_week, -- 月曜:1, 日曜:7
                    EXTRACT(hour FROM EVENT_TIME) AS hour_of_day,
                    COUNT(*) AS record_count
                FROM filtered_data
                GROUP BY day_of_week, hour_of_day
            """
            try:
                heatmap_df = con.execute(heatmap_query).fetchdf()

                if not heatmap_df.empty:
                    # 曜日名のマッピング
                    day_map = {
                        1: "月",
                        2: "火",
                        3: "水",
                        4: "木",
                        5: "金",
                        6: "土",
                        7: "日",
                    }
                    heatmap_df["day_of_week_str"] = heatmap_df["day_of_week"].map(
                        day_map
                    )

                    # ヒートマップの作成
                    fig_heatmap = px.density_heatmap(
                        heatmap_df,
                        x="hour_of_day",
                        y="day_of_week_str",
                        z="record_count",
                        nbinsx=24,
                        nbinsy=7,
                        title="アクティビティの発生時間帯",
                        labels={
                            "hour_of_day": "時間帯",
                            "day_of_week_str": "曜日",
                            "z": "件数",
                        },
                        category_orders={
                            "day_of_week_str": [
                                "月",
                                "火",
                                "水",
                                "木",
                                "金",
                                "土",
                                "日",
                            ]
                        },
                        color_continuous_scale="Blues",
                    )
                    fig_heatmap.update_layout(
                        xaxis_title="時間帯 (0-23時)",
                        yaxis_title="曜日",
                        xaxis=dict(tickmode="linear", dtick=2),
                    )
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                else:
                    st.warning("ヒートマップ表示用のデータがありません。")

            except Exception as e:
                st.error(f"ヒートマップの生成中にエラーが発生しました: {e}")

            # --- 取引金額の分布 ---
            st.header("取引金額の分布")
            try:
                dist_query = "SELECT EVENT_VALUE, is_fraud FROM filtered_data"
                dist_df = con.execute(dist_query).fetchdf()

                if not dist_df.empty:
                    fig_dist = px.histogram(
                        dist_df,
                        x="EVENT_VALUE",
                        color="is_fraud",
                        marginal="box",  # or "rug", "violin"
                        barmode="overlay",
                        title="取引金額の分布（不正利用別）",
                        labels={
                            "EVENT_VALUE": "取引金額 (EVENT_VALUE)",
                            "is_fraud": "不正利用フラグ",
                        },
                        color_discrete_map={True: "red", False: "royalblue"},
                        opacity=0.7,
                    )
                    fig_dist.update_layout(
                        xaxis_title="取引金額",
                        yaxis_title="件数",
                        legend_title_text="不正利用",
                    )
                    st.plotly_chart(fig_dist, use_container_width=True)
                else:
                    st.warning("取引金額分布の表示用データがありません。")
            except Exception as e:
                st.error(f"取引金額分布の生成中にエラーが発生しました: {e}")

            # --- 不正利用率の散布図 ---
            st.header("カテゴリ別 不正利用率")
            try:
                # 上位N件のカテゴリに絞る
                top_n_cats = top_by_count.head(top_n)[agg_col].to_list()

                if top_n_cats:
                    # カテゴリリストをSQLのIN句で使えるようにフォーマット
                    cats_in_clause = ", ".join([f"'{cat}'" for cat in top_n_cats])

                    fraud_rate_query = f"""
                        SELECT
                            {agg_col},
                            COUNT(*) AS total_count,
                            COUNT(*) FILTER (WHERE is_fraud = true) AS fraud_count,
                            (CAST(fraud_count AS DOUBLE) / total_count) AS fraud_rate
                        FROM filtered_data
                        WHERE {agg_col} IN ({cats_in_clause})
                        GROUP BY {agg_col}
                    """
                    fraud_rate_df = con.execute(fraud_rate_query).fetchdf()

                    if not fraud_rate_df.empty:
                        fig_scatter = px.scatter(
                            fraud_rate_df,
                            x="total_count",
                            y="fraud_rate",
                            size="total_count",
                            color=agg_col,
                            hover_name=agg_col,
                            title=f"`{agg_col}`別 取引件数 vs 不正利用率 (上位{top_n})",
                            labels={
                                "total_count": "取引件数",
                                "fraud_rate": "不正利用率",
                                "color": "カテゴリ",
                            },
                            size_max=60,
                        )
                        fig_scatter.update_layout(
                            xaxis_title="総取引件数",
                            yaxis_title="不正利用率",
                            yaxis=dict(tickformat=".2%"),  # Y軸をパーセント表示に
                        )
                        st.plotly_chart(fig_scatter, use_container_width=True)
                    else:
                        st.warning("散布図の表示用データがありません。")
                else:
                    st.warning("分析対象のカテゴリがありません。")
            except Exception as e:
                st.error(f"不正利用率散布図の生成中にエラーが発生しました: {e}")

    else:
        st.info("サイドバーで集計する列を選択してください。")
else:
    st.info("サイドバーで表示するデータソースを選択してください。")
