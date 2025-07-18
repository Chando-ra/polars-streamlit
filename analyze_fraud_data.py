from pathlib import Path

import pandas as pd
import plotly.express as px
import polars as pl
import streamlit as st
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score

st.set_page_config(layout="wide")
st.title("不正取引分析ダッシュボード")


# --- データ読み込み ---
@st.cache_data
def load_data(data_dir: Path) -> pl.DataFrame:
    """
    指定されたディレクトリからParquetファイルを読み込み、単一のPolars DataFrameに結合する。
    """
    if not data_dir.exists():
        st.error(f"データディレクトリが見つかりません: {data_dir}")
        return pl.DataFrame()

    parquet_files = list(data_dir.glob("**/*.parquet"))
    if not parquet_files:
        st.warning(f"データディレクトリ内にParquetファイルが見つかりません: {data_dir}")
        return pl.DataFrame()

    try:
        # scan_parquetを使い、全てのparquetファイルを効率的にスキャン
        lazy_df = pl.scan_parquet(parquet_files)
        df = lazy_df.collect()
        return df
    except Exception as e:
        st.error(f"データの読み込み中にエラーが発生しました: {e}")
        return pl.DataFrame()


# --- メイン処理 ---
prepared_data_dir = Path("prepared_data")
df = load_data(prepared_data_dir)

if df.is_empty():
    st.info("分析対象のデータがありません。まずはデータローダーを実行してください。")
else:
    st.header("分析設定")

    # --- サイドバー ---
    st.sidebar.header("設定")
    score_threshold = st.sidebar.slider(
        "不正と判断するSCOREの閾値",
        min_value=int(df["SCORE"].min()),
        max_value=int(df["SCORE"].max()),
        value=1500,
        step=50,
    )

    # --- メインの分析処理 ---
    st.header(f"SCORE閾値: {score_threshold} での分析結果")

    # 予測ラベルと正解ラベルを準備
    y_true = df["is_fraud"]
    y_pred = df["SCORE"] >= score_threshold

    # 混同行列の計算
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()

    # 評価指標の計算
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)

    # --- 結果の表示 ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("評価指標")
        st.metric("適合率 (Precision)", f"{precision:.2%}")
        st.metric("再現率 (Recall)", f"{recall:.2%}")
        st.metric("F1スコア", f"{f1:.2f}")

    with col2:
        st.subheader("混同行列 (Confusion Matrix)")
        cm_df = pd.DataFrame(
            cm,
            index=["実際: 正常", "実際: 不正"],
            columns=["予測: 正常", "予測: 不正"],
        )
        st.table(cm_df)
        st.markdown(
            f"""
            - **見逃し (False Negative):** {fn} 件
            - **誤検出 (False Positive):** {fp} 件
            """
        )

    # --- スコア分布の可視化 ---
    st.header("不正・正常取引のSCORE分布")
    log_y_axis = st.checkbox("Y軸を対数スケールで表示", value=True)
    fig_hist = px.histogram(
        df.to_pandas(),
        x="SCORE",
        color="is_fraud",
        barmode="overlay",
        nbins=100,
        title="SCORE分布（青: 正常, 赤: 不正）",
        labels={"is_fraud": "不正フラグ"},
        opacity=0.6,
        color_discrete_map={True: "red", False: "blue"},
        log_y=log_y_axis,
    )
    fig_hist.add_vline(
        x=score_threshold, line_width=3, line_dash="dash", line_color="green"
    )
    st.plotly_chart(fig_hist, use_container_width=True)

    # --- 誤分類データの詳細分析 ---
    st.header("誤分類データの詳細分析")

    # 誤分類データを抽出
    fn_df = df.filter((y_true == True) & (y_pred == False))
    fp_df = df.filter((y_true == False) & (y_pred == True))

    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"見逃し (FN) データ: {len(fn_df)}件")
        if not fn_df.is_empty():
            fig = px.histogram(
                fn_df.to_pandas(),
                x="score_level",
                title="見逃しデータのScore Level分布",
                category_orders={"score_level": ["low", "mid", "high"]},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("見逃しデータはありません。")

    with col2:
        st.subheader(f"誤検出 (FP) データ: {len(fp_df)}件")
        if not fp_df.is_empty():
            fig = px.histogram(
                fp_df.to_pandas(),
                x="score_level",
                title="誤検出データのScore Level分布",
                category_orders={"score_level": ["low", "mid", "high"]},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("誤検出データはありません。")

    st.header("読み込みデータサンプル")
    st.dataframe(df.head().to_pandas())

    st.header("基本統計量")
    st.dataframe(df.describe().to_pandas())
