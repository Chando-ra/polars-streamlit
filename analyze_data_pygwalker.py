import pandas as pd
import polars as pl
import pygwalker as pygw
import streamlit as st

st.set_page_config(layout="wide")

st.title("PyGWalker with Polars")

# Parquetファイルを読み込む
df = pl.read_parquet("test_data_500.parquet")

# PyGWalkerコンポーネントを描画
pygw.walk(df, env="Streamlit", kernel_computation=True)
