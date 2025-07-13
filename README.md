# データローダー・分析プロジェクト

このプロジェクトは、様々な形式の入力データ（TSV, TXT, tar.gz）を効率的に処理し、分析用のParquet形式に変換するためのパイプラインを提供します。

## 1. 環境構築 (初回のみ)

このプロジェクトを実行するには、Pythonとパッケージ管理ツール `uv` が必要です。

### uv のインストール

`uv` がインストールされていない場合は、以下のコマンドでインストールしてください。

**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```powershell
irm https://astral.sh/uv/install.ps1 | iex
```

### 仮想環境の作成と依存関係の同期

プロジェクトのルートディレクトリで、以下のコマンドを実行します。`uv.lock`ファイルをもとに、プロジェクトで定義されたバージョンのパッケージが正確にインストールされます。

```bash
# 仮想環境を作成
uv venv

# 仮想環境を有効化
# macOS / Linux
source .venv/bin/activate
# Windows (Command Prompt)
.venv\Scripts\activate.bat
# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# 依存関係をuv.lockファイルと同期
uv sync
```

## 2. 使い方

### Step 1: テストデータの準備 (任意)

`input_data`に分析したいデータがない場合、以下のコマンドでテストデータを生成できます。

```bash
python create_test_data.py
```
これにより、`input_data`ディレクトリに`test_data.tsv`が作成されます。

### Step 2: データの前処理

`input_data`ディレクトリに配置されたデータを処理し、`prepared_data`ディレクトリにParquet形式で保存します。

```bash
python data_loader.py
```

### Step 3: データの分析

前処理済みのデータを読み込んで分析を実行します。

```bash
python analyze_data.py
```

## プロジェクト概要

### ディレクトリ構造

- `input_data/`: 前処理対象のデータファイルを配置するディレクトリ。
- `prepared_data/`: 前処理済みのParquetファイルが保存されるディレクトリ。
- `*.py`: 各種処理を実行するためのPythonスクリプト群。

### 主要なスクリプト

- **`data_loader.py`**: 本プロジェクトの中核となるデータ処理パイプライン。`input_data`内のデータを再帰的に探索し、前処理を適用後、`prepared_data`にParquet形式で保存します。
- **`create_test_data.py`**: テスト用のダミーデータを生成します。
- **`analyze_data.py`**: 処理済みParquetデータを読み込み、分析を行うサンプルスクリプトです。
