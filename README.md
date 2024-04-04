# Dataset_PMC

このプロジェクトは、ライフサイエンス系のデータセット構築を目的としています。Docker、Poetry、および Google Cloud Platform (GCP) の Dataflow を使用して、PMC OA Subsetからデータを処理し、分析用のデータセットを構築します。
devcontainerを使用しているため、VSCodeでの開発を推奨します。

## Usage

```sh
poetry run python src/pipeline_dataflow.py --location [location] --batch_name [batch_name] --gcp_project_id [gcp_project_id] --credidental_path [credidental_path]
# 引数はDefault値が設定されています。
```

## Dataset
ダウンロードしてくる全データのうち、ライセンスが「CC BY」または「CC0」のデータのみを抽出しています。
`target` ディレクトリのCSVファイルには「CC BY」または「CC0」のデータが記載されており、記載されているデータのみを対象としてparquetファイルに変換しています。