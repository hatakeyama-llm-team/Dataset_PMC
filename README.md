# Dataset_PMC

このプロジェクトは、ライフサイエンス系のデータセット構築を目的としています。Docker、Poetry、および Google Cloud Platform (GCP) の Dataflow を使用して、PMC OA Subsetからデータを処理し、分析用のデータセットを構築します。
devcontainerを使用しているため、VSCodeでの開発を推奨します。

## Usage

```sh
docker build -t gcr.io/geniac-416410/pmc:latest .
# 認証がまだの場合
gcloud auth activate-service-account --key-file=./sec/geniac-416410-5bded920e947.json
docker push gcr.io/geniac-416410/pmc:latest
```

```sh
# オプション
poetry run python src/main.py --start_batch 0 --end_batch 10 \
    --machine_type e2-standard-4 \
    --runner DataflowRunner \
    --location us-east1 \
    --sdk_container_image gcr.io/geniac-416410/pmc:latest \
    --experiments use_runner_v2
```

## Dataset
ダウンロードしてくる全データのうち、ライセンスが「CC BY」または「CC0」のデータのみを `target/`  に抽出しています。
`target` ディレクトリのCSVファイルには「CC BY」または「CC0」のデータが記載されており、記載されているデータのみを対象としてparquetファイルに変換しています。

## XML Convert
XMLから `<abstract></abstract>` と `<body></body>` のテキストを抽出し、Parquetファイルに変換しています。

| tag | process |
| --- | --- |
| p | text |
| bold | text |
| italic | text |
| sec | text + \n |
| title | text + \n |
| xref | x |
| fig | x |

- text ... テキストのみ
- text + \n ... テキスト + 改行
- x ... 除去

## Parquet
| column | type |
| --- | --- |
| content | string |

- content ... XMLのAbstract+Bodyを結合したテキスト