# Dataset_PMC

このプロジェクトは、ライフサイエンス系のデータセット構築を目的としています。Docker、Poetry、および Google Cloud Platform (GCP) の Dataflow を使用して、PMC OA Subsetからデータを処理し、分析用のデータセットを構築します。
devcontainerを使用しているため、VSCodeでの開発を推奨します。

## Setup
```sh
# 環境のPythonを3.9.15に合わせる
## conda
conda create --name 3.9.15 python=3.9.15 && conda activate 3.9.15

## asdf
asdf install python 3.9.15 && adsf local python 3.9.15
```

## Usage

```sh
# PMC000 - PMC010に対して、DirectRunnerで実行
poetry run python src/main.py --start_batch 0 --end_batch 10 --runner DirectRunner --machine_type m3-ultramem-64
```

## Dataset
ダウンロードしてくる全データのうち、ライセンスが「CC BY」または「CC0」のデータのみを `target/`  に抽出しています。
`target` ディレクトリのCSVファイルには「CC BY」または「CC0」のデータが記載されており、記載されているデータのみを対象として抽出しています。

## XML Convert
XMLから `<abstract></abstract>` と `<body></body>` のテキストを抽出して連結し、JSONファイルに変換しています。

| tag | process |
| --- | --- |
| p | ' ' + text |
| bold | ' ' + text |
| italic | text |
| sec | ' ' + text + \n |
| title | ' ' + text + \n |
| xref | x |
| fig | x |

- text ... テキストのみ
- text + \n ... テキスト + 改行
- x ... 除去

## JSONL(JSON Lines)

`text` のみからなるJSONを連結し、batchごとに長大なJSONLを生成します。

```jsonl
{
    "text": "Background Previous reports indicate altered ..."
},
{
    "text": "ackground Neurogenic Para-Osteo-Arthropathy ..."
}
```

1ファイルが50GBを超える場合は複数のファイルにチャンキングされます。

## Hugging Face

JSONLの生成完了後に自動でHugging Faceにアップロードします。