# Dataset_PMC

## Setup
### XMLを配置
以下のような形で XML を配置する。

```
├── input
│   └── PMC001xxxxxx
│       ├── PMC1997110.xml
│       ├── PMC1997112.xml
│   ├── PMC002xxxxxx
```

### パッケージのインストール
```shell
# asdfの場合
asdf install python 3.8.19
adsf local python 3.8.19
poetry install
```

#### Apple Silicon 由来のエラー

M1+の MacOS の場合は nmslib というパッケージのインストール時にエラーが出るため、以下のようにコンパイルオプションを指定してください。

```
CFLAGS="-mavx -DWARN(a)=(a)" poetry install
```

cf. https://github.com/nmslib/nmslib/issues/476

## 実行

```shell
poetry run python src/pipeline.py {first_arg} {second_arg}

# 例
poetry run python src/pipeline.py first100 csv
```

| first_arg | 説明                                             |
| --------- | ------------------------------------------------ |
| all       | 全て                                             |
| first100  | input フォルダの最初の 100 件の XML を対象とする |
| last100   | input フォルダの最後の 100 件の XML を対象とする |

| second_arg | 説明               |
| ---------- | ------------------ |
| csv        | CSV 形式で出力     |
| parquet    | parquet 形式で出力 |

## Pipeline
1. input/**/*.XMLをロード
1. XMLから `abstract` セクションと `body` のテキストを抽出
1. ScispaCyを用いて文単位に分割
1. ルールに従って不完全な文のフィルタリング
1. `未実装` 重複する文のフィルタリング

出力結果の例は `~/output/output_example.csv` です。

### 不完全な文のフィルタリングルール

#### 含まれるデータ

```
# 通常の完全な文
Although the miRNA is only 22 nucleotides long, its 5′ and 3′ ends seem to have distinct roles in binding.

# ".", "?", "!" で終わる文
(524 KB JPG).Click here for additional data file.
```

#### 除外されるデータ

```
# 3単語以下の文
(a)

# 文末が ".", "?", "!" 以外で終わっている文
```
