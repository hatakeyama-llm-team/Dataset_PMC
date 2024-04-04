FROM python:3.9.15

# 必要なツールのインストール
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Poetryのバージョン指定でインストール
RUN pip install poetry==1.8.2

WORKDIR /app

# en_core_sci_lgをダウンロードして展開
RUN wget https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.4/en_core_sci_lg-0.5.4.tar.gz && \
    tar -zxvf en_core_sci_lg-0.5.4.tar.gz && \
    rm en_core_sci_lg-0.5.4.tar.gz

COPY pyproject.toml poetry.lock* /app/
COPY src/ /app
COPY input/ /app/input
COPY --from=apache/beam_python3.9_sdk:2.54.0 /opt/apache/beam /opt/apache/beam

# Poetryを使って依存関係をインストール
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi
