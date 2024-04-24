import os
import sys
from huggingface_hub import HfApi
from datasets import load_dataset
from dotenv import load_dotenv

# 環境変数を読み込む
load_dotenv()

# アクセストークンとリポジトリID
access_token = os.getenv("ACCESS_TOKEN")
repo_id = "hatakeyama-llm-team/PMC"

# ファイルリストをコマンドライン引数から取得
jsonl_files = sys.argv[1:]

# Hugging Face APIクライアントを作成
api = HfApi()

for filename in jsonl_files:
    print(f"Uploading {filename}...")

    # ファイルをHugging Face Hubにアップロード
    api.upload_file(
        token=access_token,
        repo_id=repo_id,
        path_in_repo=f"{filename}",
        path_or_fileobj=filename,
        repo_type='dataset'
    )

    print(f"Dataset {filename} uploaded successfully to {repo_id}")

print("All datasets uploaded successfully!")
