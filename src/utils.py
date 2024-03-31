
from google.cloud import storage

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """GCS から特定の Blob をダウンロードする

    Parameters:
    bucket_name (str): ダウンロードするファイルがある GCS のバケット名
    source_blob_name (str): ダウンロードしたい Blob の名前
    destination_file_name (str): ダウンロードしたファイルを保存するローカルのファイルパス
    """
    # GCS クライアントを初期化
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Blob をダウンロード
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"{source_blob_name} を {destination_file_name} にダウンロードしました。")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"{source_file_name} to {destination_blob_name}")