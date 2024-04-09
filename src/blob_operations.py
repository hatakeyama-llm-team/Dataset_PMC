import os
from google.cloud import storage
from tqdm import tqdm

def download_blob(bucket_name, source_blob_name, destination_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
    blob_size = blob.size
    if blob_size is not None:
        with open(destination_file_name, 'wb') as file_obj, tqdm(
                desc=f"Downloading {source_blob_name}",
                total=blob_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024) as pbar:
            storage_client.download_blob_to_file(blob, file_obj)
            pbar.update(blob_size)
    else:
        with open(destination_file_name, 'wb') as file_obj:
            storage_client.download_blob_to_file(blob, file_obj)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob_size = os.path.getsize(source_file_name)
    with open(source_file_name, 'rb') as file_obj, tqdm(
            desc=f"Uploading {destination_blob_name}",
            total=blob_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024) as pbar:
        blob.upload_from_file(file_obj)
        pbar.update(blob_size)

def append_to_skipped_files(file_name):
    skipped_file_path = os.path.join(os.path.dirname(__file__), '..', 'target', 'skipped.csv')
    os.makedirs(os.path.dirname(skipped_file_path), exist_ok=True)
    
    with open(skipped_file_path, 'a') as csvfile:
        if os.stat(skipped_file_path).st_size == 0:
            csvfile.write("file_name\n")
        csvfile.write(f"{file_name}\n")
