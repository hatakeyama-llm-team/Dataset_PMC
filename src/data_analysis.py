import os
import pandas as pd
from blob_operations import append_to_skipped_files, download_blob, upload_blob
from sentence_analysis import analyze_text_sentences  # Ensure this module is correctly referenced

def download_blob_to_tmp(blob_path, bucket_name):
    tmp_root = "./tmp"
    file_name = os.path.basename(blob_path)
    tmp_path = os.path.join(tmp_root, file_name)
    download_blob(bucket_name, blob_path, tmp_path)
    return tmp_path

def analyze_and_upload(input_gs_path, output_gs_path, bucket_name, valid_files):
    file_name_only = os.path.basename(input_gs_path)

    if file_name_only not in valid_files:
        append_to_skipped_files(file_name_only)
        return
    tmp_path = download_blob_to_tmp(input_gs_path, bucket_name)
    sentences = analyze_text_sentences(tmp_path)
    sentences_data = pd.DataFrame([sentences])
    tmp_output_path = tmp_path.replace("xml", "parquet")
    sentences_data.to_parquet(tmp_output_path)
    upload_blob(bucket_name, tmp_output_path, output_gs_path)
