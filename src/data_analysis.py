import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from blob_operations import download_blob_as_string, upload_blob
from sentence_analysis import analyze_text_sentences

def analyze_and_upload(batch_name, bucket_name, valid_files, pbar):
    if batch_name is None or bucket_name is None:
        raise ValueError("batch_name and bucket_name must not be None")

    tmp_root = "./tmp"
    os.makedirs(tmp_root, exist_ok=True)
    table_list = []
    error_log_path = os.path.join(tmp_root, "errors.log")

    for file_name in valid_files:
        if file_name is None:
            continue  # Skip if file_name is None

        blob_path = f"xml_files/{batch_name}/{file_name}"
        try:
            xml_string = download_blob_as_string(bucket_name, blob_path)
            text = analyze_text_sentences(xml_string)
            if not text:
                continue
            
            # DEBUG: tmpディレクトリにテキストファイルを保存
            # with open(f"./tmp/{str(file_name).replace('.xml', '.txt')}", "w") as text_file:
            #     text_file.write(text)
            df = pd.DataFrame([text], columns=["Text"])
            table = pa.Table.from_pandas(df)
            table_list.append(table)
            
        except Exception as e:
            error_msg = f"{str(e)} | batch_name: {batch_name}, bucket_name: {bucket_name}, file_name: {file_name}"
            with open(error_log_path, "a") as error_log:
                error_log.write(f"Error processing {blob_path}: {error_msg}\n")
            continue
        finally:
            pbar.update(1)

    if table_list:
        combined_table = pa.concat_tables(table_list)
        tmp_output_path = os.path.join(tmp_root, f"{batch_name}.parquet")
        pq.write_table(combined_table, tmp_output_path)

        upload_blob(bucket_name, tmp_output_path, f"parquet_files/{batch_name}/{batch_name}.parquet")
