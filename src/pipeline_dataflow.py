import os
import argparse
import warnings
from google.cloud import storage
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions
import pandas as pd
from tqdm import tqdm

warnings.simplefilter(action='ignore', category=FutureWarning)

# TODO: tqdmでもうちょいいい感じの進捗表示にする
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
    """処理をスキップしたファイル名をCSVに追記する"""
    skipped_file_path = os.path.join(os.path.dirname(__file__), '..', 'target', 'skipped.csv')
    os.makedirs(os.path.dirname(skipped_file_path), exist_ok=True)
    
    with open(skipped_file_path, 'a') as csvfile:
        if os.stat(skipped_file_path).st_size == 0:
            csvfile.write("file_name\n")
        csvfile.write(f"{file_name}\n")

def analyze_and_upload(input_gs_path, output_gs_path, bucket_name, valid_files):
    from sentence_analysis import analyze_text_sentences
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

def download_blob_to_tmp(blob_path, bucket_name):
    tmp_root = "./tmp"
    file_name = os.path.basename(blob_path)
    tmp_path = os.path.join(tmp_root, file_name)
    download_blob(bucket_name, blob_path, tmp_path)
    return tmp_path

def setup_pipeline_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--location', default="us-east1")
    parser.add_argument('--batch_name', default="PMC000xxxxxx", type=str)
    parser.add_argument('--gcp_project_id', default="geniac-416410", type=str)
    parser.add_argument('--credidental_path', default="sec/geniac-416410-5bded920e947.json", type=str)
    return parser.parse_known_args(argv)

def configure_pipeline_options(known_args, pipeline_args):
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.region = known_args.location
    google_cloud_options.project = known_args.gcp_project_id
    google_cloud_options.job_name = f"genaic-dataflow-pmc-{known_args.batch_name.lower()}-{known_args.location}"
    google_cloud_options.staging_location = f"gs://geniac-pmc/binaries"
    google_cloud_options.temp_location = f"gs://geniac-pmc/temp"
    options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"
    return options

def read_valid_files(batch_name):
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'target', f'{batch_name}_oa_files.csv')
    df = pd.read_csv(csv_path)
    return set(df['file_name'].apply(lambda x: x.split('/')[-1]))

def generate_params_dict_list(batch_name, valid_files):
    bucket_name = "geniac-pmc"
    storage_client = storage.Client()
    blobs = list(storage_client.list_blobs(bucket_name, prefix=f"xml_files/{batch_name}/"))
    prefix_length = len(f"xml_files/{batch_name}/")

    return [{
        "bucket_name": bucket_name,
        "input_gs_path": blob.name,
        "output_gs_path": f"parquet_files/{batch_name}/{os.path.basename(blob.name).replace('.xml', '.parquet')}"
    } for blob in blobs if blob.name[prefix_length:] in valid_files]

def main(argv=None):
    known_args, pipeline_args = setup_pipeline_args(argv)
    options = configure_pipeline_options(known_args, pipeline_args)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = known_args.credidental_path
    valid_files = read_valid_files(known_args.batch_name)

    params_dict_list = generate_params_dict_list(known_args.batch_name, valid_files)
    
    with beam.Pipeline(options=options) as pipeline:
        (pipeline
         | 'Create file list' >> beam.Create(params_dict_list)
         | 'Process records' >> beam.Map(lambda params: analyze_and_upload(**params, valid_files=valid_files)))

if __name__ == "__main__":
    main()
