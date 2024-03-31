
"""
gcpの場合は, 他の依存関係を置かないようにする.
"""

import os
import argparse
from tqdm import tqdm
from pickle import NONE

import pandas as pd
from google.cloud import storage

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

def process_batch_record(params):
    """
    まずinputする
    """
    from sentence_analysis import process_files_in_parallel, analyze_text_sentences
    from utils import download_blob, upload_blob
    # params
    bucket_name = params["bucket_name"]
    input_gs_path = params["input_gs_path"]
    output_gs_path = params["output_gs_path"]

    # fixed params
    tmp_root = "./tmp"
    file_name = os.path.basename(input_gs_path)
    tmp_path = os.path.join(tmp_root, file_name)
    tmp_output_path = tmp_path.replace("xml", "parquet")
    os.makedirs(tmp_root, exist_ok=True)

    # load_data
    download_blob(bucket_name, input_gs_path, tmp_path)
    # preprocess
    sentences = analyze_text_sentences(tmp_path)
    sentences_data = pd.DataFrame(sentences)
    sentences_data.to_parquet(tmp_output_path) 
    # save
    upload_blob(bucket_name, tmp_output_path, output_gs_path)


def main(argv=None):
    # argparse options
    parser = argparse.ArgumentParser()
    parser.add_argument('--user_name',
                        default='namiuchi',
                        help='Dataflow User name')
    parser.add_argument('--location',
                        default="us-east1",
                        help='Conduct location')
    parser.add_argument('--batch_name',
                        default="PMC000xxxxxx",
                        type=str,
                        help='CommonCrawl Version Name')
    known_args, pipeline_args = parser.parse_known_args(argv)
    user_name = known_args.user_name
    batch_name = known_args.batch_name
    location = known_args.location
    JOB_NAME = f"genaic-dataflow-commoncraw-{user_name}-{batch_name}-{location}"
    # setup
    if user_name == "namiuchi":
        GCP_PROJECT_ID = "matsuo-hatakeyama-team"
        GCS_BUCKET_NAME = "gs://commoncrawl_pmc/tmp"
        BUCKET_NAME = "commoncrawl_pmc"
        LOCATION = known_args.location
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sec/matsuo-hatakeyama-team-16d8ca47bcae.json"
    elif user_name == "yamada":
        GCP_PROJECT_ID = "geniac-416410"
        GCS_BUCKET_NAME = "gs://geniac-dataflow-commoncrawl"
        BUCKET_NAME = "geniac-dataflow-commoncrawl"
        LOCATION = "us-east1"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sec/geniac-416410-5bded920e947.json"
    
    bucket_name = "commoncrawl_pmc"
    destination_blob_root = "preprocessed_files/PMC000xxxxxx"
    batch_name = "PMC000xxxxxx"
    destination_blob_path = f"{destination_blob_root}/{batch_name}"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sec/matsuo-hatakeyama-team-16d8ca47bcae.json"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket, prefix=destination_blob_path))
    output_paths = [
        os.path.join("parquet_files", 
                    batch_name,
                    os.path.basename(blob.name.replace(".xml", ".parquet")))
                     for blob in blobs
    ]
    input_paths = [
        str(blob.name) for blob in blobs
    ]

    params_dict_list = [
        {
            "bucket_name" : bucket_name,
            "input_gs_path" : input_paths[i],
            "output_gs_path" : output_paths[i]
        }
        for i in range(len(blobs))
    ]


    # options = PipelineOptions()
    options = PipelineOptions(pipeline_args)
    # google cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.region = LOCATION
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = f"{GCS_BUCKET_NAME}/binaries"
    google_cloud_options.temp_location = f"{GCS_BUCKET_NAME}/temp"
    # Worker Options
    options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"
    # options.view_as(StandardOptions).runner = "DirectRunner"
    # standard Options
    with beam.Pipeline(options=options) as pipeline:
        ocr_results = (pipeline
          |'create file list' >> beam.Create(params_dict_list)
          |"extract text" >> beam.Map(process_batch_record)
        )

main()