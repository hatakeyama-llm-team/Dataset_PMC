import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
from google.cloud import storage
import apache_beam as beam
from data_analysis import analyze_and_upload
from pipeline_setup import setup_pipeline_args, configure_pipeline_options
import pandas as pd

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
