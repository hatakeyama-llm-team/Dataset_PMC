import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
from google.cloud import storage
import apache_beam as beam
from data_analysis import analyze_and_upload
from pipeline_setup import setup_pipeline_args, configure_pipeline_options
import pandas as pd
from tqdm import tqdm

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
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = known_args.credidental_path

    batch_names = [known_args.batch_name] if known_args.batch_name else ["PMC0{:02d}xxxxxx".format(i) for i in range(11)]

    for batch_name in batch_names:
        valid_files = read_valid_files(batch_name)
        
        with tqdm(total=len(valid_files), desc=f"Processing batch: {batch_name}") as pbar:
            analyze_and_upload(batch_name, "geniac-pmc", valid_files, pbar)

        tqdm.write(f"Completed batch: {batch_name} with {len(valid_files)} files processed")

if __name__ == "__main__":
    main()
