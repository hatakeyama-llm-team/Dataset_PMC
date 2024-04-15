import argparse
import os
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions, WorkerOptions

def cli_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--location', default="us-east1")
    parser.add_argument('--start_batch', default=0, type=int, help="Start batch number")
    parser.add_argument('--end_batch', default=10, type=int, help="End batch number")
    parser.add_argument('--gcp_project_id', default="geniac-416410", type=str)
    parser.add_argument('--credidental_path', default="sec/geniac-416410-5bded920e947.json", type=str)
    return parser.parse_known_args(argv)

# Dataflow pipeline setup
def configure_pipeline_options(known_args, pipeline_args, batch_name):
    print(f"⌛️ Setting up pipeline for {batch_name}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = known_args.credidental_path
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.region = known_args.location
    google_cloud_options.project = known_args.gcp_project_id
    google_cloud_options.job_name = f"genaic-dataflow-pmc-iwata-{batch_name.lower()}-{known_args.location}"
    google_cloud_options.staging_location = f"gs://geniac-pmc/binaries"
    google_cloud_options.temp_location = f"gs://geniac-pmc/temp"
    options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"
    print(f"🚀 Pipeline setup complete for {batch_name}")
    return options
