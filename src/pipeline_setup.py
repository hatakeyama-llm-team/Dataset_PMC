import argparse
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, WorkerOptions

def setup_pipeline_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--location', default="us-east1")
    parser.add_argument('--batch_name', default=None, type=str, help="Batch name, e.g., PMC000xxxxxx")
    parser.add_argument('--gcp_project_id', default="geniac-416410", type=str)
    parser.add_argument('--credidental_path', default="sec/geniac-416410-5bded920e947.json", type=str)
    return parser.parse_known_args(argv)

def configure_pipeline_options(known_args, pipeline_args, batch_name):
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.region = known_args.location
    google_cloud_options.project = known_args.gcp_project_id
    google_cloud_options.job_name = f"genaic-dataflow-pmc-{batch_name.lower()}-{known_args.location}"
    google_cloud_options.staging_location = f"gs://geniac-pmc/binaries"
    google_cloud_options.temp_location = f"gs://geniac-pmc/temp"
    options.view_as(WorkerOptions).autoscaling_algorithm = "THROUGHPUT_BASED"
    return options
