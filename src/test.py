
import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions


class ComputeWordLength(beam.DoFn):
    def __init__(self):
        pass

    def process(self, element):
        yield len(element)


def main():
    options = StandardOptions()  # 1. 実行オプションの設定
    bucket_name ='dataflow-test-ok'
    options.runner = "DataflowRunner"  # Runnerもここで決めている
    google_cloud_options = options.view_as(GoogleCloudOptions)    
    job_name = f"genaic-dataflow-pmc-test-east1"
    google_cloud_options.region = "us-east1"
    google_cloud_options.project = "keen-hangar-419010"
    google_cloud_options.job_name = job_name
    google_cloud_options.staging_location = f"gs://{bucket_name}/"
    google_cloud_options.temp_location = f"gs://{bucket_name}/"
    p = beam.Pipeline(options=options)  # 1. Pipelineの生成

    (
        p
        | "ReadFromText" >> beam.io.ReadFromText("./input.txt")  # 2. input.txtから最初のPCollectionを生成
        | "ComputeWordLength" >> beam.ParDo(ComputeWordLength())  # 3. PTransformの適用
        | "WriteToText" >> beam.io.WriteToText("./output", file_name_suffix=".txt", shard_name_template="")  # 4. output.txtへPCollectionの書き込み
    )

    p.run()  # 5. RunnerでPipelineを実行


if __name__ == "__main__":
    main()