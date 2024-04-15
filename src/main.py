import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import apache_beam as beam
from pipeline_setup import cli_args, configure_pipeline_options
from text_extraction import generate_record

# Loggerの設定
logging.basicConfig(level=logging.INFO)

def process_xml_file(element):
    filepath, _ = element
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            xml_string = file.read()

        if not xml_string:
            logging.warning(f"File is empty: {filepath}")
            return None

        record = generate_record(xml_string)
        print(f"😂 {record}")
        if record == "":
            logging.warning(f"No content extracted from XML: {filepath}")
            return None
        return {'content': record, 'filepath': filepath}
    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        return None
    except Exception as e:
        logging.error(f"Failed to process file {filepath}: {e}", exc_info=True)
        return None

def run_batch(pipeline_options, bucket_name, batch_name, credidental_path):
    with beam.Pipeline(options=pipeline_options) as p:
        csv_path = f"target/{batch_name}.csv"
        df = pd.read_csv(csv_path)
        xml_filenames = df['file_name'].tolist()
        
        xml_files = [(f"gs://{bucket_name}/{file_name}", os.path.join('/temp', batch_name)) for file_name in xml_filenames]

        if not xml_files:
            logging.error("No XML files found for processing.")
            return

        records = (
            p | 'Create File List' >> beam.Create(xml_files)
            | 'Process XML Files' >> beam.Map(process_xml_file)
            | 'Filter valid records' >> beam.Filter(lambda x: x is not None)
        )

        def get_output_path(record):
            filepath = record['filepath']
            file_name = os.path.basename(filepath).replace('.xml', '.parquet')
            return f"gs://{bucket_name}/parquet_files/{batch_name}/{file_name}"

        def write_to_parquet(record):
            output_path = get_output_path(record)
            logging.info(f"Attempting to write record to Parquet at {output_path}")
            
            try:
                df = pd.DataFrame([record['content']], columns=['content'])
                table = pa.Table.from_pandas(df, schema=pa.schema([pa.field('content', pa.string())]))
                pq.write_table(table, output_path)
                logging.info(f"Successfully written to Parquet: {output_path}")
            except Exception as e:
                logging.error(f"Failed to write record to Parquet: {output_path}: {e}", exc_info=True)

        (
            records
            | 'Map record to output path' >> beam.Map(write_to_parquet)
        )

def main():
    known_args, pipeline_args = cli_args()
    for batch in range(known_args.start_batch, known_args.end_batch + 1):
        batch_name = f"PMC{str(batch).zfill(3)}xxxxxx"
        logging.info(f"🔥 Starting processing for {batch_name}")
        try:
            pipeline_options = configure_pipeline_options(known_args, pipeline_args, batch_name)
            run_batch(pipeline_options, "geniac-pmc", batch_name, known_args.credidental_path)
        except Exception as e:
            logging.error(f"Failed to process batch {batch_name}: {e}", exc_info=True)

if __name__ == "__main__":
    main()
