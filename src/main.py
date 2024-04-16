import os
import logging

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from pipeline_setup import cli_args, configure_pipeline_options
from text_extraction import generate_record

# Loggerの設定
logging.basicConfig(level=logging.DEBUG)

def run_batch(pipeline_options, bucket_name, batch_name):
    with beam.Pipeline(options=pipeline_options) as p:
        import logging
        
        def process_xml_file(filepath, bucket_name):
            xml_files_base = f"gs://{bucket_name}"
            filepath = os.path.join(xml_files_base, "xml_files", filepath)
            
            logging.debug(f"🌟 Processing file: {filepath}")
            
            try:
                with FileSystems.open(filepath) as file:
                    xml_string = file.read().decode('utf-8')

                if not xml_string:
                    logging.warning(f"File is empty: {filepath}")
                    return None

                record = generate_record(xml_string)
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

        def get_output_path(record, batch_name, bucket_name):
            filepath = record['filepath']
            file_name = os.path.basename(filepath).replace('.xml', '.parquet')
            logging.debug(f"🌝 file_name: {file_name}")
            return f"gs://{bucket_name}/parquet_files/v2/{batch_name}/{file_name}"
        
        def write_to_parquet(record, batch_name, bucket_name):
            import apache_beam as beam
            import pyarrow as pa

            output_path = get_output_path(record, batch_name, bucket_name)
            logging.debug(f"🌹 Attempting to write record to Parquet at {output_path}")

            try:
                # レコードをPCollectionに変換
                record_pcollection = beam.Create([record])

                # スキーマを定義
                schema = pa.schema([
                    pa.field('content', pa.string())
                ])

                # WriteToParquet変換を適用
                _ = (
                    record_pcollection
                    | beam.io.parquetio.WriteToParquet(
                        file_path_prefix=f"gs://{bucket_name}/parquet_files/v2/{batch_name}/",
                        schema=schema,
                        file_name_suffix="",
                        num_shards=1
                    )
                )

                # Parquetファイルの存在を確認
                if FileSystems.exists(output_path):
                    logging.debug(f"🍀 Successfully written to Parquet: {output_path}")
                else:
                    logging.warning(f"🔥 Parquet file not found after writing: {output_path}")  
                          
            except Exception as e:
                logging.error(f"💀 Failed to write record to Parquet: {output_path}: {e}", exc_info=True)
                
        csv_path = f"target/{batch_name}.csv"
        
        # Read CSV file using ReadFromText
        csv_lines = (
            p | 'Read CSV File' >> beam.io.ReadFromText(csv_path)
        )
        
        # Log the count of lines read from CSV
        (csv_lines
            | 'Count CSV Lines' >> beam.combiners.Count.Globally()
            | 'Log CSV Line Count' >> beam.Map(lambda count: logging.debug(f"Number of lines read from CSV: {count}")))

        # Parse CSV lines
        parsed_lines = (
            csv_lines
            | 'Parse CSV Lines' >> beam.Map(lambda line: line.split(','))
        )

        # Extract XML filenames from the first column
        xml_filenames = (
            parsed_lines
            | 'Extract XML Filenames' >> beam.Map(lambda fields: fields[0] if len(fields) > 0 else None)
            | 'Filter Empty Filenames' >> beam.Filter(lambda x: x is not None)
        )

        # Log if no valid XML filenames were extracted
        (xml_filenames
            | 'Count Valid XML Filenames' >> beam.combiners.Count.Globally()
            | 'Log Valid XML Filename Count' >> beam.Map(lambda count: logging.debug(f"🌼 Number of valid XML filenames extracted: {count}")))

        if not xml_filenames:
            logging.error("No XML files found for processing.")
            return

        records = (
            xml_filenames
            | 'Process XML Files' >> beam.Map(lambda x: process_xml_file(x, bucket_name))
            | 'Filter valid records' >> beam.Filter(lambda x: x is not None)
        )
        
        (
            records
            | 'Write to Parquet' >> beam.Map(lambda record: write_to_parquet(record, batch_name, bucket_name))
        )

def main():
    known_args, pipeline_args = cli_args()
    for batch in range(known_args.start_batch, known_args.end_batch + 1):
        batch_name = f"PMC{str(batch).zfill(3)}xxxxxx"
        logging.debug(f"🔥 Starting processing for {batch_name}")
        try:
            pipeline_options = configure_pipeline_options(known_args, pipeline_args, batch_name)
            bucket_name = "geniac-pmc"
            run_batch(pipeline_options, bucket_name, batch_name)
        except Exception as e:
            logging.error(f"Failed to process batch {batch_name}: {e}", exc_info=True)

if __name__ == "__main__":
    main()