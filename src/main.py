import asyncio
import json
import os
import logging
import tarfile
import pandas as pd
import time
from pipeline_setup import cli_args
from text_extraction import generate_record
from google.cloud import storage
import concurrent.futures
import xml.etree.ElementTree as ET

# Loggerの設定
logging.basicConfig(level=logging.DEBUG)


def combine_json_files(batch_name, total_files):
    json_dir = f"jsonl_files/{batch_name}/"
    output_path = f"jsonl_files/{batch_name}.jsonl"

    try:
        with open(output_path, "w") as outfile:
            json_files = [
                filename
                for filename in os.listdir(json_dir)
                if filename.endswith(".json")
            ]
            total_json_files = len(json_files)

            for i, filename in enumerate(json_files, start=1):
                filepath = os.path.join(json_dir, filename)
                with open(filepath, "r") as infile:
                    data = json.load(infile)
                    text = data["text"]
                    outfile.write(json.dumps({"text": text}) + "\n")

                progress_message = f"🔮 Combining JSONL files: {i}/{total_json_files} ({i/total_json_files*100:.2f}%)"
                print(f"\r{progress_message}", end="", flush=True)

        print()  # 改行を追加
        print(f"🍻 Successfully combined JSONL files into: {output_path}")

        # JSONLファイルの作成が完了したら、jsonl_files/{batch_name}直下のJSONファイルを全て削除
        for filename in os.listdir(json_dir):
            if filename.endswith(".json"):
                filepath = os.path.join(json_dir, filename)
                os.remove(filepath)

        print(f"🗑️  Deleted JSON files in: {json_dir}")
    except Exception as e:
        logging.error(
            f"💀 Failed to combine JSONL files for batch {batch_name}: {e}",
            exc_info=True,
        )


async def download_and_extract_tar_async(batch_name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sec/geniac-416410-5bded920e947.json"
    bucket_name = "geniac-pmc"
    tar_filename = f"oa_comm_xml.{batch_name}.baseline.2023-12-18.tar.gz"
    tar_path = f"original_files/{tar_filename}"
    destination_path = "xml_files"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(tar_path)

    try:
        os.makedirs(destination_path, exist_ok=True)
        blob.download_to_filename(tar_filename)
        print(f"📦 Successfully downloaded tar file: {tar_filename}")

        with tarfile.open(tar_filename, "r:gz") as tar:
            tar.extractall(path=destination_path)
        print(f"🗃️  Successfully extracted tar file to: {destination_path}")

        os.remove(tar_filename)
        print(f"🗑️  Deleted tar file: {tar_filename}")
    except Exception as e:
        logging.error(f"💀 Failed to download and extract tar file: {e}", exc_info=True)


async def run_batch_async(batch_name):
    await download_and_extract_tar_async(batch_name)

    def process_xml_file(filepath):
        xml_files_base = "xml_files"
        filepath = os.path.join(xml_files_base, filepath)
        try:
            record = None
            with open(filepath, "r") as file:
                xml_string = file.read()

            if not xml_string:
                logging.warning(f"File is empty: {filepath}")
                return None

            record = generate_record(xml_string)
            if record == "":
                logging.warning(f"No content extracted from XML: {filepath}")
            return {"text": record, "filepath": filepath} if record else None
        except Exception as e:
            logging.error(f"Failed to process file {filepath}: {e}", exc_info=True)
            return None

    def write_to_json(record, batch_name, total_files, current_file):
        filepath = record["filepath"]
        file_name = os.path.basename(filepath).replace(".xml", ".json")
        output_path = f"jsonl_files/{batch_name}/{file_name}"
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w") as json_file:
                json.dump(record, json_file)
            progress_message = f"🍀 Processing files: {current_file}/{total_files}"
            print(f"\r{progress_message}", end="", flush=True)
        except Exception as e:
            logging.error(
                f"💀 Failed to write record to JSON: {output_path}: {e}", exc_info=True
            )

    csv_path = f"target/{batch_name}.csv"

    try:
        df = pd.read_csv(csv_path, header=None, skiprows=1)
        xml_filenames = df.iloc[:, 0].dropna().tolist()
        total_files = len(xml_filenames)
        print(f"🌼 Number of valid XML filenames extracted: {total_files}")

        if not xml_filenames:
            logging.error("No XML files found for processing.")
            return
        
        max_workers = os.cpu_count()
        print(f"💿 {max_workers} threads")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_xml = {
                executor.submit(process_xml_file, filename): filename
                for filename in xml_filenames
            }
            for future in concurrent.futures.as_completed(future_to_xml):
                filename = future_to_xml[future]
                record = future.result()
                if record is not None:
                    write_to_json(
                        record,
                        batch_name,
                        total_files,
                        xml_filenames.index(filename) + 1,
                    )

        print()  # 改行を追加

        combine_json_files(batch_name, total_files)

    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_path}")
    except Exception as e:
        logging.error(f"Failed to process batch {batch_name}: {e}", exc_info=True)


def run_batch(batch_name):
    start_time = time.time()
    asyncio.run(run_batch_async(batch_name))
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"🕒 Batch {batch_name} completed in {execution_time:.2f} seconds")


def main():
    known_args, _ = cli_args()
    for batch in range(known_args.start_batch, known_args.end_batch + 1):
        batch_name = f"PMC{str(batch).zfill(3)}xxxxxx"
        print(f"🔥 Starting processing for {batch_name}")
        run_batch(batch_name)

if __name__ == "__main__":
    main()
