import asyncio
import json
import os
import logging
import resource
import tarfile
import pandas as pd
import time
from pipeline_setup import cli_args
from text_extraction import generate_record
from google.cloud import storage
import concurrent.futures
import aiofiles
from memory_profiler import profile

# Loggerの設定
logging.basicConfig(level=logging.DEBUG)


@profile
def combine_json_files(batch_name, total_files):
    print(f"\n🔗 Combining JSONL files for {batch_name}")
    json_dir = f"jsonl_files/{batch_name}/"
    # JSONLのチャンクサイズ
    chunk_size = 2000
    json_files = [
        filename for filename in os.listdir(json_dir) if filename.endswith(".json")
    ]
    total_json_files = len(json_files)
    current_chunk = 0
    processed_files = 0  # 処理済みファイル数のカウント

    for i in range(0, total_json_files, chunk_size):
        output_path = f"jsonl_files/{batch_name}_{str(current_chunk).zfill(2)}.jsonl"
        with open(output_path, "w") as outfile:
            for filename in json_files[i : i + chunk_size]:
                filepath = os.path.join(json_dir, filename)
                with open(filepath, "r") as infile:
                    data = json.load(infile)
                    text = data["text"]
                    outfile.write(json.dumps({"text": text}) + "\n")
                    processed_files += 1
                    progress_message = f"⛏️ Create JSONL chunk: {processed_files}/{total_json_files} ({processed_files/total_json_files*100:.2f}%)"
                    print(f"\r{progress_message}", end="", flush=True)

        print(f"\n💎 Successfully created JSONL chunk into: {output_path}")
        current_chunk += 1

    # JSONLファイルの作成が完了したら、jsonl_files/{batch_name}直下のJSONファイルを全て削除
    for filename in os.listdir(json_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(json_dir, filename)
            os.remove(filepath)

    print(f"\n🗑️  Deleted JSON files in: {json_dir}")
    pass


@profile
async def download_and_extract_tar_async(batch_name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sec/geniac-416410-5bded920e947.json"
    bucket_name = "geniac-pmc"
    tar_filename = f"oa_comm_xml.{batch_name}.baseline.2023-12-18.tar.gz"
    tar_path = f"original_files/{tar_filename}"
    destination_path = "xml_files"
    local_tar_path = os.path.join(tar_filename)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(tar_path)

    try:
        os.makedirs(destination_path, exist_ok=True)
        if not os.path.exists(local_tar_path):
            print(f"📦  Downloading tar file: {tar_filename}")
            blob.download_to_filename(local_tar_path)
        else:
            print(f"⏩  Using existing tar file: {tar_filename}")

        print(f"🗃️  Extracting downloaded tar file: {tar_filename}")
        with tarfile.open(local_tar_path, "r:gz") as tar:
            tar.extractall(path=destination_path)
        print(f"💡  Successfully extracted tar file to: {destination_path}")

        os.remove(local_tar_path)
        print(f"🗑️  Deleted tar file: {tar_filename}")
    except Exception as e:
        logging.error(f"💀 Failed to download and extract tar file: {e}", exc_info=True)
    pass


@profile
async def write_to_json(record, batch_name, total_files, current_file):
    filepath = record["filepath"]
    file_name = os.path.basename(filepath).replace(".xml", ".json")
    output_path = f"jsonl_files/{batch_name}/{file_name}"

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        async with aiofiles.open(output_path, "w") as json_file:
            await json_file.write(json.dumps(record))
        progress_message = f"🌞 Processing files: {current_file}/{total_files} ({current_file/total_files*100:.2f}%)"
        print(f"\r{progress_message}", end="", flush=True)
    except Exception as e:
        logging.error(
            f"💀 Failed to write record to JSON: {output_path}: {e}", exc_info=True
        )
    pass


@profile
async def process_xml_file(filepath, semaphore):
    try:
        async with semaphore:  # セマフォを関数の引数から受け取る
            async with aiofiles.open(filepath, "r") as file:
                xml_string = await file.read()

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
    pass


def get_max_open_files():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    print(f"📈 Soft limit: {soft}, Hard limit: {hard}")
    # セーフティマージンとして、ハードリミットの75%を使用
    return int(soft * 0.75)


async def run_batch_async(batch_name):
    loop = asyncio.get_running_loop()
    max_open_files = get_max_open_files()  # 最大ファイルオープン数を取得
    open_file_semaphore = asyncio.Semaphore(
        value=max_open_files, loop=loop
    )  # セマフォの作成

    await download_and_extract_tar_async(batch_name)
    csv_path = f"target/{batch_name}.csv"
    try:
        df = pd.read_csv(csv_path, header=None, skiprows=1)
        xml_filenames = df.iloc[:, 0].dropna().tolist()
        total_files = len(xml_filenames)

        tasks = [
            process_xml_file(os.path.join("xml_files", filename), open_file_semaphore)
            for filename in xml_filenames
        ]
        records = await asyncio.gather(*tasks)
        for index, record in enumerate(records):
            if record:
                await write_to_json(record, batch_name, total_files, index + 1)

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
