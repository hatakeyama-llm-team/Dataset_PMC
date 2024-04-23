import logging
from huggingface import upload_to_huggingface
from data_handler import download_and_extract_tar, combine_json_files  # Ensure combine_json_files is correctly imported
from xml_processing import process_batch

async def run_batch_async(batch_name):
    await download_and_extract_tar(batch_name)
    processed_files = await process_batch(batch_name)  # This returns a list of JSON file paths

    if processed_files and len(processed_files) > 0:
        jsonl_filename = f"{batch_name}.jsonl"
        combined_jsonl_path = await combine_json_files(batch_name, jsonl_filename)
        if combined_jsonl_path:  # Check if the path is not None
            await upload_to_huggingface(combined_jsonl_path)
        else:
            logging.error("Failed to combine JSON files into a JSONL file.")
