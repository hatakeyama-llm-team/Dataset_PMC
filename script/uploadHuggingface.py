import os
import time
from huggingface_hub import HfApi
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get access token and repository ID from environment
access_token = os.getenv("HF_ACCESS_TOKEN")
repo_id = os.getenv("HF_REPO_ID")

# Create a Hugging Face API client
api = HfApi()

# Directory containing the JSONL files
directory = "jsonl_files/cluster"
file_list = [f for f in os.listdir(directory) if f.endswith('.jsonl')]
progress_bar = tqdm(file_list, desc="Uploading files")

# Upload each file to HuggingFace
for filename in progress_bar:
    file_path = os.path.join(directory, filename)
    # Specify the path in the repository to include the 'cluster' directory
    path_in_repo = f"cluster/{filename}"

    # Upload the file
    api.upload_file(
        token=access_token,
        repo_id=repo_id,
        path_in_repo=path_in_repo,
        path_or_fileobj=file_path,
        repo_type='dataset'
    )

    # Update the description and post-fix of the progress bar to reflect the upload status
    progress_bar.set_description(f"Uploading {filename}")
    progress_bar.set_postfix(file=f"Uploaded {filename}")

    # Sleep for 0.1 seconds to avoid hitting API rate limits
    time.sleep(0.1)

print("All files have been uploaded successfully.")
