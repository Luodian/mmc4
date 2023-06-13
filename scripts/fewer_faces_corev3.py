import os
import sys
import zipfile
import requests
from concurrent.futures import ThreadPoolExecutor

# Check if the destination folder argument is provided
if len(sys.argv) < 2:
    print("Please provide the destination folder as an argument.")
    print("Usage: python download_and_unzip.py /path/to/destination/folder")
    sys.exit(1)

# Set the download URL base
URL_BASE="https://storage.googleapis.com/ai2-jackh-mmc4-public/data_core_v1.1/docs_no_face_shard_"
# Set the folder where you want to save the unzipped files
DESTINATION_FOLDER = sys.argv[1]

# Create the destination folder if it doesn't exist
os.makedirs(DESTINATION_FOLDER, exist_ok=True)


# Function for processing shards
def process_shard(shard):
    print(f"Processing shard {shard}...")
    url = f"{URL_BASE}{shard}_v3.jsonl.zip"
    zip_file = os.path.join(DESTINATION_FOLDER, f"shard_{shard}.zip")

    # Check if the file exists and skip the download if it does
    if not os.path.isfile(zip_file):
        print(f"Downloading shard {shard} from {url}...")

        # Download the file (continue if the file is missing or there is an error)
        try:
            response = session.get(url, timeout=(5, 20))
            response.raise_for_status()

            with open(zip_file, "wb") as f:
                f.write(response.content)
        except requests.exceptions.RequestException as e:
            print(f"Error downloading shard {shard}, continuing... {e}")
            return

    # Unzip the file if it was downloaded successfully
    if os.path.isfile(zip_file):
        print(f"Unzipping {zip_file} to {DESTINATION_FOLDER}...")
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(DESTINATION_FOLDER)

        # Remove the zip file after unzipping
        os.remove(zip_file)


# Create a session object to maintain a connection pool
session = requests.Session()

# Download and unzip the files in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(process_shard, range(23099))

print("Download and unzip process completed.")
