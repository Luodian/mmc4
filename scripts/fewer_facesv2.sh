#!/bin/bash

# Check if the destination folder argument is provided
if [ $# -eq 0 ]; then
    echo "Please provide the destination folder as an argument."
    echo "Usage: ./download_and_unzip.sh /path/to/destination/folder"
    exit 1
fi

# Set the download URL base
URL_BASE="https://storage.googleapis.com/ai2-jackh-mmc4-public/data_v1.1/docs_no_face_shard_"

# Set the folder where you want to save the unzipped files
DESTINATION_FOLDER="$1"

# Create the destination folder if it doesn't exist
mkdir -p "$DESTINATION_FOLDER"

# Function for processing shards
process_shard() {
    SHARD=$1
    echo "Processing shard $SHARD..."
    URL="${URL_BASE}${SHARD}_v2.jsonl.zip"
    ZIP_FILE="${DESTINATION_FOLDER}/shard_${SHARD}.zip"

    # Check if the file exists and skip the download if it does
    if [ ! -f "$ZIP_FILE" ]; then
        echo "Downloading shard $SHARD from $URL..."

        # Download the file (continue if the file is missing or there is an error)
        curl -fsSL --retry 3 --retry-delay 5 --max-time 20 --continue-at - "$URL" -o "$ZIP_FILE" || echo "Error downloading shard $SHARD, continuing..."
    else
        echo "Shard $SHARD already downloaded, skipping..."
    fi

    # Unzip the file if it was downloaded successfully
    if [ -f "$ZIP_FILE" ]; then
        echo "Unzipping $ZIP_FILE to $DESTINATION_FOLDER..."
        yes | unzip -q "$ZIP_FILE" -d "$DESTINATION_FOLDER"

        # Remove the zip file after unzipping
        rm "$ZIP_FILE"
    fi
}

export -f process_shard
export DESTINATION_FOLDER
export URL_BASE

# Loop through the shard numbers and download and unzip the files in parallel
seq 0 23098 | parallel -j8 process_shard {}

echo "Download and unzip process completed."
