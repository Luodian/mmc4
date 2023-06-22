import os
import tarfile
from concurrent.futures import ThreadPoolExecutor


def download_shard(shard_id):
    print(f"Checking shard {shard_id}")
    file_path = f"/home/v-boli7/azure_storage/data/mmc4/core_images/shard_{shard_id}_images_v3.tar"
    if not os.path.exists(file_path):
        print(f"Downloading shard {shard_id}")
        os.system(f"gsutil cp gs://ai2-jackh-mmc4-core-access/images_core/shard_{shard_id}_images_v3.tar {file_path}")

    # Check if the tar file is valid or corrupted
    try:
        with tarfile.open(file_path, "r") as tar:
            tar.getnames()
    except tarfile.TarError:
        print(f"Tar file for shard {shard_id} is corrupted. Redownloading...")
        os.system(f"gsutil cp gs://ai2-jackh-mmc4-core-access/images_core/shard_{shard_id}_images_v3.tar {file_path}")

    if shard_id % 5000 == 0:
        os.system("rm -rf /home/v-boli7/tempcache/*")


shard_ids = list(range(4183, 23099))

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(download_shard, shard_ids)
