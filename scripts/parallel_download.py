import os
from concurrent.futures import ThreadPoolExecutor


def download_shard(shard_id):
    os.system(f"gsutil cp gs://ai2-jackh-mmc4-core-access/images_core/shard_{shard_id}_images_v3.tar /home/v-boli7/azure_storage/data/mmc4/core_images/shard_{shard_id}_images_v3.tar")

shard_ids = list(range(4188, 23099))

with ThreadPoolExecutor(max_workers=4) as executor:    
    executor.map(download_shard, shard_ids)
