import os
check_path = "/home/v-boli7/azure_storage/data/mmc4/core_images"
for shard in range(0, 23000):
    shard_path = os.path.join(check_path, f"shard_{shard}_images_v3.tar_.gstmp")
    if os.path.exists(shard_path):
        print(shard_path)
