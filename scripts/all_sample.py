import os
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl  # 如果需要判断是否存在
from baselines.mappers.enrichers.language_id_enrichers import *
from baselines.mappers.filters.metadata_filters import *

import concurrent.futures


# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main():
    oss_dir = "oss://si002558te8h/dataset/c4/raw/C4/c4/en.noclean/"
    bucket_name, path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)

    def process(f, target_key, source_bucket_name):
        filename = os.path.basename(f)
        target_key = os.path.join(target_key, filename)
        bucket.copy_object(source_bucket_name, f, target_key)
        return target_key    

    files = oss.get_sub_files(bucket, path)

    sampled_files = []
    for idx, f in enumerate(files):
        if idx % 10 == 0:
            sampled_files.append(f)

    target_dir = "oss://si002558te8h/dataset/c4/raw/C4/c4/en.noclean_sampled_10percent/"
    _, target_dir = oss.split_file_path(target_dir)
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for f in sampled_files:
            futures.append(executor.submit(process, f, target_dir, bucket_name))

        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == '__main__':
    main()
