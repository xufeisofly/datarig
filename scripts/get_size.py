# -*- coding: utf-8 -*-
import time
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists  # 如果需要判断是否存在

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


dir_path = "oss://si002558te8h/dclm/output/Aerospace/"
bucket_name, _ = oss.split_file_path(dir_path)
bucket = oss.Bucket(bucket_name)

def get_oss_dir_size(dir_path):
    _, prefix = oss.split_file_path(dir_path)
    total_size_mb = 0    

    if 'processed_data' in prefix:
        for obj in oss.get_all_objects_iter(bucket, prefix):
            total_size_mb += obj.size / 1024 / 1024

    logging.info(f"calculating dir: {dir_path} is {total_size_mb}")

    subfolders = oss.get_sub_folders(bucket, prefix)
    if len(subfolders) == 0:
        return total_size_mb
    for subfolder in subfolders:
        total_size_mb += get_oss_dir_size(oss.join_file_path(bucket_name, subfolder))
        time.sleep(0.1)
    return total_size_mb

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir_path", help="", type=str, default='')
    args = parser.parse_args()
    logging.info("result: {}".format(get_oss_dir_size(args.dir_path)))

if __name__ == '__main__':
    main()
