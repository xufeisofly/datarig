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

def get_oss_dir_size(dir_path, dir_prefix):
    _, prefix = oss.split_file_path(dir_path)
    total_size_mb = 0    

    if dir_prefix is None or dir_prefix in prefix:
        for obj in oss.get_all_objects_iter(bucket, prefix):
            if obj.key.endswith('/'):
                continue
            total_size_mb += obj.size / 1024 / 1024

    logging.info(f"calculating dir: {dir_path} is {total_size_mb}")
    return total_size_mb

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir_path", help="", type=str, default='')
    parser.add_argument("--dir_prefix", help="", type=str, default=None)
    args = parser.parse_args()
    logging.info("result: {}GB".format(get_oss_dir_size(args.dir_path, args.dir_prefix) / 1024))

if __name__ == '__main__':
    main()
