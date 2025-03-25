# -*- coding: utf-8 -*-
import time
import logging
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
    
    for obj in oss.get_all_objects_iter(bucket, prefix):
        total_size_mb += obj.size / 1024 / 1024

    subfolders = oss.get_sub_folders(bucket, prefix)
    if len(subfolders) == 0:
        return 0
    for subfolder in subfolders:
        total_size_mb += get_oss_dir_size(oss.join_file_path(bucket_name, subfolder))
    return total_size_mb

def main():
    bucket_name, path = oss.split_file_path(lock_file)
    bucket = oss.Bucket(bucket_name)
    previous_timestamp = None

    while True:
        try:
            meta = bucket.get_object_meta(path)
            logging.info(f"Lock file last modified: {meta.last_modified}")

            # 如果之前的时间存在且与当前相同，则删除锁文件
            if previous_timestamp is not None and meta.last_modified == previous_timestamp:
                logging.info("Lock file unchanged, deleting lock file.")
                bucket.delete_object(path)
            else:
                logging.info("Lock file updated or first check.")

            previous_timestamp = meta.last_modified

        except Exception as e:
            logging.error(f"Failed to get lock file: {e}")

        time.sleep(300)  # 每 5 分钟检查一次

if __name__ == '__main__':
    main()
