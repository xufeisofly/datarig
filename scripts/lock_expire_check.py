# -*- coding: utf-8 -*-
import time
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists  # 如果需要判断是否存在

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

lock_file = "oss://si002558te8h/dclm/process_lockfile"

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
                # bucket.delete_object(path)
            else:
                logging.info("Lock file updated or first check.")

            previous_timestamp = meta.last_modified

        except Exception as e:
            logging.error(f"Failed to get lock file: {e}")

        time.sleep(10)  # 每 10 分钟检查一次

if __name__ == '__main__':
    main()
