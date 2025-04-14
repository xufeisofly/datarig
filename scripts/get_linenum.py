# subject=Virology
# subject=DesignPracticeManagement

import time
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl  # 如果需要判断是否存在

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir_path", help="", type=str, default='')
    args = parser.parse_args()    
    oss_dir = args.dir_path
    
    bucket_name, path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)

    files = oss.get_sub_files(bucket, path)

    count = 0
    valid = 0
    for f in files:
        f = oss.join_file_path(bucket_name, f)
        for line in read_jsonl(f):
            count += 1
            if line.get('text', None):
                valid += 1
        
    print("========", count, "========", valid)
    
