# -*- coding: utf-8 -*-
import time
import sys
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists, write_jsonl, read_jsonl, get_file_size
from typing import List, Dict
import random
import os

"""
验证一下 processed 的文件数量和 deduped 的文件数量是否一致
"""

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def get_sub_files(bucket, dir_path, dir_prefix):
    if not dir_path.endswith('/'):
        dir_path += '/'
    rets = list(oss.get_all_objects_iter(bucket, dir_path))
    subfolders = oss.get_sub_folders(bucket, dir_path)
    files = [ret.key for ret in rets if not ret.key.endswith('/') and (dir_prefix is None or dir_prefix in ret.key)]

    all_files = []
    def belong_to_folders(f, dirs):
        for folder in dirs:
            if folder in f:
                return True
        return False
    
    for f in files:
        if belong_to_folders(f, subfolders):
            continue
        all_files.append(f)
    return all_files


def get_subject_data(bucket, path: str) -> List[Dict]:
    _, path = oss.split_file_path(path)
    subject_dirs = oss.get_sub_folders(bucket, path)
    data = []
    
    for subject_dir in subject_dirs:
        files = oss.get_sub_files(bucket, subject_dir)
        data.extend(files)
        print(f"done: {subject_dir} {len(files)}")

    return data


def main():
    deduped_data = []
    deduped_base_dir = "oss://train1/basemodel-subjet-data-processed/r2/"
    bucket_name, _ = oss.split_file_path(deduped_base_dir)
    bucket = oss.Bucket(bucket_name)    
    # for sub_dir in ["dclm"]:
    #     path = f"{deduped_base_dir}{sub_dir}/"
    #     data = get_subject_data(bucket, path)
    #     deduped_data.extend(data)

    # random.shuffle(deduped_data)

    f = open("../shuffled.txt", mode="rb")

    deduped_data = f.readlines()
    output_dir = "oss://train1/basemodel-subjet-data-processed/output2/"
    total_size = 0
    threshold = 10 * 1024 * 1024 * 1024
    for file_path in deduped_data:
        file_path_str = file_path.decode('utf-8')
        file_path_str = file_path_str.replace('\n', '')
file_path = file_path_str.encode('utf-8')        
        file_path = file_path.replace(b'\n', b'')
        path = oss.join_file_path(bucket_name, file_path)
        s = get_file_size(path)
        total_size += s

        if total_size > threshold:
            break

        filename = os.path.basename(file_path)
        target_key = os.path.join(output_dir, filename)
        bucket.copy_object(bucket_name, file_path, target_key)
        # 读取文件大小

    f.close()


if __name__ == '__main__':
    main()
