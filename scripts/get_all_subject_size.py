# -*- coding: utf-8 -*-
import time
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, write_jsonl  # 如果需要判断是否存在

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def get_sub_files_with_size(bucket, dir_path, dir_prefix):
    if not dir_path.endswith('/'):
        dir_path += '/'
    rets = list(oss.get_all_objects_iter(bucket, dir_path))
    subfolders = oss.get_sub_folders(bucket, dir_path)
    files = [(ret.key, ret.size) for ret in rets if not ret.key.endswith('/') and (dir_prefix is None or dir_prefix in ret.key)]

    all_files = []
    def belong_to_folders(f, dirs):
        for folder in dirs:
            if folder in f:
                return True
        return False
    
    for f in files:
        if belong_to_folders(f[0], subfolders):
            continue
        all_files.append(f)
    return all_files

def get_oss_dir_size(bucket, dir_path, dir_prefix):
    bucket_name, path = oss.split_file_path(dir_path)
    files_with_size = get_sub_files_with_size(bucket, path, dir_prefix)
    total_size_mb = 0
    if len(files_with_size) > 0:
        for fs in files_with_size:
            total_size_mb += fs[1] / 1024 / 1024

    sub_dirs = oss.get_sub_folders(bucket, path)

    if len(sub_dirs) == 0:
        return total_size_mb

    for sub_dir in sub_dirs:
        sub_dir = oss.join_file_path(bucket_name, sub_dir)
        total_size_mb += get_oss_dir_size(bucket, sub_dir, dir_prefix)

    logging.info(f"calculating dir: {dir_path} is {total_size_mb} MB")
    return total_size_mb

def main():
    dir_path = "oss://train1/basemodel-subjet-data/r2/dclm/"
    bucket_name, path = oss.split_file_path(dir_path)
    bucket = oss.Bucket(bucket_name)
    subject_dirs = oss.get_sub_folders(bucket, path)

    stat_data = []
    for subject_dir in subject_dirs:
        subject_dir = oss.join_file_path(bucket_name, subject_dir)
        subject_size = get_oss_dir_size(bucket, subject_dir, None) / 1024
        stat_data.append({
            "subject_dir": subject_dir,
            "size_gb": subject_size,
        })
        logging.info(f"subject: {subject_dir}, size: {subject_size}GB")
        time.sleep(2)

    dir_path = "oss://train1/basemodel-subjet-data/r2/fineweb/"
    _, path = oss.split_file_path(dir_path) 
    subject_dirs = oss.get_sub_folders(bucket, path)

    for subject_dir in subject_dirs:
        subject_dir = oss.join_file_path(bucket_name, subject_dir)
        subject_size = get_oss_dir_size(bucket, subject_dir, None) / 1024
        stat_data.append({
            "subject_dir": subject_dir,
            "size_gb": subject_size,
        })
        logging.info(f"subject: {subject_dir}, size: {subject_size}GB")
        time.sleep(2)        

    write_jsonl(stat_data, "./oridata_size.jsonl")
    

if __name__ == '__main__':
    main()
