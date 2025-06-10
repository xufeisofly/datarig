# -*- coding: utf-8 -*-
import os
import concurrent.futures
import argparse

from baselines.oss import oss


def download_folder_from_oss(oss_dir, local_dir):
    bucket_name, dir_path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)
    sub_files = oss.get_sub_files(bucket, dir_path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for sub_file in sub_files:
            futures.append(executor.submit(oss.download_file_resumable, sub_file, local_dir, bucket))
            
        for future in concurrent.futures.as_completed(futures):
            future.result()            


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_dir", help="本地文件夹路径", type=str, default="")
    parser.add_argument("--oss_dir", help="OSS 文件夹路径", type=str, default="")
    args = parser.parse_args()
    download_folder_from_oss(args.oss_dir, args.local_dir)
    # oss_dir = "oss://si002558te8h/dclm/output/tokenized/r2/"
    # local_dir = "/root/dataprocess_nas/xf/tokenized/r2/"
