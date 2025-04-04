# -*- coding: utf-8 -*-
import os
import argparse

from baselines.oss import oss


def upload_files_to_oss(from_dir, files, to_dir):
    """
    上传 from_dir 下的文件列表到 OSS 的 to_dir 目录
    """
    bucket_name, _ = oss.split_file_path(to_dir)
    bucket = oss.Bucket(bucket_name)
    for f in files:
        file_path = os.path.join(from_dir, f)
        oss.upload_file_to_oss(file_path, to_dir, bucket)


def upload_all(from_dir, to_dir):
    """
    递归上传 from_dir 中的所有文件和文件夹到 OSS，
    其中 to_dir 为 OSS 中的目标目录。
    仅递归一层，不考虑更深层次文件夹（当然这里实现的是递归所有层）
    """
    # 上传当前目录下的文件
    files = [f for f in os.listdir(from_dir) if os.path.isfile(os.path.join(from_dir, f))]
    try:
        upload_files_to_oss(from_dir, files, to_dir)
    except BaseException as e:
        print(e)
    
    # 获取当前目录下的所有子文件夹，递归上传
    dirs = [d for d in os.listdir(from_dir) if os.path.isdir(os.path.join(from_dir, d))]
    for d in dirs:
        new_from_dir = os.path.join(from_dir, d)
        new_to_dir = os.path.join(to_dir, d)
        upload_all(new_from_dir, new_to_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--local_dir", help="本地文件夹路径", type=str, default="")
    parser.add_argument("--oss_dir", help="OSS 文件夹路径", type=str, default="")
    args = parser.parse_args()    
    upload_all(args.local_dir, args.oss_dir)
