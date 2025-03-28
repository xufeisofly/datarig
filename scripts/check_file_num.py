# -*- coding: utf-8 -*-
import time
import sys
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists, write_jsonl, read_jsonl
from typing import List, Dict

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


def get_oss_dir_filenum(bucket, dir_path, dir_prefix):
    bucket_name, path = oss.split_file_path(dir_path)
    files = get_sub_files(bucket, path, dir_prefix)
    total_num = len(files)

    sub_dirs = oss.get_sub_folders(bucket, path)

    if len(sub_dirs) == 0:
        return total_num

    for sub_dir in sub_dirs:
        sub_dir = oss.join_file_path(bucket_name, sub_dir)
        total_num += get_oss_dir_filenum(bucket, sub_dir, dir_prefix)

    logging.info(f"calculating dir: {dir_path} filenum is {total_num}")
    return total_num


def get_subject_data(bucket, path: str, label: str|None) -> List[Dict]:
    _, path = oss.split_file_path(path)
    subject_dirs = oss.get_sub_folders(bucket, path)
    data = []
    for subject_dir in subject_dirs:
        subject_dir = oss.join_file_path(bucket.bucket_name, subject_dir)
        subject_filenum = get_oss_dir_filenum(bucket, subject_dir, label)
        tag = "/".join(subject_dir.split("/")[-3:-1])  # Extract subject name (assumes structure is consistent)
        data.append({
            "subject_name": tag,
            "filenum": subject_filenum,
        })
        logging.info(f"{label} subject: {subject_dir}, filenum: {subject_filenum}")
    return data

# 合并数据并返回最终的统计数据
def merge_stat_data(processed_data: List[Dict], deduped_data: List[Dict]) -> List[Dict]:
    def get_item_from_data(item, data):
        return next((x for x in data if item['subject_name'] == x['subject_name']), None)

    merge_stat = []
    total_ori_size = 0
    for item in processed_data:
        deduped = get_item_from_data(item, deduped_data)
        total_ori_size += item['filenum']

        if deduped:
            item['deduped_filenum'] = deduped['filenum']

        merge_stat.append(item)
    
    return merge_stat

def main():
    # 获取processed数据
    processed_data = []
    processed_base_dir = "oss://si002558te8h/dclm/output/r2_formal/"
    bucket_name, _ = oss.split_file_path(processed_base_dir)
    bucket = oss.Bucket(bucket_name)
    
    for sub_dir in ["dclm", "fineweb"]:
        dir_path = f"{processed_base_dir}{sub_dir}/"
        data = get_subject_data(bucket, dir_path, "processed_data")
        processed_data.extend(data)

    # 获取deduped数据
    deduped_data = []
    deduped_base_dir = "oss://train1/basemodel-subjet-data-processed/r2/"
    bucket_name, _ = oss.split_file_path(deduped_base_dir)
    bucket = oss.Bucket(bucket_name)    
    for sub_dir in ["dclm", "fineweb"]:
        dir_path = f"{deduped_base_dir}{sub_dir}/"
        data = get_subject_data(bucket, dir_path, None)
        deduped_data.extend(data)

    # 合并数据
    merged_data = merge_stat_data(processed_data, deduped_data)

    # 写入最终的 JSONL 文件
    write_jsonl(merged_data, "./filenum.jsonl")

    data = list(read_jsonl("./filenum.jsonl"))

    ret = []
    for item in data:
        if item['filenum'] != item.get('deduped_filenum', 0):
            ret.append(item)

    write_jsonl(ret, "./error_deduped_subject.jsonl")
    

if __name__ == '__main__':
    main()
