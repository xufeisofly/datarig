# -*- coding: utf-8 -*-
import time
import sys
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists, write_jsonl, read_jsonl
from typing import List, Dict

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def get_limited_objects_iter(bucket, prefix, limit=100):
    result = bucket.list_objects(prefix=prefix, max_keys=limit)
    objects = result.object_list
    for o in objects:
        yield o


def get_sub_files_with_uncompressed_size(bucket, dir_path, dir_prefix, limit=100):
    if not dir_path.endswith('/'):
        dir_path += '/'
    bucket_name, dir_path = oss.split_file_path(dir_path)
    rets = list(get_limited_objects_iter(bucket, dir_path, limit=int(limit*1.5)))
    subfolders = oss.get_sub_folders(bucket, dir_path)

    def get_object_uncompressed_size(key):
        size = 0
        for line in read_jsonl(key):
            size += sys.getsizeof(line)
        return size
    
    files = [ret.key for ret in rets if not ret.key.endswith('/') and (dir_prefix is None or dir_prefix in ret.key)]

    total_size = 0
    def belong_to_folders(f, dirs):
        for folder in dirs:
            if folder in f:
                return True
        return False

    count = 0
    for f in files:
        if belong_to_folders(f, subfolders):
            continue
        f_path = oss.join_file_path(bucket_name, f)
        total_size += get_object_uncompressed_size(f_path)
        count += 1
        print(f"------{count} - {total_size}")
        if count > limit:
            break
    return total_size


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


def get_uncompressed_size_of_limited_files(bucket, dir_path, dir_prefix, limit=100):
    pass
    


def get_subject_data(bucket, path: str, label: str|None) -> List[Dict]:
    _, path = oss.split_file_path(path)
    subject_dirs = oss.get_sub_folders(bucket, path)
    data = []
    for subject_dir in subject_dirs:
        subject_dir = oss.join_file_path(bucket.bucket_name, subject_dir)
        subject_size = get_oss_dir_size(bucket, subject_dir, label) / 1024  # Convert to GB
        tag = "/".join(subject_dir.split("/")[-3:-1])  # Extract subject name (assumes structure is consistent)
        data.append({
            "subject_name": tag,
            "size_gb": subject_size,
        })
        logging.info(f"{label} subject: {subject_dir}, size: {subject_size}GB")
        time.sleep(0.5)  # Optional: only if rate-limiting is required
    return data

# 合并数据并返回最终的统计数据
def merge_stat_data(stat_data: List[Dict], processed_data: List[Dict], deduped_data: List[Dict]) -> List[Dict]:
    def get_item_from_data(item, data):
        return next((x for x in data if item['subject_name'] == x['subject_name']), None)

    merge_stat = []
    total_ori_size = 0
    total_processed_size = 0
    total_deduped_size = 0
    for item in stat_data:
        processed = get_item_from_data(item, processed_data)
        deduped = get_item_from_data(item, deduped_data)

        total_ori_size += item['size_gb']

        if processed:
            item['processed_gb'] = processed['size_gb']
            item['processed_rate'] = f"{(processed['size_gb'] / item['size_gb']) * 100}%"
            total_processed_size = processed['size_gb']

        if deduped:
            item['deduped_gb'] = deduped['size_gb']
            item['deduped_rate'] = f"{(deduped['size_gb'] / item['processed_gb']) * 100}%"
            total_deduped_size = deduped['size_gb']

        merge_stat.append(item)

    merge_stat.append({
        'total_origin_size_gb': total_ori_size,
        'total_processed_size_gb': total_processed_size,
        'total_deduped_size_gb': total_deduped_size,
        'total_processed_rate': f"{(total_processed_size / total_ori_size) * 100}%",
        'total_deduped_rate': f"{(total_deduped_size / total_processed_size) * 100}%",
    })
    
    return merge_stat

def main():
    # 定义OSS路径和桶
    base_dir = "oss://si002558te8h/dclm/output/output/"
    bucket_name, _ = oss.split_file_path(base_dir)
    bucket = oss.Bucket(bucket_name)

    # 获取stat数据
    stat_data = []
    dir_path = f"{base_dir}"
    data = get_subject_data(bucket, dir_path, None)
    stat_data.extend(data)

    write_jsonl(stat_data, "./sample_info.jsonl")

    total = 0
    for item in data:
        total += item['size_gb']
    print(total)

if __name__ == '__main__':
    main()    
