# -*- coding: utf-8 -*-
import time
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl, get_file_size

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def get_oss_dir_size(dataset_path):
    bucket_name, prefix = oss.split_file_path(dataset_path)
    bucket = oss.Bucket(bucket_name)
    total_size = 0
    for obj in oss.get_all_objects_iter(bucket, prefix):
        total_size += obj.size
    return total_size


def main():
    dir_path = "oss://train1/basemodel-subjet-data/r2/dclm/subject=Accounting/"
    dir_path = "oss://si002558te8h/dclm/output/r2_formal/dclm/subject=Accounting/processed_data/"
    dir_path = 'oss://si002558te8h/dclm/output/r2_formal_deduped/dclm/subject=Accounting/'
    dir_path = 'oss://train1/basemodel-subjet-data-processed/r2/dclm/subject=Accounting/'
    
    fineweb_task_file = "oss://si002558te8h/dclm/dclm_dedup_tasks.jsonl"
    write_jsonl(list(read_jsonl(fineweb_task_file)), "./dclm_dedup_tasks.jsonl")

    local_task_file = "./dclm_dedup_tasks.jsonl"
    write_jsonl(list(read_jsonl(local_task_file)), fineweb_task_file)
    
    bucket_name, path = oss.split_file_path(dir_path)
    bucket = oss.Bucket(bucket_name)    
    keys = oss.get_sub_files(bucket, path)

    dir_path = "oss://train1/basemodel-subjet-data-processed/r2_deduped/dclm/subject=Accounting/"

    total = 0
    file_keys = []
    for key in keys[0:1]:
        key = oss.join_file_path(bucket_name, key)
        total += get_file_size(key) 
        file_keys.append(key)

    data = []
    for file_key in file_keys:
        data.extend(list(read_jsonl(file_key)))

    pure_data = []
    for file_key in file_keys:
        for line in read_jsonl(file_key):
            pure_data.append({"text": line['text']})        

    write_jsonl(data, './dedup_noano.tsv')
    write_jsonl(pure_data, './pure_account.tsv')

    import os
    dedup_total = 0
    dedup_file_keys = []
    for file_key in file_keys:
        filename = os.path.basename(file_key)
        dedup_file_path = "oss://train1/basemodel-subjet-data-processed/r2/dclm/subject=Accounting/" + filename
        dedup_file_keys.append(dedup_file_path)
        dedup_total += get_file_size(dedup_file_path)
        print(is_exists(dedup_file_path))

    data = []
    for dedup_file_key in dedup_file_keys:
        data.extend(list(read_jsonl(dedup_file_key)))

    dedup_pure_data = []
    for dedup_file_key in dedup_file_keys:
        for line in read_jsonl(dedup_file_key):
            dedup_pure_data.append({"text": line['text']})
        

    write_jsonl(data, './account_deduped.tsv.gz')
    write_jsonl(dedup_pure_data, './pure_account_deduped.tsv')

    
    oss_file_path = oss.join_file_path(bucket_name, file_path)
    data = list(read_jsonl(oss_file_path))
    write_jsonl(data, './origin.tsv.gz')

if __name__ == '__main__':
    main()
