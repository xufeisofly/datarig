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
    task_file = "oss://si002558te8h/dclm/dclm_dedup_tasks.jsonl"
    write_jsonl(list(read_jsonl(task_file)), "./dclm_dedup_tasks.jsonl")

    task_file = "oss://si002558te8h/dclm/fineweb_dedup_tasks.jsonl"
    write_jsonl(list(read_jsonl(task_file)), "./fineweb_dedup_tasks.jsonl")    

    # local_task_file = "./dclm_dedup_tasks.jsonl"
    # write_jsonl(list(read_jsonl(local_task_file)), fineweb_task_file)

if __name__ == '__main__':
    while True:
        main()
        print("synced")
        time.sleep(10)
