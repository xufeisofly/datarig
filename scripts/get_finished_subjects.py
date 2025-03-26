# -*- coding: utf-8 -*-
import json
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists, write_jsonl, read_jsonl

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main():
    dir_path = "oss://train1/basemodel-subjet-data/r2/dclm/"
    bucket_name, path = oss.split_file_path(dir_path)
    bucket = oss.Bucket(bucket_name)
    subject_dirs = oss.get_sub_folders(bucket, path)


    dir_path = "oss://train1/basemodel-subjet-data/r2/fineweb/"
    _, path = oss.split_file_path(dir_path) 
    subject_dirs += oss.get_sub_folders(bucket, path)
        

    finished = set()
    tasks = list(read_jsonl('./process_tasks.jsonl'))

    def in_tasks(subject_dir):
        for task in tasks:
            if subject_dir in task['shard_dir'] or subject_dir in task.get('original_shard_dir', ''):
                return True
        return False
    
    for subject_dir in subject_dirs:
        if in_tasks(subject_dir):
            continue
        finished.add(subject_dir)

    dir_path = "oss://train1/basemodel-subjet-data-processed/r2/dclm/"
    _, path = oss.split_file_path(dir_path) 
    deduped_dirs = oss.get_sub_folders(bucket, path)

    dir_path = "oss://train1/basemodel-subjet-data-processed/r2/fineweb/"
    _, path = oss.split_file_path(dir_path) 
    deduped_dirs += oss.get_sub_folders(bucket, path)


    def tag_in_deduped(tag):
        for deduped_dir in deduped_dirs:
            if tag in deduped_dir:
                return True
        return False

    diff = set()
    for fin_dir in list(finished):
        tag = fin_dir.split("/")[-3] + "/" + fin_dir.split("/")[-2]
        if tag_in_deduped(tag):
            continue
        diff.add(fin_dir)

    def join_bucket_name(paths):
        ret = []
        for path in paths:
            ret.append(oss.join_file_path(bucket_name, path))
        return ret
    

    data = {
        "processed": join_bucket_name(list(finished)),
        "deduped": join_bucket_name(list(deduped_dirs)),
        "diff": join_bucket_name(list(diff)), 
    }

    with open('./progress.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    


if __name__ == '__main__':
    main()
