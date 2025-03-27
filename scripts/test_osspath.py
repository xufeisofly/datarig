# -*- coding: utf-8 -*-
import time
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def one():
    file_path = "oss://si002558te8h/dclm/process_tasks.jsonl"
    fin_file_path = oss.finished_task_file(file_path)

    data = list(read_jsonl(file_path))
    write_jsonl(data, "./process_tasks.jsonl")

    if is_exists(fin_file_path):
        write_jsonl(list(read_jsonl(fin_file_path)), "./finished_process_tasks.jsonl")

    tmp_folder = "oss://si002558te8h/dclm/temp_dir_300/"
    bucket_name, path = oss.split_file_path(tmp_folder)
    bucket = oss.Bucket(bucket_name)
    files = oss.get_sub_files(bucket, path)
    logging.info("temp files: {}".format(len(files)))

    fin_file = "./finished_process_tasks.jsonl"
    fins = list(read_jsonl(fin_file))
    ori_files = [f for f in fins if f['is_temp'] is False]
    logging.info("processed origin tasks: {}".format(len(ori_files)))    

if __name__ == '__main__':
    while True:
        one()
        time.sleep(30)

    # f = "oss://si002558te8h/dclm/temp_dir_300/p7_20250322082825803g1fusa1ty1a5_M1_1_4_0-0_TableSink1-0-.tsv.gz"
    # is_exists(f)
