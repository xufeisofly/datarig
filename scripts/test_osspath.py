# -*- coding: utf-8 -*-
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl


if __name__ == '__main__':
    file_path = "oss://si002558te8h/dclm/process_tasks.jsonl"
    fin_file_path = oss.finished_task_file(file_path)
    fail_file_path = oss.failed_task_file(file_path)

    data = list(read_jsonl(file_path))
    write_jsonl(data, "./process_tasks.jsonl")

    if is_exists(fin_file_path):
        write_jsonl(list(read_jsonl(fin_file_path)), "./finished_process_tasks.jsonl")

    if is_exists(fail_file_path):
        write_jsonl(list(read_jsonl(fail_file_path)), "./failed_process_tasks.jsonl")
