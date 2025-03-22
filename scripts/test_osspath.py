# -*- coding: utf-8 -*-
import os
import json

from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl

def finished_task_file(task_file_path):
    filename = os.path.basename(task_file_path)
    return task_file_path.replace(filename, "finished_"+filename)


if __name__ == '__main__':
    file_path = "oss://si002558te8h/dclm/process_tasks.jsonl"
    fin_file_path = finished_task_file(file_path)

    data = list(read_jsonl(file_path))
    write_jsonl(data, "./process_tasks.jsonl")

    if is_exists(fin_file_path):
        write_jsonl(list(read_jsonl(fin_file_path)), "./finished_process_tasks.jsonl")
