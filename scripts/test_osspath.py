# -*- coding: utf-8 -*-
import os
import json

from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl

def finished_task_file(task_file_path):
    filename = os.path.basename(task_file_path)
    return task_file_path.replace(filename, "finished_"+filename)


if __name__ == '__main__':
    file_path = "oss://si002558te8h/dclm/process_tasks.json"
    fin_file_path = finished_task_file(file_path)

    with open("./process_tasks.jsonl", "w") as localf:
        for line in read_jsonl(file_path):
            localf.write(line)
    
    # list(read_jsonl(file_path))
