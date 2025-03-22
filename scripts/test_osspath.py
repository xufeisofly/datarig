# -*- coding: utf-8 -*-
import os

from baselines.oss import oss
from baselines.core.file_utils import read_jsonl


if __name__ == '__main__':
    file_path = "oss://si002558te8h/dclm/process_tasks.jsonle"

    list(read_jsonl(file_path))
