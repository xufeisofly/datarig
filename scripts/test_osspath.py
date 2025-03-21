# -*- coding: utf-8 -*-
import os

from baselines.oss import oss
from baselines.core.file_utils import read_jsonl


if __name__ == '__main__':
    file_path = "oss://si002558te8h/dclm/output/tianwei_0319/DCLM_sub_by_keywords/processed_data/s1=10_2025030815581036g0lsuxg0sw1_R2_1_1_16_0-0_TableSink1-0-.tsv_processed.tsv"

    read_jsonl(file_path)
