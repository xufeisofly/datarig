# subject=Virology
# subject=DesignPracticeManagement
# subject=Toxicology
# subject=GastroenterologyHepatology
# subject=BehavioralScienceComparativePsychology
# subject=EmergencyCriticalCareMedicine
# subject=Accounting

from tqdm import tqdm
import os
import logging
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl  # 如果需要判断是否存在
from baselines.mappers.enrichers.language_id_enrichers import *
from baselines.mappers.filters.metadata_filters import *

import concurrent.futures

# 已经抽取完成的学科
# finish_subject = ['sample','AerospaceEngineering','Agriculture','Astronomy','Biology','Chemistry','ComputerScience','Engineering','Geography','MaterialsScience','Mathematics','Medicine','Physics']
finish_subject = ['sample']

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def r3_sample(subject_path, lang='en'):
    """_summary_

    Args:
        subject_path (_type_): 学科路径
        lang (str, optional): 语言类型，默认英语. Defaults to 'en'.
    """
    subject_name = subject_path.split('/')[-2] # 学科名
    # print(subject_name)
    
    num = 3000000 # 抽取数量
    # subject_path = subject_path + 'result/'
    files = oss.get_sub_files(bucket, subject_path)
    sample_data_list = []
    for file in files:
        file_path = oss.join_file_path('train1', file)
        file_lines = read_jsonl(file_path)
        for line in file_lines:
            if len(sample_data_list) >= num:
                break
            sample_data_list.append(line)
        if len(sample_data_list) >= num:
            break
    print(f"{subject_name} {lang} done {len(sample_data_list)}")
    # 写出路径
    output_folder = "/root/dataprocess_nas/zh/mixed/"    # 本地测试
    # output_folder = "oss://train1/basemodel-subjet-data-processed/hpc-processed/r3_mixed_dedupe_sample/"    
    write_jsonl(sample_data_list, os.path.join(output_folder, f"{subject_name}_{num}.jsonl"))
    print(f"success write jsonl {subject_name}")

if __name__ == '__main__':
    oss_dir = "oss://train1/basemodel-subjet-data-processed/hpc-processed/r3_mixed_dedupe/"
    bucket_name, path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)
    subject_paths = oss.get_sub_folders(bucket, path)
    # print(subject_paths)
    for subject_path in tqdm(subject_paths):
        subject_name = subject_path.split('/')[-2] # 学科名
        if subject_name in finish_subject:
            continue
        r3_sample(subject_path, 'en')
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     futures = []
    #     for subject_path in subject_paths:
    #         subject_name = subject_path.split('/')[-2] # 学科名
    #         if subject_name in finish_subject:
    #             continue
    #         futures.append(executor.submit(r3_sample, subject_path, 'en'))

    #     for future in concurrent.futures.as_completed(futures):
    #         future.result()
