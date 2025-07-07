# subject=Virology
# subject=DesignPracticeManagement
# subject=Toxicology
# subject=GastroenterologyHepatology
# subject=BehavioralScienceComparativePsychology
# subject=EmergencyCriticalCareMedicine
# subject=Accounting

from tqdm import tqdm
import csv
import os
import logging
import pandas as pd
from collections import defaultdict
import argparse
from baselines.oss import oss
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl  # 如果需要判断是否存在
from baselines.mappers.enrichers.language_id_enrichers import *
from baselines.mappers.filters.metadata_filters import *
from baselines.mappers.core_utils import split_paragraphs, split_sentences, split_words, split_words_of_page

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
    
    
def count_n_word_line(file_name):
    output_dict = defaultdict(list)
    words_n_output = defaultdict(list)
    file_path = oss.join_file_path('si002558te8h', file_name)
    file_lines = read_jsonl(file_path)
    for i, s_dict in enumerate(file_lines):
        if i > 100000:
            break
        text_str = s_dict.get('text', '')
        output_dict['text'].append(text_str)
        page_lines = text_str.split('\n')
        output_dict['total_line'].append(len(page_lines))
        words_1 = words_2 = words_3 = words_4 = words_5 = words_6 = 0
        words_n_output['text'].append(text_str)
        short_line_words_1 = []
        short_line_words_2 = []
        short_line_words_3 = []
        short_line_words_4 = []
        short_line_words_5 = []
        short_line_words_6 = []
        for line in page_lines:
            words = split_words(line, ignore_punctuation=True)      
            if len(words) == 1:              
                words_1 += 1
                short_line_words_1.append(line)
            elif len(words) == 2:
                words_2 += 1
                short_line_words_2.append(line)
            elif len(words) == 3:   
                words_3 += 1
                short_line_words_3.append(line)
            elif len(words) == 4:
                words_4 += 1
                short_line_words_4.append(line)
            elif len(words) == 5:
                words_5 += 1
                short_line_words_5.append(line)
            elif len(words) == 6:
                words_6 += 1
                short_line_words_6.append(line)
        output_dict['words_1'].append(words_1)
        output_dict['words_2'].append(words_2)
        output_dict['words_3'].append(words_3)  
        output_dict['words_4'].append(words_4)
        output_dict['words_5'].append(words_5)
        output_dict['words_6'].append(words_6)
        words_n_output['short_line_words_1'].append('\n'.join(short_line_words_1))
        words_n_output['short_line_words_2'].append('\n'.join(short_line_words_2))
        words_n_output['short_line_words_3'].append('\n'.join(short_line_words_3))
        words_n_output['short_line_words_4'].append('\n'.join(short_line_words_4))
        words_n_output['short_line_words_5'].append('\n'.join(short_line_words_5))
        words_n_output['short_line_words_6'].append('\n'.join(short_line_words_6))
        
    # 输出结果
    df1 = pd.DataFrame(words_n_output)
    df2 = pd.DataFrame(output_dict)
    df2.to_excel('/mnt/nas/zh_data/dclm_pool_head_en_n_word_lines.xlsx', index=False, engine='xlsxwriter')
    df1.to_excel('/mnt/nas/zh_data/dclm_pool_head_en_all_short_line_words_output.xlsx', index=False, engine='xlsxwriter')    
    
    
if __name__ == '__main__':
    oss_dir = "oss://si002558te8h/dclm/output/dclm_pool_en/s2/s1=1/s2=0/processed_data/"
    bucket_name, path = oss.split_file_path(oss_dir)
    bucket = oss.Bucket(bucket_name)
    files_list = oss.get_sub_files(bucket, path)
    # print(len(files_list))
    count_n_word_line(files_list[0])
    
    # subject_paths = oss.get_sub_folders(bucket, path)
    # # print(subject_paths)
    # for subject_path in tqdm(subject_paths):
    #     subject_name = subject_path.split('/')[-2] # 学科名
    #     if subject_name in finish_subject:
    #         continue
    #     r3_sample(subject_path, 'en')
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     futures = []
    #     for subject_path in subject_paths:
    #         subject_name = subject_path.split('/')[-2] # 学科名
    #         if subject_name in finish_subject:
    #             continue
    #         futures.append(executor.submit(r3_sample, subject_path, 'en'))

    #     for future in concurrent.futures.as_completed(futures):
    #         future.result()
