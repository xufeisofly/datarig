# -*- coding: utf-8 -*-
import csv
import os
import logging
from baselines.oss import oss
from baselines.core.file_utils import is_exists, write_jsonl, read_jsonl
import concurrent.futures
import argparse

"""
对处理后数据的 stats.jsonl 进行汇总，使用方式
python3 scripts/gen_all_stats.py --base_dir 'oss://si002558te8h/dclm/output/r2_formal/' --output_csv './stats.csv' 
"""

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def get_step_name(name, i):
    return name + "_" + str(i)


def get_stats_of_file(oss_stat_file):
    file_stat = []
    
    for i, line in enumerate(read_jsonl(oss_stat_file)):
        if not line.get('pages_in', None):
            continue
        name = line.get('name', None)
        pages_in = line.get('pages_in', None)
        pages_out = line.get('pages_out', None)
        removed = line.get('removed', None)
        
        file_stat.append({
            'name': get_step_name(name, i),
            'pages_in': pages_in,
            'pages_out': pages_out,
            'removed': removed,
        })

    return file_stat


def generate_csv(subjects_stats, output_csv):
    # 写入头部字段
    headers = ['subject_dir']
    steps = subjects_stats[0]['steps']
    
    # 动态生成每个步骤的字段
    for step in steps:
        headers.append(f'{step["name"]}.pages_in')
        headers.append(f'{step["name"]}.pages_out')
        headers.append(f'{step["name"]}.removed')

    # 添加总和的字段
    headers += ['total_removed']
    
    # 打开文件并写入数据
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # 写入头部
        writer.writerow(headers)
        
        # 写入每个 subject_dir 的数据
        for subject in subjects_stats:
            subject_dir = subject['subject']
            steps_data = subject['steps']
            
            # 初始化每个字段的数据
            row = [subject_dir]
            total_removed = 0
            
            for step in steps_data:
                row.append(step['pages_in'])
                row.append(step['pages_out'])
                row.append(step['removed'])
                
                total_removed += step['removed']
            
            # 添加总和
            row.append(total_removed)
            
            # 写入数据行
            writer.writerow(row)

def process_subject(subject_folder, bucket_name, bucket):
    cur_step_stats = []
    stat_subject_folder = os.path.join(subject_folder, 'stats')
    stat_files = oss.get_sub_files(bucket, stat_subject_folder)
    
    for stat_file in stat_files:
        oss_stat_file = oss.join_file_path(bucket_name, stat_file)
        file_step_stats = get_stats_of_file(oss_stat_file)
        
        if len(cur_step_stats) == 0:
            cur_step_stats = file_step_stats
        else:
            for step_stat in file_step_stats:
                for cur_step_stat in cur_step_stats:
                    if cur_step_stat['name'] == step_stat['name']:
                        cur_step_stat['pages_in'] += step_stat['pages_in']
                        cur_step_stat['pages_out'] += step_stat['pages_out']
                        cur_step_stat['removed'] += step_stat['removed']
    
    return {
        'subject': subject_folder.split('/')[-2],
        'steps': cur_step_stats,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_dir", help="", type=str, default="oss://si002558te8h/dclm/output/r2_formal/")
    parser.add_argument("--output_csv", help="", type=str, default="./subjects_stats.csv")
    args = parser.parse_args()
    
    processed_base_dir = args.base_dir
    bucket_name, base_path = oss.split_file_path(processed_base_dir)
    bucket = oss.Bucket(bucket_name)        

    subjects_stats = []

    # 用线程池来处理每个 subject_dir
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for sub_dir in ["dclm"]:
            dir_path = f"{base_path}{sub_dir}/"
            subject_folders = oss.get_sub_folders(bucket, dir_path)
            
            for subject_folder in subject_folders:
                # 每个线程处理一个 subject_folder
                futures.append(executor.submit(process_subject, subject_folder, bucket_name, bucket))

        # 获取线程池中的所有任务的结果
        for future in concurrent.futures.as_completed(futures):
            subject_stats = future.result()
            subjects_stats.append(subject_stats)
            print(f"==== {subject_stats['subject']} done ====")
    
    output_csv = args.output_csv        
    generate_csv(subjects_stats, output_csv)
    

if __name__ == '__main__':
    main()

    
