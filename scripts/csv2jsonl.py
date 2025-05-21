import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv
import sys
import os


# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main(file_path, output_folder):
    records = []
    csv.field_size_limit(sys.maxsize)
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) == 0:
                continue
            records.append({
                'text': row[1],
                'warc_record_id': row[0],
                'metadata': {},
            })

    records = records[1:]
    write_jsonl(records, os.path.join(output_folder, os.path.basename(file_path) + '.jsonl'))


def get_file_paths(folder_path):
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith('.csv')
    ]    

base_folder = '/Users/sofly/projects/dataprocess/data/standard/0521/'

if __name__ == '__main__':
    csv_files = get_file_paths(base_folder)
    output_folder = os.path.join(base_folder, 'output')
    for file_path in csv_files:
        main(file_path, output_folder=output_folder)
