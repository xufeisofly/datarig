import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv
import sys
from typing import Dict
from baselines.core.constants import TERMINAL_PUNCTUATION
from baselines.mappers.filters.content_filters import high_quality_ratio, find_duplicates
import concurrent.futures
from baselines.mappers.enrichers.quality_prediction_enrichers_calc_fasttext import *

csv.field_size_limit(sys.maxsize)


def fineweb_quality(doc, short_line_length=30) -> Dict:
    text = doc['text']
    lines = text.split("\n")
    doc['line_num'] = len(lines)
    stop_chars = tuple(TERMINAL_PUNCTUATION)
    ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
    doc['line_punct_ratio'] = ratio
    ratio = sum(1 for line in lines if len(line) <= short_line_length) / len(lines)
    doc['short_line_ratio'] = ratio
    ratio = find_duplicates(lines)[1] / len(text.replace("\n", ""))
    doc['char_dup_ratio'] = ratio
    
    return doc


def high_quality_ratio_filter(doc) -> Dict:
    text = doc['text']
    lines = text.split("\n")
    doc['high_quality_ratio_10'] = high_quality_ratio(lines, model='fasttext', high_quality_min_line_num=10, language='eng')
    return doc


def fasttext_filter(doc) -> Dict:
    model = load_fasttext_model('fasttext_oh_eli5.bin')
    doc['score'] = classify_fasttext_hq_prob(model, doc['text'])
    return doc
    

            
if __name__ == '__main__':
    f = "oss://si002558te8h/dclm/output/c4_noclean_prepare_sampled/c4-train.00000-of-07168_processed.jsonl.zst"

    lines = []
    count = 0
    for line in read_jsonl(f):
        if count >= 30000:
            break
        lines.append(line)
        count += 1

    target_path = "oss://si002558te8h/dclm/origin/v2_raw_sample/c4-train.00000-of-07168_processed.jsonl.zst"
    write_jsonl(lines, target_path)

    target_path = "oss://si002558te8h/dclm/output/v2_fineweb_quality_thr/processed_data/c4-train.00000-of-07168_processed.jsonl.zst"

    docs = []
    old_docs = list(read_jsonl(target_path))

    def process(doc, i):
        doc = fineweb_quality(doc)
        # doc = high_quality_ratio_filter(doc)
        # doc = fasttext_filter(doc)
        print("=====", i)
        return doc
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=40) as executor:
        futures = []
        
        for i, doc in enumerate(old_docs):
            futures.append(executor.submit(process, doc, i))

        for future in concurrent.futures.as_completed(futures):
            docs.append(future.result())

        print(len(docs))

    stat_path = "oss://si002558te8h/dclm/output/v2_fineweb_quality_thr/c4-train.00000-of-07168_stat.jsonl.zst"
    write_jsonl(docs, stat_path)


    headers = ['text', 'line_num', 'line_punct_ratio', 'short_line_ratio', 'char_dup_ratio']
    output_csv = "./c4-train.00000-of-07168_stat.csv"
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for line in read_jsonl(stat_path):
            row = {
                'text': line['text'],
                'line_num': line['line_num'],
                'line_punct_ratio': line['line_punct_ratio'],
                'short_line_ratio': line['short_line_ratio'],
                'char_dup_ratio': line['char_dup_ratio'],
            }
            writer.writerow(row.values())    
        
