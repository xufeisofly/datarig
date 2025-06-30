import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv
import sys
from typing import Dict
from baselines.core.constants import TERMINAL_PUNCTUATION
from baselines.mappers.filters.content_filters import high_quality_ratio
import concurrent.futures
from baselines.mappers.enrichers.quality_prediction_enrichers_calc_fasttext import *

csv.field_size_limit(sys.maxsize)


def fineweb_quality(doc, short_line_length=30) -> Dict:
    text = doc['text']
    lines = text.split("\n")
    stop_chars = tuple(TERMINAL_PUNCTUATION)
    ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
    doc['line_punct_ratio'] = ratio
    ratio = sum(1 for line in lines if len(line) <= short_line_length) / len(lines)
    doc['short_line_ratio'] = ratio
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
    f = "oss://si002558te8h/dclm/output/c4_noclean_v2_raw/c4/en.noclean/processed_data/c4-train.00000-of-07168_processed.jsonl.zst"

    lines = []
    count = 0
    for line in read_jsonl(f):
        if count >= 5000:
            break
        lines.append(line)
        count += 1

    target_path = "oss://si002558te8h/dclm/origin/v2_raw_sample/c4-train.00000-of-07168_processed.jsonl.zst"
    write_jsonl(lines, target_path)

    docs = []
    old_docs = list(read_jsonl(target_path))

    def process(doc, i):
        doc = fineweb_quality(doc)
        doc = high_quality_ratio_filter(doc)
        doc = fasttext_filter(doc)
        print("=====", i)
        return doc
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=40) as executor:
        futures = []
        
        for i, doc in enumerate(old_docs):
            futures.append(executor.submit(process, doc, i))

        for future in concurrent.futures.as_completed(futures):
            docs.append(future.result())

        print(len(docs))

    stat_path = "oss://si002558te8h/dclm/origin/v2_raw_sample/c4-train.00000-of-07168_stat3.jsonl.zst"
    write_jsonl(docs, stat_path)
