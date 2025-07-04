import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv
import sys
from typing import Dict

csv.field_size_limit(sys.maxsize)

DCLM_URL_FILTER = 'Dclm_UrlFilter'
DCLM_GOPHER_QUALITY = 'Dclm_GopherQuality'
DCLM_GOPHER_REPETION = 'Dclm_GopherRepetition'
DCLM_REFINED_WEB_QUALITY = 'Dclm_LineWiseQuality'
DCLM_FASTTEXT_FILTER = 'Dclm_FasttextFilter'
FINEWEB_C4_QUALITY = 'Fineweb_C4Quality'
FINEWEB_GOPHER_QUALITY = 'Fineweb_GopherQuality'
FINEWEB_GOPHER_REPETITION = 'Fineweb_GopherRepetition'
FINEWEB_FINEWEB_QUALITY = 'Fineweb_FinewebQuality'

dclmMap = {
    DCLM_URL_FILTER: "oss://si002558te8h/dclm/output/Experiment6/zhang/urlfilter/processed_data/subject_str_1030_zhang_processed.jsonl",
    DCLM_GOPHER_QUALITY: "oss://si002558te8h/dclm/output/Experiment6/zhang/gopher_quality/processed_data/subject_str_1030_zhang_processed.jsonl",
    DCLM_GOPHER_REPETION: "oss://si002558te8h/dclm/output/Experiment6/zhang/gopher_repetition/processed_data/subject_str_1030_zhang_processed.jsonl",
    DCLM_REFINED_WEB_QUALITY: "oss://si002558te8h/dclm/output/Experiment6/zhang/linewise_quality/processed_data/subject_str_1030_zhang_processed.jsonl",
    DCLM_FASTTEXT_FILTER: "oss://si002558te8h/dclm/output/Experiment6/zhang/fasttext_filter/processed_data/subject_str_1030_zhang_processed.jsonl",
}

finewebMap = {
    FINEWEB_C4_QUALITY: "/Users/sofly/projects/dataprocess/data/Experiment_shixi/exp_tag/zhang/subject_str_1030_zhang_c4.jsonl",
    FINEWEB_GOPHER_QUALITY: "/Users/sofly/projects/dataprocess/data/Experiment_shixi/exp_tag/zhang/subject_str_1030_zhang_gopher_qual.jsonl",
    FINEWEB_GOPHER_REPETITION: "/Users/sofly/projects/dataprocess/data/Experiment_shixi/exp_tag/zhang/subject_str_1030_zhang_gopher_rep.jsonl",
    FINEWEB_FINEWEB_QUALITY: "/Users/sofly/projects/dataprocess/data/Experiment_shixi/exp_tag/zhang/subject_str_1030_zhang_fineweb_qual.jsonl",    
}

def read_dclm_file(dclm_file_path, dclm_lines: Dict, module: str):
    for line in read_jsonl(dclm_file_path):
        id = line['warc_record_id']
        url = line.get('warc_target_uri', None)
        if not url:
            url = line['url']
        reason = line.get('filter_reason', '')
        if type(reason) == tuple:
            reason = reason[0]
        if not dclm_lines.get(id):
            dclm_lines[id] = {
                "id": id,
                "text": line['text'],
                "url": url,
                module: reason,
            }
        else:
            dclm_lines[id][module] = reason
    return dclm_lines

def read_dclm_files(subject):
    dclm_lines = {}
    
    for module in [DCLM_URL_FILTER,
                   DCLM_GOPHER_QUALITY,
                   DCLM_GOPHER_REPETION,
                   DCLM_REFINED_WEB_QUALITY,
                   DCLM_FASTTEXT_FILTER]:
        file_path = dclmMap[module]
        file_path = file_path.replace('subject_str', subject)
        dclm_lines = read_dclm_file(file_path, dclm_lines, module)    
    
    return dclm_lines


def read_fineweb_file(fineweb_file_path, fineweb_lines: Dict, module: str):
    for line in read_jsonl(fineweb_file_path):
        id = line['warc_record_id']
        url = line['warc_target_uri']

        if not fineweb_lines.get(id):
            fineweb_lines[id] = {
                "id": id,
                "text": line['text'],
                "url": url,
                module: line.get('filter_reason', ''),           
            }
        else:
            fineweb_lines[id][module] = line.get('filter_reason', '')
    return fineweb_lines

def read_fineweb_files(subject):
    fineweb_lines = {}

    for module in [FINEWEB_C4_QUALITY,
                   FINEWEB_GOPHER_QUALITY,
                   FINEWEB_GOPHER_REPETITION,
                   FINEWEB_FINEWEB_QUALITY]:
        file_path = finewebMap[module]
        file_path = file_path.replace('subject_str', subject)
        fineweb_lines = read_fineweb_file(file_path, fineweb_lines, module)
    
    return fineweb_lines


def merge(subject):
    dclm_lines = read_dclm_files(subject)
    fineweb_lines = read_fineweb_files(subject)
    
    lines = []
    for id, dclm_info in dclm_lines.items():
        fineweb_info = fineweb_lines[id]

        lines.append({
            "id": id,
            "text": dclm_info['text'],
            DCLM_URL_FILTER: dclm_info[DCLM_URL_FILTER],
            DCLM_GOPHER_QUALITY: dclm_info[DCLM_GOPHER_QUALITY],
            DCLM_GOPHER_REPETION: dclm_info[DCLM_GOPHER_REPETION],
            DCLM_REFINED_WEB_QUALITY: dclm_info[DCLM_REFINED_WEB_QUALITY],
            DCLM_FASTTEXT_FILTER: dclm_info[DCLM_FASTTEXT_FILTER],
            FINEWEB_GOPHER_QUALITY: fineweb_info[FINEWEB_GOPHER_QUALITY],
            FINEWEB_GOPHER_REPETITION: fineweb_info[FINEWEB_GOPHER_REPETITION],
            FINEWEB_C4_QUALITY: fineweb_info[FINEWEB_C4_QUALITY],
            FINEWEB_FINEWEB_QUALITY: fineweb_info[FINEWEB_FINEWEB_QUALITY],
        })

    print(subject, "dclm: ", len(dclm_lines), "fineweb: ", len(fineweb_lines))
        
    headers = ['warc_record_id', 'text',
               DCLM_URL_FILTER, DCLM_GOPHER_QUALITY, DCLM_GOPHER_REPETION, DCLM_REFINED_WEB_QUALITY, DCLM_FASTTEXT_FILTER,
               FINEWEB_GOPHER_QUALITY, FINEWEB_GOPHER_REPETITION, FINEWEB_C4_QUALITY, FINEWEB_FINEWEB_QUALITY]

    output_csv = f"/Users/sofly/projects/dataprocess/data/Experiment_shixi/{subject}_zhang_1030.csv"
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)        
        for line in lines:
            writer.writerow([line['id'], line['text'],
                             line[DCLM_URL_FILTER] or 'nil',
                             line[DCLM_GOPHER_QUALITY] or 'nil',
                             line[DCLM_GOPHER_REPETION] or 'nil',
                             line[DCLM_REFINED_WEB_QUALITY] or 'nil',
                             line[DCLM_FASTTEXT_FILTER] or 'nil',
                             line[FINEWEB_GOPHER_QUALITY] or 'nil',
                             line[FINEWEB_GOPHER_REPETITION] or 'nil',
                             line[FINEWEB_C4_QUALITY] or 'nil',
                             line[FINEWEB_FINEWEB_QUALITY] or 'nil'])        



def merge_mannual_csv(machinary_csv, artificial_csv):
    machinary_csv = '/Users/sofly/projects/dataprocess/data/sample_en_1740.csv'
    artificial_csv = '/Users/sofly/projects/dataprocess/data/sample_1030.csv'
    lines2 = []
    adds = []
    with open(artificial_csv, newline='', encoding='utf-8') as acsv:
        reader = csv.reader(acsv)
        for row in reader:
            lines2.append(row)
            adds.append(row[3:])

    lines2 = lines2[1:]
    add_header = adds[0]

    newlines = []
    with open(machinary_csv, newline='', encoding='utf-8') as mcsv:
        reader = csv.reader(mcsv)
        for row in reader:
            newlines.append(row)

    header = newlines[0]
    header += add_header
    newlines = newlines[1:]

    lines = []
    for l in newlines:
        for l2 in lines2:
            if l[0] == l2[1]:
                lines.append(l+l2[3:])


    with open("./sample_cleaned_1740_merged.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)        
        for line in lines:
            writer.writerow(line)

            
if __name__ == '__main__':
    subjects = ['sample']

    for subject in subjects:        
        merge(subject)

    # merge_mannual_csv() 
