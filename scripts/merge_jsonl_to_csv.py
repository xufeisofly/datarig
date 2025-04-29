import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv
import sys

csv.field_size_limit(sys.maxsize)


def main(subject, dclm_file_path, fineweb_file_path):
    dclm_lines = {}
    for line in read_jsonl(dclm_file_path):
        id = line['warc_record_id']
        dclm_lines[id] = {
            "id": id,
            "text": line['text'],
            "url": line['url'],
            "filter_reason": line.get('filter_reason', ''),
        }

    fineweb_lines = {}
    for line in read_jsonl(fineweb_file_path):
        id = line['metadata'].get('warc_record_id', None)
        if not id:
            id = line['warc_record_id']

        url = line['metadata'].get('warc_target_uri', None)
        if not url:
            url = line['warc_target_uri']

        fineweb_lines[id] = {
            "id": id,
            "text": line['text'],
            "url": url,
            "filter_reason": line['metadata'].get('filter_reason', ''),            
        }

    lines = []
    for id, dclm_info in dclm_lines.items():
        fineweb_info = fineweb_lines[id]

        lines.append({
            "id": id,
            "text": dclm_info['text'],
            'dclm_filter_reason': dclm_info['filter_reason'],
            'fineweb_filter_reason': fineweb_info['filter_reason'],
        })

    print(subject, "dclm: ", len(dclm_lines), "fineweb: ", len(fineweb_lines))
        
    headers = ['warc_record_id', 'text', 'dclm_dropped', 'fineweb_dropped', 'dclm_filter_reason', 'fineweb_filter_reason']

    output_csv = f"/root/dataprocess/data/experiment2/csv/{subject}_1000.csv"
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)        
        for line in lines:
            dclm_dropped = bool(line['dclm_filter_reason'])
            fineweb_dropped = bool(line['fineweb_filter_reason'])
            writer.writerow([line['id'], line['text'], dclm_dropped, fineweb_dropped, line['dclm_filter_reason'], line['fineweb_filter_reason']])


if __name__ == '__main__':
    subjects = ['ArtificialIntelligenceImageProcessing', 'EnvironmentalEngineering', 'SoftwareEngineering', 'Geology', 'Economics', 'Philosophy', 'LiteraryStudies', 'SocialPsychology']

    for subject in subjects:
        dclm_file_path = "/root/dataprocess/data/experiment2/" + subject + "_1000_processed.jsonl"
        fineweb_file_path = "/root/dataprocess/data/experiment2/" + subject + "_1000_tag.jsonl"        
        main(subject, dclm_file_path, fineweb_file_path)
