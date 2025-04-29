"""
                metadata = {
                    "WARC-Type": record.rec_headers.get_header('WARC-Type', ''),
                    "WARC-Date": record.rec_headers.get_header('WARC-Date', ''),
                    "WARC-Record-ID": record.rec_headers.get_header('WARC-Record-ID', ''),
                    "WARC-Target-URI": record.rec_headers.get_header('WARC-Target-URI', ''),
                    "Content-Type": record.rec_headers.get_header('Content-Type', ''),
                    "Content-Length": record.rec_headers.get_header('Content-Length', ''),
                    "WARC-Refers-To": record.rec_headers.get_header('WARC-Refers-To', ''),
                    "WARC-Block-Digest": record.rec_headers.get_header('WARC-Block-Digest', ''),
                }

                record_data = {
                    "text": content,
                    "metadata": metadata,
                    "warcinfo": warcinfo_data
                }
"""
import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv



# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
import sys

if __name__ == '__main__':
    file_path = '/root/dataprocess/OrganicChemistry_12494rows.csv'
    records = []
    csv.field_size_limit(sys.maxsize)
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            records.append({
                'text': row[2],
                'url': row[1],
                'id': row[0],
            })

    records = records[1:]
    write_jsonl(records, './OrganicChemistry_12494rows.jsonl')
    
    oss_file = "oss://si002558te8h/dclm/output/deduped/OrganicChemistry2/OrganicChemistry_12494rows_processed.jsonl"

    headers = ['id', 'url', 'text']
    output_csv = './OrganicChemistry_12494rows_deduped.csv'
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        for line in read_jsonl(oss_file):
            row = [line['id'], line['url'], line['text']]
            writer.writerow(row)    
