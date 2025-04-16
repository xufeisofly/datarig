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


if __name__ == '__main__':
    file_path = '/Users/sofly/projects/dataprocess/data/OrganicChemistry_25073rows.csv'
    records = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            records.append({
                'text': row[2],
                'url': row[1],
                'id': row[0],
            })

    records = records[1:]
    write_jsonl(records, './OrganicChemistry_25073rows.jsonl')
    
