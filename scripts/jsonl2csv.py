import logging
from baselines.core.file_utils import is_exists, read_jsonl, write_jsonl
import csv
import os


# 设置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def main(jsonl_filepath):
    stats = [
              'n_words',
              'avg_word_length',
              "avg_words_per_line",
              "short_word_ratio_3",
              "long_word_ratio_7",
              "type_token_ratio",
              "uppercase_word_ratio",
              "capitalized_word_ratio",
              "stop_word_ratio",
              "n_lines",
              "avg_line_length",
              "short_line_ratio_chars_10",
              "short_line_ratio_chars_30",
              "long_line_ratio_chars_2000",
              "long_line_ratio_chars_10000",
              "lines_ending_with_terminal_mark_ratio",
              "bullet_point_lines_ratio",
              "line_duplicates",
              "line_char_duplicates",
              "length",
              "white_space_ratio",
              "non_alpha_digit_ratio",
              "digit_ratio",
              "uppercase_ratio",
              "elipsis_ratio",
              "punctuation_ratio",        
    ]

    headers = ['warc_record_id', 'text']
    headers += stats
    
    output_csv = f"{jsonl_filepath}.csv"
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for line in read_jsonl(jsonl_filepath):
            metadata = line['metadata']
            row = {
                'warc_record_id': metadata['warc_record_id'],
                'text': line['text'],                
            }
            for stat in stats:
                row[stat] = metadata.get(stat)

            writer.writerow(row.values())


if __name__ == '__main__':
    for jsonl_filename in ['d1_abstract_stats.jsonl', 'd1_nonabstract_stats.jsonl', 'd1_raw_stats.jsonl']:
        path = os.path.join('/Users/sofly/Downloads/output', jsonl_filename)
        main(path)
