#!/bin/bash

cargo run --release bff \
   --inputs oss://si002558te8h/dclm/output/tianwei_0319/DCLM_sub_by_keywords/processed_data/ \
   --output-directory oss://si002558te8h/dclm/dedup_output/ \
   --expected-ngram-count 1000000 \
   --fp-rate 0.01 \
   --min-ngram-size 13 \
   --max-ngram-size 13 \
   --filtering-threshold 0.8 \
   --remove-type old-both \
   --annotate 
