#!/usr/bin/env bash
set -e

ray start --head --port 6379 --node-ip-address=127.0.0.1 --disable-usage-stats &
sleep 3

python3 /app/dclm-sci/ray_processing/process.py "$@"
