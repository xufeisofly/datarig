#!/usr/bin/env bash
set -e

ray start --head --port 6379 &

python3 /app/ray_processing/process.py "$@"
