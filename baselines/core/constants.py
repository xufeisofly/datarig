# JSONL keys
from typing import Dict

CONTENT = "text"
URL = "url"
CHUNK = "chunk"
FILTER_REASON = "filter_reason"

# Stats file keys
PROCESS_SETUP_KEY_NAME = 'process_setup'
PROCESS_END_KEY_NAME = 'process_finished'
COMMIT_KEY_NAME = 'commit'

GLOBAL_FUNCTIONS = {
	'exact_dedup': None,
}

def set_filter_reason_if_annotate(page: Dict, reason: str, annotate: bool):
    if not annotate: 
        return []

    if not page.get(FILTER_REASON):
        page[FILTER_REASON] = reason
    return [page]    
