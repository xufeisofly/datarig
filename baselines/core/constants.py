# JSONL keys
from typing import Dict, Optional

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

LANG_KEY_PREFIX = "language_id_paragraph"

def set_filter_reason_if_annotate(page: Dict, reason: str, annotate: bool):
    if not annotate: 
        return []

    if not page.get(FILTER_REASON):
        page[FILTER_REASON] = reason
    return [page]    


def get_lang_from_page(page: Dict, language_key="language_id_whole_page_fasttext"):
    lang_dict = page.get(language_key, {})

    if not lang_dict:
        return "eng"

    lang = max(lang_dict, key=lang_dict.get, default="eng")

    print("===== lang: ", lang)
    return lang
