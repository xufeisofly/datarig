# JSONL keys
CONTENT = "text"
URL = "url"
CHUNK = "chunk"

# Stats file keys
PROCESS_SETUP_KEY_NAME = 'process_setup'
PROCESS_END_KEY_NAME = 'process_finished'
COMMIT_KEY_NAME = 'commit'

GLOBAL_FUNCTIONS = {
	'exact_dedup': None,
}


split_word_model = 'uniseg'

def set_word_model(m: str):
    global split_word_model
    split_word_model = m

def get_word_model():
    global split_word_model
    return split_word_model
