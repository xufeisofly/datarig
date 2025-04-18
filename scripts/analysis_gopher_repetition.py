from baselines.mappers.filters.content_filters import *


if __name__ == '__main__':
    data = """
    """
    page = {
        'text': data,
    }
    ret = massive_web_repetition_filters(page, tokenizer='fasttext', annotate=True, token="")
    print(ret)
