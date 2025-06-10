import os
from typing import List, Dict, Union, Optional
import re

from baselines.mappers.core_utils import split_paragraphs, split_sentences, split_words
from core.factory_utils import factory_function
from core.constants import CONTENT, get_lang_from_page, set_filter_reason_if_annotate, TERMINAL_PUNCTUATION, PUNCTUATION_SET

from typing import Union, Dict, List, Optional, Tuple
from collections import Counter
import numpy as np
import re
from nltk import ngrams, word_tokenize
from transformers import AutoTokenizer

# Taken from https://github.com/togethercomputer/RedPajama-Data/blob/rp_v1/data_prep/github/github_run_filter.py#L18
RPJ_GITHUB_EXTENSIONS = (".asm", ".bat", ".cmd", ".c", ".h", ".cs", ".cpp",
                        ".hpp", ".c++", ".h++", ".cc", ".hh", ".C", ".H",
                        ".cmake", ".css", ".dockerfile", ".f90", ".f", ".f03",
                        ".f08", ".f77", ".f95", ".for", ".fpp", ".go", ".hs",
                        ".html", ".java", ".js", ".jl", ".lua", ".md",
                        ".markdown", ".php", ".php3", ".php4", ".php5",
                        ".phps", ".phpt", ".pl", ".pm", ".pod", ".perl",
                        ".ps1", ".psd1", ".psm1", ".py", ".rb", ".rs", ".sql",
                        ".scala", ".sh", ".bash", ".command", ".zsh", ".ts",
                        ".tsx", ".tex", ".vb", "Dockerfile", "Makefile",
                        ".xml", ".rst", ".m", ".smali")


def github_extension_filter(page: Dict, filename_key: str = 'filename', allowed_extensions: Union[Tuple[str], List[str]]=None) -> List[Dict]:
    """
    Removes files that do not have an extension from a set of allowed extensions. If the filename is not present, then removes the page.

    Args:
    page (Dict): A dictionary representing a JSON object.
    filename_key (str): Key name in the dictionary where the filename is stored. Defaults to 'filename'.
    allowed_extensions (set): A set of allowed file extensions.

    Returns:
    List[Dict]: A list containing the input JSON object if its file extension is in the allowed set, or an empty list otherwise.
    """
    if allowed_extensions is None:
        allowed_extensions = RPJ_GITHUB_EXTENSIONS
    elif isinstance(allowed_extensions, list):
        # str.endswith can aceept a tuple but not a list
        allowed_extensions = tuple(allowed_extensions)

    filename = page.get(filename_key, '')

    return [page] if filename.endswith(allowed_extensions) else []

def line_length_filter(page: Dict, length_type: str = 'max', max_length=1000) -> List[Dict]:
    """
    Remove files whose average or maximum line lengths do not fall within designated ranges.

    Args:
    page (Dict): A dictionary representing a JSON object.
    length_type (str): The type of length to measure. Options are 'max' and 'avg'. Defaults to the values from RPJs
    max_length (int): The maximum allowed line length. Defaults to 1000 characters.

    Returns:
    List[Dict]: A list containing the input JSON object if it meets the length criteria, or an empty list otherwise.
    """
    if not page[CONTENT]:
        return []

    lines = page[CONTENT].splitlines()
    if length_type == 'max':
        line_length = max(len(line) for line in lines)
    elif length_type == 'avg':
        total_length = sum(len(line) for line in lines)
        line_length = total_length / len(lines)
    else:
        raise ValueError("length_type must be one of {max, avg}")

    return [] if line_length > max_length else [page]        

def alphanumeric_char_ratio_filter(page: Dict, max_alnum_ratio=0.25) -> List[Dict]:
    """
    Discard files whose proportion of alphanumeric characters is less than a specified ratio.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    max_ratio -- The maximum allowed ratio of alphanumeric characters. Defaults to 0.25, which rpj-v1 
            uses to filter github pages.
                 
    Returns:
    A list containing the input JSON object if it passes the filter, or an empty list if it doesn't.
    """
    if page[CONTENT] == '':
        return []

    alnum_count = sum(map(lambda char: 1 if char.isalnum() else 0, page[CONTENT]))
    alnum_ratio = alnum_count / len(page[CONTENT])

    if alnum_ratio < max_alnum_ratio:
        return []
    
    return [page]

@factory_function
def alphabetic_characters_to_tokens_filter(tokenizer_name: str = "EleutherAI/pythia-6.9b-deduped") -> List[Dict]:
    """
    Files with a ratio between the number of alphabetical characters and the number tokens of less than 1.5. This is used in rpj-v1's
    filtering of its github source. 

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    tokenizer_name -- The name of the tokenizer used to get token counts. Defaults to the one in rpj-v1's
            version of this filter for github. 
    max_ratio -- The maximum allowed ratio of alphabetical characters, defaults to rpj-v1's choice of 1.5 for github
                
    Returns:
    A list containing the input JSON object if it passes the filter, or an empty list if it doesn't.
    """
    
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)

    def filter_fn(page: Dict, max_ratio=1.5) -> List[Dict]:
        if page[CONTENT] == '':
            return []

        num_token = len(tokenizer.tokenize(page[CONTENT]))
        num_alpha = len([c for c in page[CONTENT] if c.isalpha()])
        alpha_ratio = num_alpha / num_token

        return [] if alpha_ratio < max_ratio else [page]
        
    return filter_fn

def massive_web_repetition_filters(page: Dict, skip_paragraph=False, tokenizer='uniseg', annotate=False, token="", debug=False, language_key='language_id_whole_page_fasttext', use_fineweb_implementation=False) -> List[Dict]:
    """
    Applies the repetition filters from Gopher (Rae et al., 2021)
    Calls repetition_filter across many different granularities
    for {2,3,4}-grams we need to count the fraction of characters in the most common n-gram, and 
    for {5,6,7,8,9,10}-grams we count the function of characters appearing in n-grams that repeat more than once
    
    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    skip_paragraph -- If True, skips the paragraph-based filters, such as in the case where text extraction does 
            not distinguish lines v.s. paragraphs. 
    Returns:
    A list containing the input JSON object if it passes the set of repetition filters,
    or an empty list if it doesn't.
    """

    if use_fineweb_implementation:
        return fineweb_gopher_repetition_filter(page, language_key=language_key, annotate=annotate)

    cache = {}
    if len(repetition_filter(page, "line", 0.3, count_characters=False, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:line"+token, annotate)
    elif not skip_paragraph and len(repetition_filter(page, "paragraph", 0.3, count_characters=False, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:paragraph"+token, annotate)
    elif len(repetition_filter(page, "line", 0.2, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:line_char"+token, annotate)
    elif not skip_paragraph and len(repetition_filter(page, "paragraph", 0.2, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:paragraph_char"+token, annotate)
    elif len(repetition_filter(page, 2, 0.2, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:2_gram"+token, annotate)
    elif len(repetition_filter(page, 3, 0.18, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:3_gram"+token, annotate)
    elif len(repetition_filter(page, 4, 0.16, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:4_gram"+token, annotate)
    elif len(repetition_filter(page, 5, 0.15, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:5_gram"+token, annotate)
    elif len(repetition_filter(page, 6, 0.14, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:6_gram"+token, annotate)
    elif len(repetition_filter(page, 7, 0.13, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:7_gram"+token, annotate)
    elif len(repetition_filter(page, 8, 0.12, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:8_gram"+token, annotate)
    elif len(repetition_filter(page, 9, 0.11, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:9_gram"+token, annotate)
    elif len(repetition_filter(page, 10, 0.10, cache=cache, tokenizer=tokenizer, debug=debug, language_key=language_key)) == 0:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:10_gram"+token, annotate)
    else:
        return [page]


def find_all_duplicate(words: list[str], n: int) -> int:
    n_words = len(words)
    unique = set()
    repeated_chars, idx = 0, 0
    while idx < n_words - n + 1:
        n_gram = "".join(words[idx : idx + n])
        if n_gram in unique:
            print(n_gram)
            repeated_chars += len(n_gram)
            idx += n
        else:
            unique.add(n_gram)
            idx += 1
    assert repeated_chars <= len("".join(words))
    return repeated_chars    
    

def repetition_filter(page: Dict, granularity: Union[str, int], max_fraction: float, 
                      count_characters: bool=True, ngram_char_ratio: str=None, ignore_case: bool=False, cache: Dict=None, tokenizer='uniseg', debug=False, language_key='language_id_whole_page_fasttext') -> List[Dict]:
    """
    Filters the input JSON object based on the ratio of repetition at {line, paragraph, n-gram} granularity of the CONTENT field.

    This function measures the ratio of repetition at various granularities.

    If the ratio of repetition is greater than `max_fraction`, it returns an empty list.
    If the length is less/equal to `max_fraction`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    granularity -- An string or int for how the repetition is measured. Options for string are {"line", "paragraph"}.
            If it is an int, it is the n-gram size.
    max_fraction -- The maximum ratio of repetition.
    count_characters -- Whether to count characters in duplicates or not for granularity "line" or "paragraph". Defaults to True.
    ngram_char_ratio -- When the granularity is n-grams, this specifies what ratio to measure. Choices are either 'most_common' which
            which looks at the characters taken up by the most repeated n-gram and 'all' which looks at characters taken up by all
            repeated n-grams (without double counting words). If not supplied, uses the defaults from Gopher for various n-gram sizes. 
    ignore_case -- Whether or not to convert text to lowercase before looking for duplicates
    cache -- An optional dictionary containing cached computations to speed up repeated invocations of repetition_filter (e.g., as 
            used in the implementation of massive_web_repetition_filters)

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """

    if page[CONTENT] == '':
        return []

    if cache is None:
        cache = {}

    text = page[CONTENT].lower() if ignore_case else page[CONTENT]

    if isinstance(granularity, str):
        assert granularity in ['line', 'paragraph'], "granularity must be either 'line', 'paragraph', or an int"
        sep = '\n\n' if granularity == 'paragraph' else '\n'

        if granularity not in cache:
            cache[granularity] = segments = split_paragraphs(text, paragraph_end=sep, remove_empty=True)
        else:
            segments = cache[granularity]

        if len(segments) == 1:
            return [page]
        elif len(segments) == 0:
            return []

        if granularity + '/count' not in cache:
            cache[granularity + '/chars'] = total_chars = sum(len(s) for s in segments)  # Do not count empty lines as characters
            cache[granularity + '/count'] = segment_counts = Counter(segments)
        else:
            total_chars = cache[granularity + '/chars']
            segment_counts = cache[granularity + '/count']

        if count_characters:
            repeated_fraction = sum((len(segment) * count) for segment, count in segment_counts.items() if count > 1) / total_chars
        else:
            repeated_fraction = sum(count for count in segment_counts.values() if count > 1) / len(segments)
        
        if repeated_fraction > max_fraction:
            return []

    elif isinstance(granularity, int):
        if 'words' not in cache:
            cache['words'] = words = split_words(text, ignore_punctuation=True, model=tokenizer,
                                                 language=get_lang_from_page(page, language_key))
            cache['words/chars'] = total_chars = sum(len(w) for w in words) # Do not count whitespace/punctuation as characters for words
        else:
            words = cache['words']
            total_chars = cache['words/chars']

        # No point caching the n-grams, we are using each granularity only once
        n_grams = list(ngrams(words, granularity))    

        if len(n_grams) == 0:
            return [page]

        # Use the gopher default settings if ngram_char_ratio is not explicitly supplied
        if ngram_char_ratio is None:
            if granularity in {2,3,4}:
                ngram_char_ratio = 'most_common'
            elif granularity in {5,6,7,8,9,10}:
                ngram_char_ratio = 'all'
            else:
                raise ValueError("For n-gram counts, if ngram_char_ratio is not given, the granularity must be one of {2,3,4,5,6,7,8,9,10}")

        # No point caching the n-grams Counter, we are using each granularity only once
        ngram_counts = Counter(n_grams)

        # If no n-grams are repeated, then just return the page
        ordered_counts = ngram_counts.most_common()
        most_common_ngram, most_common_count = ordered_counts[0]
        if most_common_count == 1:
            return [page]

        repeated_words = []
        if ngram_char_ratio == 'most_common':
            # Check if there is a longer n-gram (in chars) that also has the same count 
            most_common_length = sum(len(w) for w in most_common_ngram)
            for ngram, count in ordered_counts:
                if count != most_common_count:
                    break
                else:
                    ngram_length = sum(len(w) for w in ngram)
                    most_common_length = max(ngram_length, most_common_length)

            most_common_char_count = most_common_length * most_common_count
            repeated_fraction = most_common_char_count / total_chars
        elif ngram_char_ratio == 'all':
            repeated_word_indices = set()
            for idx, ngram in enumerate(n_grams):
                if ngram_counts[ngram] > 1:
                    repeated_word_indices.update(range(idx, idx + granularity))
            repeated_word_char_count = sum((len(words[i]) for i in repeated_word_indices))
            repeated_fraction = repeated_word_char_count / total_chars
            if debug:
                repeated_words = [words[i] for i in repeated_word_indices]            
        else:
            raise ValueError("For n-gram counts, ngram_char_ratio must one of {None, 'most_common', 'all'}")

        if repeated_fraction > max_fraction:
            if debug:
                print(f"repeated_fraction: {repeated_fraction}, repeated words in n_gram: ", repeated_words)
            return []

    else:
        raise ValueError("granularity must be either 'line', 'paragraph', or an int")

    return [page]


def page_length_filter(page: Dict, length_type: str, min_length: int = 1,
                       max_length: int = float('inf'), annotate=False, token="", language_key="language_id_whole_page_fasttext", **kwargs) -> List[Dict]:
    """
    Filters the input JSON object based on the length of the CONTENT field.

    This function measures page length according to a specified atomic unit (e.g., char, word, sentence,
    line, paragraph).

    If the length is less than `min_length`, it returns an empty list. 
    If the length is greater/equal to `min_length`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    length_type -- An string for how length is measured. Options are {"char", "word", "sentence", "line", "paragraph"}
    min_length -- The minimum length threshold as measured by length_type
    max_length -- The maximum length threshold as measured by length_type
    kwargs -- Any parameters specific to one of the split_ functions in core_utils

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """

    # TODO: Do we want to cache some of these splits for other methods?
    try:
        if length_type == 'word':
            split_text = split_words(page[CONTENT], language=get_lang_from_page(page, language_key=language_key), **kwargs)
        elif length_type == 'sentence':
            split_text = split_sentences(page[CONTENT], **kwargs)
        elif length_type == 'line':
            split_text = split_paragraphs(page[CONTENT], paragraph_end='\n', **kwargs)
        elif length_type == 'paragraph':
            split_text = split_paragraphs(page[CONTENT], paragraph_end='\n\n', **kwargs)
        elif length_type == 'char':
            split_text = page[CONTENT]
        else:
            raise ValueError("length_type needs to be one of {word, sentence, line, paragraph}")

        page_length = len(split_text)
        if page_length < min_length or page_length > max_length:
            return set_filter_reason_if_annotate(page, "page_length_filter"+token, annotate)
        else:
            return [page]
    except Exception:
        if len(page[CONTENT]) > 1000:
            return [page]
        return set_filter_reason_if_annotate(page, "word_split_falied"+token, annotate)


@factory_function
def substring_filter(banlist: Union[str, List] = None, banlist_from_fname: str = None,
                     location: str = 'any', case_sensitive=False, exact_word=False) -> List[Dict]:
    """
    Filters the input JSON object by removing any document that contains a particular substring contained
    in a banlist. Also, one may specify whether the banned substring must exist at the beginning/end of 
    the page. Use cases include 
        - lorem ipsum filter from C4 (banlist = "lorem ipsum", location = 'any') from C4
        - curly bracket filter from C4 (banlist = "{", location = 'any') from C4
        - checking for LDNOOBW to filter for in appropriate content from C4

    This function is implemented as a factory_function since we provide the option to load in a banlist from
    a text file and do not want to repeatedly load in this file when calling the function. The resulting 
    filter_fn removes any page that contains a substring in banlist in the specified location 
    within the page. If the substring is detected, it returns an empty list. If it's not, it returns a 
    list containing the original JSON object.

    Arguments:
    page (only for the compiled filter_fn) -- A dictionary representing a JSON object. It should have a 
    CONTENT field that contains the text to be analyzed
    banlist -- The substring that one checks the presence of. One may also pass in a list of substrings
    banlist_from_fname -- Gives the option to load in a large banlist from a .txt file where each substring
                          is on a spearate line. This takes precedence over passing in via banlist 
    location -- Specifies where the substring must be located. Options are "prefix" for beginning,
    "suffix" for end, and "any" for anywhere.
    case_sensitive -- Specifies whether substring must match casing
    exact_word -- Specifies whether the banlist word must appear as its own word

    Returns:
    A list containing the updated input JSON object if it passes the filter, or an empty list
    if it doesn't.
    """
    if location not in {'any', 'prefix', 'suffix'}:
        raise ValueError("location must be one of {any, prefix, suffix}")

    if banlist_from_fname is not None:
        with open(banlist_from_fname, "r") as file:
            banlist = file.read().splitlines()
    elif isinstance(banlist, str):
        banlist = [banlist]

    banlist = banlist if case_sensitive else [b.lower() for b in banlist]
    pattern = f"(?:{'|'.join(banlist)})"
    
    if exact_word:
        pattern = rf"\b{pattern}\b"

    if location == 'prefix':
        pattern = rf"^{pattern}"
    elif location == 'suffix':
        pattern = rf"{pattern}$"

    pattern = re.compile(pattern)

    def filter_fn(page: Dict) -> List[Dict]:
        text = page[CONTENT] if case_sensitive else page[CONTENT].lower()
        return [] if pattern.search(text) else [page]

    return filter_fn


def bullet_count_filter(page: Dict, max_bullet_start_ratio: float = 0.9, annotate=False, token="") -> List[Dict]:
    """
    Filters the input JSON object based on the number of lines starting with bullet in the CONTENT field.

    This function measures the ratio of lines starting with bullet,
        i.e., (the number of lines starting with bullet) / (the number of lines).

    If the ratio of lines starting with bullet is greater than `max_bullet_start_ratio`, it returns an empty list.
    If the ratio of lines starting with bullet is less/equal to `max_bullet_start_ratio`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    max_bullet_start_ratio -- The maximum ratio of lines starting with bullet

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """

    lines = split_paragraphs(page[CONTENT], paragraph_end='\n')
    max_bullet_count = max_bullet_start_ratio * len(lines)

    if sum([any(line.startswith(bullet) for bullet in ['●', '•', '*', '-']) for line in lines]) > max_bullet_count:
        return set_filter_reason_if_annotate(page, "bullet_count_filter" + token, annotate)
    return [page]


def ellipsis_count_filter(page: Dict, max_ellipsis_end_ratio: float = 0.3, annotate=False, token="") -> List[Dict]:
    """
    Filters the input JSON object based on the number of lines ending with ellipsis in the CONTENT field.

    This function measures the ratio of lines ending with ellipsis,
        i.e., (the number of lines ending with ellipsis) / (the number of lines).

    If the ratio of lines ending with ellipsis is greater than `max_ellipsis_end_ratio`, it returns an empty list.
    If the ratio of lines ending with ellipsis is less/equal to `max_ellipsis_end_ratio`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    max_ellipsis_end_ratio -- The maximum ratio of lines ending with ellipsis

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """

    lines = split_paragraphs(page[CONTENT], paragraph_end='\n')
    max_ellipsis_count = max_ellipsis_end_ratio * len(lines)

    if sum([any(line.endswith(ell) for ell in ['...', '. . .', '\u2026']) for line in lines]) > max_ellipsis_count:
        return set_filter_reason_if_annotate(page, "ellipsis_count_filter"+token, annotate)
    return [page]


def stop_word_filter(page: Dict, count_unique: bool = False, min_stop_word: int = 2, annotate=False, token="") -> List[Dict]:
    """
    Filters the input JSON object based on the number of stop words in the text.

    This function measures the number of stop words (i.e., the, be, to, of, and, that, have, with).

    If the number of stop words is less than `min_stop_word`, it returns an empty list.
    If the number of stop words is greater/equal to `min_stop_word`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    count_unique -- Whether to only count unique stop words instead of all instances of stop words
    min_stop_word -- The minimum number of stop words threshold

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """

    stop_words = {'the', 'be', 'to', 'of', 'and', 'that', 'have', 'with'}

    if count_unique:
        occur_stop_words = set()
        for word in page[CONTENT].split():
            word_lower = word.lower()
            if word_lower in stop_words:
                occur_stop_words.add(word_lower)
                if len(occur_stop_words) >= min_stop_word:
                    return [page]
    else:
        count = 0
        for word in page[CONTENT].split():
            if word.lower() in stop_words:
                count += 1
                if count >= min_stop_word:
                    return [page]

    return set_filter_reason_if_annotate(page, "stop_word_filter"+token, annotate)


def word_length_filter(page: Dict, min_length: int = 0, max_length: int = float('inf'), annotate=False, token="", model='split', language_key='language_id_whole_page_fasttext', **kwargs) -> List[Dict]:
    """
    Filters the input JSON object based on average word length in the CONTENT field.

    This function measures average word length.

    If the length is less than `min_length` or greater than `max_length`, it returns an empty list.
    If the length is greater/equal to `min_length` and less/equal to `max_length`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    min_length -- The minimum length threshold for average word length
    max_length -- The maximum length threshold for average word length

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """

    words = split_words(page[CONTENT], language=get_lang_from_page(page, language_key=language_key), model=model, **kwargs)
    if not words:
        return set_filter_reason_if_annotate(page, "word_length_filter"+token, annotate)

    average_word_length = sum([len(word) for word in words]) / len(words)
    if average_word_length < min_length or average_word_length > max_length:
        return set_filter_reason_if_annotate(page, "word_length_filter"+token, annotate)
    return [page]


def symbol_ratio_filter(page: Dict, max_symbol_to_word_ratio: float = 0.1, annotate=False, token="") -> List[Dict]:
    """
    Filters the input JSON object based on the symbol to word ratio according to the CONTENT field.

    This function measures the symbol to word ratio, where symbols are hash and ellipsis.

    If the symbol to word ratio is greater than `max_symbol_to_word_ratio`, it returns an empty list.
    If the length is less/equal to `max_symbol_to_word_ratio`, it returns a list containing the original JSON object.

    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    max_symbol_to_word_ratio -- The maximum symbol to word ratio

    Returns:
    A list containing the input JSON object if it passes the filter,
    or an empty list if it doesn't.
    """
    
    SYMBOLS = ["#", "...", ". . .", "\u2026"]
    number_of_symbol = sum(page[CONTENT].count(sym) for sym in SYMBOLS)
    number_of_word = len(page[CONTENT].split())
    if number_of_word == 0 or number_of_symbol / number_of_word > max_symbol_to_word_ratio:
        return set_filter_reason_if_annotate(page, "symbol_ratio_filter"+token, annotate)
    return [page]

def word_removal_ratio_filter(page: Dict, prev_word_count_key: str, new_word_count_key: Optional[str] = None,
                              max_removed_ratio: float = 0.05, ignore_punctuation=True, annotate=False, token="", language_key="language_id_whole_page_fasttext", **kwargs) -> Optional[Dict]:
    """
    Filter out pages where the number of words removed by other modifiers is more than percent_removed_threshold (defaults to 5%). Note: this 
    method assumes that you have previously counted the number of words in the document with word_counter_enricher.  

    Arguments:
    page: The page dictionary containing the content and additional metadata.
    prev_word_count_key: The key to use for retrieving the "before modifiers" word count.
    new_word_count_key: The key to use for the "after modifiers" word count. If not provided, it will be calculated within this method. 
    max_removed_ratio: The ratio of words that can be removed without filtering out the page (default 0.05, equivalent to 5%).
    kwargs: Extra arguments passed into split_words such as model and ignore_whitespace. The default for ignore_punctuation is set to be True for more
    accurate word counts.  

    Returns: The same page if <= max_words_removed_ratio of words were removed, otherwise returns None.

    Note:
    - This function assumes that the 'prev_num_words_key' exists in the page dictionary.
    - The 'new_word_count_key' can be optional; if not provided, the current word count is computed.
    """

    # Retrieve the 'before' words count
    assert prev_word_count_key in page, f"'{prev_word_count_key}' key must exist in the page dictionary"
    prev_word_count = page[prev_word_count_key]

    if prev_word_count == 0:
        return set_filter_reason_if_annotate(page, "word_removal_ratio_filter"+token, annotate)

    # Compute or retrieve the 'after' words count
    if new_word_count_key and new_word_count_key in page:
        new_word_count = page[new_word_count_key]
    else:
        new_word_count = len(split_words(page[CONTENT], ignore_punctuation=ignore_punctuation,
                                         language=get_lang_from_page(page, language_key=language_key), **kwargs))

    # Calculate the percentage of words removed
    ratio_removed = (prev_word_count - new_word_count) / prev_word_count

    # Check if more than percent_removed_threshold of words were removed
    if ratio_removed > max_removed_ratio:
        return set_filter_reason_if_annotate(page, "word_removal_ratio_filter"+token, annotate)

    return [page]

def alphabetic_word_ratio_filter(page: Dict, max_ratio: float = 0.2, annotate=False, token="", language_key="language_id_whole_page_fasttext", model="split", **kwargs) -> List[Dict]:
    """
    Filters the input JSON object based on the percentage of words that do not contain
    at least one alphabetic character.
    This function calculates the percentage of words in the CONTENT that contain at least
    one alphabetic character. If this percentage exceeds the provided threshold, the function
    returns an empty list. If the percentage is less than or equal to the threshold, the
    function returns a list containing the original JSON object.
    Non-alphabetic characters include digits, punctuation, spaces, tabs, and newline characters.
    Arguments:
    page -- A dictionary representing a JSON object. It should have a CONTENT field
            that contains the text to be analyzed.
    max_ratio -- The maximum percentage of (fully) non-alphabetic words for which a
                 JSON object is allowed to pass the filter. This should be provided as a
                 float, where 1.0 represents 100%.
    Returns:
    A list containing the input JSON object if it passes the filter, or an empty list if
    it doesn't.
    """
    words = split_words(page[CONTENT], language=get_lang_from_page(page, language_key=language_key), model=model, **kwargs)
    total_words = len(words)

    if total_words == 0:
        return set_filter_reason_if_annotate(page, "alphabetic_word_ratio_filter"+token, annotate) 

    non_alpha_word_count = sum(1 for word in words if not any(char.isalpha() for char in word))
    non_alpha_word_ratio = non_alpha_word_count / total_words
        
    return [page] if non_alpha_word_ratio <= max_ratio else set_filter_reason_if_annotate(page, "alphabetic_word_ratio_filter"+token, annotate)

# ========= Fineweb Custom Filters ==========

def line_punct_ratio_filter(
        page: Dict,
        line_punct_thr: float = 0.12, line_punct_exclude_zero: bool = False,
        stop_chars = None,
        high_quality_ratio_value: float = 0.75,
        high_quality_min_line_num: int = 10,
        annotate=False, token="", model="fineweb", **kwargs) -> List[Dict]:
    lines = page[CONTENT].split("\n")
    lines = [line for line in lines if line.strip() != ""]
    if len(lines) == 0:
        return set_filter_reason_if_annotate(page, "line_punct_ratio_filter"+token, annotate)
    if not stop_chars:
        stop_chars = tuple(TERMINAL_PUNCTUATION)
    ratio = sum(1 for line in lines if line.endswith(stop_chars)) / len(lines)
    if ratio < line_punct_thr and not (ratio == 0 and line_punct_exclude_zero):
        if high_quality_ratio(
                lines,
                model=model,
                high_quality_min_line_num=high_quality_min_line_num) < high_quality_ratio_value:
            return set_filter_reason_if_annotate(page, "line_punct_ratio_filter"+token, annotate)
    return [page]


def check_line_word_num(words, min_word_num: int = 3):
    return len(words) >= min_word_num


def high_quality_ratio(lines, model, high_quality_min_line_num):
    if len(lines) == 0:
        return 0
    high_quality_num = 0
    all_quality_num = sum([len(_line) for _line in lines])
    for line in lines:
        try:
            words = split_words(line, model=model)
        except Exception:
            continue
        if check_line_word_num(words,min_word_num=high_quality_min_line_num):
            high_quality_num+=len(line)
    return high_quality_num/all_quality_num        


def short_line_ratio_filter(
        page: Dict,
        short_line_thr: float = 0.67,
        short_line_length: int = 30,
        annotate=False, token="", **kwargs) -> List[Dict]:
    lines = page[CONTENT].split("\n")
    lines = [line for line in lines if line.strip() != ""]
    if len(lines) == 0:
        return set_filter_reason_if_annotate(page, "short_line_ratio_filter"+token, annotate)    
    ratio = sum(1 for line in lines if len(line) <= short_line_length) / len(lines)
    if ratio > short_line_thr:
        return set_filter_reason_if_annotate(page, "short_line_ratio_filter"+token, annotate)
    return [page]


def char_dup_ratio_filter(
        page: Dict,
        char_duplicates_ratio: float = 0.1,
        annotate=False, token="", **kwargs) -> List[Dict]:
    lines = page[CONTENT].split("\n")
    lines = [line for line in lines if line.strip() != ""]
    if len(lines) == 0:
        return set_filter_reason_if_annotate(page, "char_dup_ratio_filter"+token, annotate)

    ratio = find_duplicates(lines)[1] / len(page[CONTENT].replace("\n", ""))

    if ratio > char_duplicates_ratio:
        return set_filter_reason_if_annotate(page, "char_dup_ratio_filter"+token, annotate)
    return [page]


def list_ratio_filter(
        page: Dict,
        new_line_ratio: float = 0.3,
        annotate=False, token="", model="fineweb", **kwargs) -> List[Dict]:
    lines = page[CONTENT].split("\n")
    lines = [line for line in lines if line.strip() != ""]
    if len(lines) == 0:
        return set_filter_reason_if_annotate(page, "list_ratio_filter"+token, annotate)

    words = split_words(page[CONTENT], model=model)
    new_line = page[CONTENT].count("\n")
    if new_line / len(words) > new_line_ratio:
        return set_filter_reason_if_annotate(page, "list_ratio_filter"+token, annotate)
    return [page]
    

"""
fineweb gopher repetition
"""

def fineweb_gopher_repetition_filter(
        page: Dict,
        dup_line_frac: float | None = 0.3,
        dup_para_frac: float | None = 0.3,
        dup_line_char_frac: float | None = 0.2,
        dup_para_char_frac: float | None = 0.2,
        top_n_grams = ((2, 0.2), (3, 0.18), (4, 0.16)),
        dup_n_grams = ((5, 0.15), (6, 0.14), (7, 0.13), (8, 0.12), (9, 0.11), (10, 0.10)),
        language_key: str = 'language_id_whole_page_fasttext',
        annotate=False,
) -> List[Dict]:
    language = get_lang_from_page(page, language_key)
    text = page[CONTENT]
    paragraph_exp = re.compile(r"\n{2,}")

    try:
        paragraphs = paragraph_exp.split(text.strip())
    except Exception:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:split_failed", annotate)

    if len(text) == 0:
        print("--------1", text)    
    paragraphs_duplicates, char_duplicates = find_duplicates(paragraphs)
    if dup_para_frac and paragraphs_duplicates / len(paragraphs) > dup_para_frac:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:paragraph", annotate)
    if dup_para_char_frac and char_duplicates / len(text) > dup_para_char_frac:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:paragraph_char", annotate)

    line_splitter = re.compile("\n+")
    lines = line_splitter.split(text)
        
    line_duplicates, char_duplicates = find_duplicates(lines)
    if dup_line_frac and line_duplicates / len(lines) > dup_line_frac:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:line", annotate)
    if dup_line_char_frac and char_duplicates / len(text) > dup_line_char_frac:
        return set_filter_reason_if_annotate(page, "massive_web_repetition_filters:line_char", annotate)

    try:
        words = split_words(text, model='fineweb', language=language)
    except Exception:
        if len(text) > 1000:
            return [page]
        return set_filter_reason_if_annotate(page, "word_split_falied", annotate)

    for n, n_frac in top_n_grams:
        n_grams = get_n_grams(words, n)
        if not n_grams:
            continue
        top_char_length = find_top_duplicate(n_grams)
        if top_char_length / len(text) > n_frac:
            return set_filter_reason_if_annotate(page, f"massive_web_repetition_filters:{n}_gram", annotate)

    for n, n_frac in dup_n_grams:
        n_duplicates_char = find_all_duplicate(words, n)
        if n_duplicates_char / len(text) > n_frac:
            return set_filter_reason_if_annotate(page, f"massive_web_repetition_filters:{n}_gram", annotate)

    return [page]

def get_n_grams(words: list[str], n: int) -> list[str]:
    return [" ".join(words[i : i + n]) for i in range(len(words) - n + 1)]


def find_duplicates(x: list[str]) -> tuple[int, int]:
    unique_x = set()
    duplicate_chars = 0
    duplicate_elements = 0
    for element in x:
        if element in unique_x:
            duplicate_chars += len(element)
            duplicate_elements += 1

        else:
            unique_x.add(element)
    return duplicate_elements, duplicate_chars


def find_top_duplicate(x: list[str]) -> int:
    counter = Counter()
    for element in x:
        counter[element] += 1
    top_n_gram = counter.most_common(1)[0]
    return len(top_n_gram[0]) * top_n_gram[1]


def find_all_duplicate(words: list[str], n: int) -> int:
    n_words = len(words)
    unique = set()
    repeated_chars, idx = 0, 0
    while idx < n_words - n + 1:
        n_gram = "".join(words[idx : idx + n])
        if n_gram in unique:
            repeated_chars += len(n_gram)
            idx += n
        else:
            unique.add(n_gram)
            idx += 1
    assert repeated_chars <= len("".join(words))
    return repeated_chars

STOP_WORDS = ["the", "be", "to", "of", "and", "that", "have", "with"]

def fineweb_gopher_quality_filter(
        page: Dict,
        min_doc_words: int = 50,
        max_doc_words: int = 100000,
        min_avg_word_length: int = 3,
        max_avg_word_length: int | None = 10,
        max_symbol_word_ratio: float | None = 0.1,
        max_bullet_lines_ratio: float | None = 0.9,
        max_ellipsis_lines_ratio: float | None = 0.3,
        max_non_alpha_words_ratio: float | None = 0.8,
        min_stop_words: int | None = 2,
        whitelist_chars=('(', ')', '%'),
        use_whitelist = False,
        annotate=False,
        language_key='',
) -> List[Dict]:
        text = page[CONTENT]
        language = get_lang_from_page(page, language_key)
        stop_words = set(STOP_WORDS)
        try:
            words = split_words(text, model='fineweb', language=language)
        except Exception:
            if len(text) > 1000:
                return [page]
            return []

        n_words = len(words)

        non_symbol_words = [w for w in words if any(ch not in PUNCTUATION_SET for ch in w)]
        n_non_symbol_words_words = len(non_symbol_words)

        # words < min_doc_words or words > max_doc_words
        if min_doc_words and n_non_symbol_words_words < min_doc_words:
            return set_filter_reason_if_annotate(page, "gopher_short_doc", annotate)
        if max_doc_words and n_non_symbol_words_words > max_doc_words:
            return set_filter_reason_if_annotate(page, "gopher_long_doc", annotate)

        # mean word length is outside the range of 3 to 10 characters
        avg_n_words = np.mean([len(w) for w in non_symbol_words])
        if min_avg_word_length and avg_n_words < min_avg_word_length:
            return set_filter_reason_if_annotate(page, "gopher_below_avg_threshold", annotate)
        if max_avg_word_length and avg_n_words > max_avg_word_length:
            return set_filter_reason_if_annotate(page, "gopher_above_avg_threshold", annotate)

        # symbol-to-word ratio greater than 0.1 for either the hash symbol or the ellipsis
        if max_symbol_word_ratio and text.count("#") / n_words > max_symbol_word_ratio:
            return set_filter_reason_if_annotate(page, "gopher_too_many_hashes", annotate)
        if max_symbol_word_ratio and (text.count("...") + text.count("…")) / n_words > max_symbol_word_ratio:
            return set_filter_reason_if_annotate(page, "gopher_too_many_ellipsis", annotate)

        # any document with more than 90 % of lines starting with a bullet point,
        # or more than 30 % ending with an ellipsis.
        lines = text.splitlines()
        if (
            max_bullet_lines_ratio
            and sum(s.lstrip().startswith("•") or s.lstrip().startswith("-") for s in lines) / len(lines)
            > max_bullet_lines_ratio
        ):
            return set_filter_reason_if_annotate(page, "gopher_too_many_bullets", annotate)
        if (
            max_ellipsis_lines_ratio
            and sum(s.rstrip().endswith("...") or s.rstrip().endswith("…") for s in lines) / len(lines)
            > max_ellipsis_lines_ratio
        ):
            return set_filter_reason_if_annotate(page, "gopher_too_many_end_ellipsis", annotate)

        # that 80 % of words in a document contain at least one alphabetic character
        if (
            max_non_alpha_words_ratio
            # nb of words with at least 1 alpha char < 0.8
            and not check_non_alpha_ratio(words,
                                          max_non_alpha_words_ratio=max_non_alpha_words_ratio,
                                          whitelist_chars=whitelist_chars,
                                          use_whitelist=use_whitelist)):
            return set_filter_reason_if_annotate(page, "gopher_below_alpha_threshold", annotate)

        # stop word filter
        if min_stop_words and sum(w in stop_words for w in words) < min_stop_words:
            return set_filter_reason_if_annotate(page, "gopher_enough_stop_words", annotate)

        return [page]
    

def in_non_alpha_whitelist(w, whitelist_chars = ()):
    return w.isdigit() or w in whitelist_chars


def check_non_alpha_ratio(words,
                          max_non_alpha_words_ratio,
                          whitelist_chars,
                          use_whitelist):
    n_words = len(words)

    # that 80 % of words in a document contain at least one alphabetic character
    if (sum([any((c.isalpha() for c in w)) or (use_whitelist and in_non_alpha_whitelist(w, whitelist_chars)) for w in words]) / n_words < max_non_alpha_words_ratio
    ):
        return False
    return True    
