import re
import nltk
import string
from typing import Iterable
from collections import Counter, defaultdict

import numpy as np
import scipy.stats as st
from py3langid.langid import LanguageIdentifier
from py3langid.langid import MODEL_FILE as LANGID_MODEL_FILE

from type_infer.dtype import dtype


try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

try:
    from nltk.corpus import stopwords
    stopwords.words('english')
except LookupError:
    nltk.download('stopwords', quiet=True)


def get_identifier_description_mp(arg_tup):
    data, column_name, data_dtype = arg_tup
    return get_identifier_description(data, column_name, data_dtype)


def get_identifier_description(data: Iterable, column_name: str, data_dtype: dtype):
    data = list(data)
    if isinstance(data[0], list):
        nr_unique = len(set(tuple(x) for x in data))
    elif isinstance(data[0], dict):
        nr_unique = len(set(str(x) for x in data))
    else:
        nr_unique = len(set(data))

    if nr_unique == 1:
        return 'No Information'

    unique_pct = nr_unique / len(data)

    spaces = [len(str(x).split(' ')) - 1 for x in data]
    mean_spaces = np.mean(spaces) if len(spaces) > 0 else 0.0

    # Detect hash
    all_same_length = all(len(str(data[0])) == len(str(x)) for x in data)
    uuid_charset = set('0123456789abcdefABCDEF-')
    all_uuid_charset = all(set(str(x)).issubset(uuid_charset) for x in data)
    is_uuid = all_uuid_charset and all_same_length

    if all_same_length and len(data) == nr_unique and data_dtype not in (dtype.integer, dtype.float):
        str_data = [str(x) for x in data]
        randomness_per_index = []
        for i, _ in enumerate(str_data[0]):
            N = len(set(x[i] for x in str_data))
            S = st.entropy([*Counter(x[i] for x in str_data).values()])
            if S == 0:
                randomness_per_index.append(0.0)
            else:
                randomness_per_index.append(S / np.log(N))

        mean_randomness = np.mean(randomness_per_index) if len(randomness_per_index) > 0 else 0
        if mean_randomness > 0.95:
            return 'Hash-like identifier'

    # Detect foreign key
    if data_dtype == dtype.integer:
        if _is_foreign_key_name(column_name):
            return 'Foreign key'

    if _is_identifier_name(column_name) or data_dtype in (dtype.categorical, dtype.binary):
        if unique_pct > 0.98:
            if is_uuid:
                return 'UUID'
            else:
                return 'Unknown identifier'

    # Everything is unique and it's too short to be rich text
    if data_dtype in (dtype.categorical, dtype.binary, dtype.short_text, dtype.rich_text) and \
            unique_pct > 0.99999 and mean_spaces < 1:
        return 'Unknown identifier'

    return None


def _is_foreign_key_name(name):
    for endings in ['id', 'ID', 'Id']:
        for add in ['-', '_', ' ']:
            if name.endswith(add + endings):
                return True
    for endings in ['ID', 'Id']:
        if name.endswith(endings):
            return True
    return False


def _is_identifier_name(name):
    for keyword in ['account', 'uuid', 'identifier', 'user']:
        if keyword in name:
            return True
    return False


def get_language_dist(data):
    lang_dist = defaultdict(lambda: 0)
    lang_dist['Unknown'] = 0
    lang_probs_cache = dict()
    identifier = LanguageIdentifier.from_pickled_model(LANGID_MODEL_FILE, norm_probs=True)
    for text in data:
        text = str(text)
        text = text.translate(str.maketrans('', '', string.punctuation))
        if text not in lang_probs_cache:
            try:
                lang_probs = identifier.classify(text)
            except Exception:
                lang_probs = []
            lang_probs_cache[text] = lang_probs

        lang_probs = lang_probs_cache[text]
        if len(lang_probs) > 0 and lang_probs[1] > 10 * (1 / len(identifier.nb_classes)):
            lang_dist[lang_probs[0]] += 1
        else:
            lang_dist['Unknown'] += 1

    return dict(lang_dist)


def analyze_sentences(data):
    nr_words = 0
    word_dist = defaultdict(int)
    nr_words_dist = defaultdict(int)
    stop_words = set(stopwords.words('english'))
    for text in map(str, data):
        text = text.lower()
        text_dist = defaultdict(int)
        tokens = tokenize_text(text)
        tokens_no_stop = (x for x in tokens if x not in stop_words)
        for tok in tokens_no_stop:
            text_dist[tok] += 1

        n_tokens = len(text_dist)
        nr_words_dist[n_tokens] += 1
        nr_words += n_tokens

        # merge text_dist into word_dist
        for k, v in text_dist.items():
            word_dist[k] += v

    return nr_words, dict(word_dist), dict(nr_words_dist)


def contains_alnum(text):
    for c in text:
        if c.isalnum():
            return True
    return False


def tokenize_text(text):
    """ Generator instead of list comprehension for optimal memory usage & runtime """
    return (t.lower() for t in nltk.word_tokenize(decontracted(text)) if contains_alnum(t))


def decontracted(phrase):
    # specific
    phrase = re.sub(r"won\'t", "will not", phrase)
    phrase = re.sub(r"can\'t", "can not", phrase)

    # general
    phrase = re.sub(r"n\'t", " not", phrase)
    phrase = re.sub(r"\'re", " are", phrase)
    phrase = re.sub(r"\'s", " is", phrase)
    phrase = re.sub(r"\'d", " would", phrase)
    phrase = re.sub(r"\'ll", " will", phrase)
    phrase = re.sub(r"\'t", " not", phrase)
    phrase = re.sub(r"\'ve", " have", phrase)
    phrase = re.sub(r"\'m", " am", phrase)
    return phrase
