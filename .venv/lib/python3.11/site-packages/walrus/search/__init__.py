import re

from walrus.search.metaphone import dm as double_metaphone
from walrus.search.porter import PorterStemmer
from walrus.utils import decode
from walrus.utils import load_stopwords


class Tokenizer(object):
    def __init__(self, stemmer=True, metaphone=False,
                 stopwords_file='stopwords.txt', min_word_length=None):
        self._use_stemmer = stemmer
        self._use_metaphone = metaphone
        self._min_word_length = min_word_length
        self._symbols_re = re.compile(
            '[\.,;:"\'\\/!@#\$%\?\*\(\)\-=+\[\]\{\}_]')

        self._stopwords = self._load_stopwords(stopwords_file)

    def _load_stopwords(self, filename):
        stopwords = load_stopwords(filename)
        if stopwords:
            return set(stopwords.splitlines())

    def split_phrase(self, phrase):
        """Split the document or search query into tokens."""
        return self._symbols_re.sub(' ', phrase).split()

    def stem(self, words):
        """
        Use the porter stemmer to generate consistent forms of
        words, e.g.::

            from walrus.search.utils import PorterStemmer
            stemmer = PorterStemmer()
            for word in ['faith', 'faiths', 'faithful']:
                print s.stem(word, 0, len(word) - 1)

            # Prints:
            # faith
            # faith
            # faith
        """
        stemmer = PorterStemmer()
        _stem = stemmer.stem
        for word in words:
            yield _stem(word, 0, len(word) - 1)

    def metaphone(self, words):
        """
        Apply the double metaphone algorithm to the given words.
        Using metaphone allows the search index to tolerate
        misspellings and small typos.

        Example::

            >>> from walrus.search.metaphone import dm as metaphone
            >>> print metaphone('walrus')
            ('ALRS', 'FLRS')

            >>> print metaphone('python')
            ('P0N', 'PTN')

            >>> print metaphone('pithonn')
            ('P0N', 'PTN')
        """
        for word in words:
            r = 0
            for w in double_metaphone(word):
                if w:
                    w = w.strip()
                    if w:
                        r += 1
                        yield w
            if not r:
                yield word

    def tokenize(self, value):
        """
        Split the incoming value into tokens and process each token,
        optionally stemming or running metaphone.

        :returns: A ``dict`` mapping token to score. The score is
            based on the relative frequency of the word in the
            document.
        """
        words = self.split_phrase(decode(value).lower())
        if self._stopwords:
            words = [w for w in words if w not in self._stopwords]
        if self._min_word_length:
            words = [w for w in words if len(w) >= self._min_word_length]

        fraction = 1. / (len(words) + 1)  # Prevent division by zero.

        # Apply optional transformations.
        if self._use_stemmer:
            words = self.stem(words)
        if self._use_metaphone:
            words = self.metaphone(words)

        scores = {}
        for word in words:
            scores.setdefault(word, 0)
            scores[word] += fraction
        return scores
