from walrus.query import Executor
from walrus.query import OP_MATCH
from walrus.query import parse
from walrus.utils import decode
from walrus.utils import decode_dict
from walrus.search import Tokenizer


class Index(object):
    """
    Full-text search index.

    Store documents, along with arbitrary metadata, and perform full-text
    search on the document content. Supports porter-stemming, stopword
    filtering, basic result ranking, and (optionally) double-metaphone for
    phonetic search.
    """
    def __init__(self, db, name, **tokenizer_settings):
        """
        :param Database db: a walrus database object.
        :param str name: name for the search index.
        :param bool stemmer: use porter stemmer (default True).
        :param bool metaphone: use double metaphone (default False).
        :param str stopwords_file: defaults to walrus stopwords.txt.
        :param int min_word_length: specify minimum word length.

        Create a search index for storing and searching documents.
        """
        self.db = db
        self.name = name
        self.tokenizer = Tokenizer(**tokenizer_settings)
        self.members = self.db.Set('fts.%s' % self.name)

    def get_key(self, word):
        return self.db.ZSet('fts.%s.%s' % (self.name, word))

    def _get_hash(self, document_id):
        return self.db.Hash('doc.%s.%s' % (self.name, decode(document_id)))

    def get_document(self, document_id):
        """
        :param document_id: Document unique identifier.
        :returns: a dictionary containing the document content and
                  any associated metadata.
        """
        key = 'doc.%s.%s' % (self.name, decode(document_id))
        return decode_dict(self.db.hgetall(key))

    def add(self, key, content, __metadata=None, **metadata):
        """
        :param key: Document unique identifier.
        :param str content: Content to store and index for search.
        :param metadata: Arbitrary key/value pairs to store for document.

        Add a document to the search index.
        """
        if __metadata is None:
            __metadata = metadata
        elif metadata:
            __metadata.update(metadata)

        if not isinstance(key, str):
            key = str(key)

        self.members.add(key)
        document_hash = self._get_hash(key)
        document_hash.update(__metadata, content=content)

        for word, score in self.tokenizer.tokenize(content).items():
            word_key = self.get_key(word)
            word_key[key] = -score

    def remove(self, key, preserve_data=False):
        """
        :param key: Document unique identifier.

        Remove the document from the search index.
        """
        if not isinstance(key, str):
            key = str(key)

        if self.members.remove(key) != 1:
            raise KeyError('Document with key "%s" not found.' % key)
        document_hash = self._get_hash(key)
        content = decode(document_hash['content'])
        if not preserve_data:
            document_hash.clear()

        for word in self.tokenizer.tokenize(content):
            word_key = self.get_key(word)
            del word_key[key]
            if len(word_key) == 0:
                word_key.clear()

    def update(self, key, content, __metadata=None, **metadata):
        """
        :param key: Document unique identifier.
        :param str content: Content to store and index for search.
        :param metadata: Arbitrary key/value pairs to store for document.

        Update the given document. Existing metadata will be preserved and,
        optionally, updated with the provided metadata.
        """
        self.remove(key, preserve_data=True)
        self.add(key, content, __metadata, **metadata)

    def replace(self, key, content, __metadata=None, **metadata):
        """
        :param key: Document unique identifier.
        :param str content: Content to store and index for search.
        :param metadata: Arbitrary key/value pairs to store for document.

        Update the given document. Existing metadata will not be removed and
        replaced with the provided metadata.
        """
        self.remove(key)
        self.add(key, content, __metadata, **metadata)

    def get_index(self, op):
        assert op == OP_MATCH
        return self

    def db_value(self, value):
        return value

    def _search(self, query):
        expression = parse(query, self)
        if expression is None:
            return [(member, 0) for member in self.members]
        executor = Executor(self.db)
        return executor.execute(expression)

    def search(self, query):
        """
        :param str query: Search query. May contain boolean/set operations
            and parentheses.
        :returns: a list of document hashes corresponding to matching
            documents.

        Search the index. The return value is a list of dictionaries
        corresponding to the documents that matched. These dictionaries contain
        a ``content`` key with the original indexed content, along with any
        additional metadata that was specified.
        """
        return [self.get_document(key) for key, _ in self._search(query)]

    def search_items(self, query):
        """
        :param str query: Search query. May contain boolean/set operations
            and parentheses.
        :returns: a list of (key, document hashes) tuples corresponding to
            matching documents.

        Search the index. The return value is a list of (key, document dict)
        corresponding to the documents that matched. These dictionaries contain
        a ``content`` key with the original indexed content, along with any
        additional metadata that was specified.
        """
        return [(decode(key), self.get_document(key))
                for key, _ in self._search(query)]
