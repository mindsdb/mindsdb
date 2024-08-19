import json
import re

from walrus.utils import decode
from walrus.utils import encode
from walrus.utils import load_stopwords
from walrus.utils import PY3


class Autocomplete(object):
    """
    Autocompletion for ascii-encoded string data. Titles are stored,
    along with any corollary data in Redis. Substrings of the title
    are stored in sorted sets using a unique scoring algorithm. The
    scoring algorithm aims to return results in a sensible order,
    by looking at the entire title and the position of the matched
    substring within the title.

    Additionally, the autocomplete object supports boosting search
    results by object ID or object type.
    """
    def __init__(self, database, namespace='walrus', cache_timeout=600,
                 stopwords_file='stopwords.txt', use_json=True):
        """
        :param database: A :py:class:`Database` instance.
        :param namespace: Namespace to prefix keys used to store
            metadata.
        :param cache_timeout: Complex searches using boosts will be
            cached. Specify the amount of time these results are
            cached for.
        :param stopwords_file: Filename containing newline-separated
            stopwords. Set to `None` to disable stopwords filtering.
        :param bool use_json: Whether object data should be
            serialized as JSON.
        """
        self.database = database
        self.namespace = namespace
        self._cache_timeout = cache_timeout
        self._stopwords_file = stopwords_file
        self._use_json = use_json
        self._load_stopwords()

        self._data = self.database.Hash('%s:d' % self.namespace)
        self._title_data = self.database.Hash('%s:t' % self.namespace)
        self._boosts = self.database.Hash('%s:b' % self.namespace)

        self._max_title = 10
        self._offset = self.score_token('z' * self._max_title) + 1

    def _load_stopwords(self):
        if self._stopwords_file:
            stopwords = load_stopwords(self._stopwords_file)
            self._stopwords = set(stopwords.splitlines())
        else:
            self._stopwords = set()

    def tokenize_title(self, phrase, stopwords=True):
        if isinstance(phrase, bytes):
            phrase = decode(phrase)
        phrase = re.sub('[^a-z0-9_\-\s]', '', phrase.lower())
        if stopwords and self._stopwords:
            return [w for w in phrase.split() if w not in self._stopwords]
        else:
            return phrase.split()

    def score_token(self, token):
        l = len(token)
        a = ord('a') - 2
        score = 0

        for i in range(self._max_title):
            if i < l:
                c = ord(token[i]) - a
                if c < 2 or c > 27:
                    c = 1
            else:
                c = 1
            score += c * (27 ** (self._max_title - i - 1))

        return score

    def substrings(self, w):
        for i in range(1, len(w)):
            yield w[:i]
        yield w

    def object_key(self, obj_id, obj_type):
        return '%s\x01%s' % (obj_id, obj_type or '')

    def word_key(self, word):
        return '%s:s:%s' % (self.namespace, word)

    def store(self, obj_id, title=None, data=None, obj_type=None):
        """
        Store data in the autocomplete index.

        :param obj_id: Either a unique identifier for the object
            being indexed or the word/phrase to be indexed.
        :param title: The word or phrase to be indexed. If not
            provided, the ``obj_id`` will be used as the title.
        :param data: Arbitrary data to index, which will be
            returned when searching for results. If not provided,
            this value will default to the title being indexed.
        :param obj_type: Optional object type. Since results can be
            boosted by type, you might find it useful to specify this
            when storing multiple types of objects.

        You have the option of storing several types of data as
        defined by the parameters. At the minimum, you can specify
        an ``obj_id``, which will be the word or phrase you wish to
        index. Alternatively, if for instance you were indexing blog
        posts, you might specify all parameters.
        """
        if title is None:
            title = obj_id
        if data is None:
            data = title
        obj_type = obj_type or ''

        if self._use_json:
            data = json.dumps(data)

        combined_id = self.object_key(obj_id, obj_type)

        if self.exists(obj_id, obj_type):
            stored_title = self._title_data[combined_id]
            if stored_title == title:
                self._data[combined_id] = data
                return
            else:
                self.remove(obj_id, obj_type)

        self._data[combined_id] = data
        self._title_data[combined_id] = title

        clean_title = ' '.join(self.tokenize_title(title))
        title_score = self.score_token(clean_title)

        for idx, word in enumerate(self.tokenize_title(title)):
            word_score = self.score_token(word)
            position_score = word_score + (self._offset * idx)
            key_score = position_score + title_score
            for substring in self.substrings(word):
                self.database.zadd(self.word_key(substring),
                                   {combined_id: key_score})

        return True

    def remove(self, obj_id, obj_type=None):
        """
        Remove an object identified by the given ``obj_id`` (and
        optionally ``obj_type``) from the search index.

        :param obj_id: The object's unique identifier.
        :param obj_type: The object's type.
        """
        if not self.exists(obj_id, obj_type):
            raise KeyError('Object not found.')

        combined_id = self.object_key(obj_id, obj_type)
        title = self._title_data[combined_id]

        for word in self.tokenize_title(title):
            for substring in self.substrings(word):
                key = self.word_key(substring)
                if not self.database.zrange(key, 1, 2):
                    self.database.delete(key)
                else:
                    self.database.zrem(key, combined_id)

        del self._data[combined_id]
        del self._title_data[combined_id]
        del self._boosts[combined_id]

    def exists(self, obj_id, obj_type=None):
        """
        Return whether the given object exists in the search index.

        :param obj_id: The object's unique identifier.
        :param obj_type: The object's type.
        """
        return self.object_key(obj_id, obj_type) in self._data

    def boost_object(self, obj_id=None, obj_type=None, multiplier=1.1,
                     relative=True):
        """
        Boost search results for the given object or type by the
        amount specified. When the ``multiplier`` is greater than
        1, the results will percolate to the top. Values between
        0 and 1 will percolate results to the bottom.

        Either an ``obj_id`` or ``obj_type`` (or both) must be
        specified.

        :param obj_id: An object's unique identifier (optional).
        :param obj_type: The object's type (optional).
        :param multiplier: A positive floating-point number.
        :param relative: If ``True``, then any pre-existing saved
            boost will be updated using the given multiplier.

        Examples:

        .. code-block:: python

            # Make all objects of type=photos percolate to top.
            ac.boost_object(obj_type='photo', multiplier=2.0)

            # Boost a particularly popular blog entry.
            ac.boost_object(
                popular_entry.id,
                'entry',
                multipler=5.0,
                relative=False)
        """
        combined_id = self.object_key(obj_id or '', obj_type or '')
        if relative:
            current = float(self._boosts[combined_id] or 1.0)
            self._boosts[combined_id] = current * multiplier
        else:
            self._boosts[combined_id] = multiplier

    def _load_objects(self, obj_id_zset, limit, chunk_size=1000):
        ct = i = 0
        while True:
            id_chunk = obj_id_zset[i:i + chunk_size]
            if not id_chunk:
                return

            i += chunk_size
            for raw_data in self._data[id_chunk]:
                if not raw_data:
                    continue
                if self._use_json:
                    yield json.loads(decode(raw_data))
                else:
                    yield raw_data
                ct += 1
                if limit and ct == limit:
                    return

    def _load_saved_boosts(self):
        boosts = {}
        for combined_id, score in self._boosts:
            obj_id, obj_type = combined_id.split(encode('\x01'), 1)
            score = float(score)
            if obj_id and obj_type:
                boosts[combined_id] = score
            elif obj_id:
                boosts[obj_id] = score
            elif obj_type:
                boosts[obj_type] = score
        return boosts

    def search(self, phrase, limit=None, boosts=None, chunk_size=1000):
        """
        Perform a search for the given phrase. Objects whose title
        matches the search will be returned. The values returned
        will be whatever you specified as the ``data`` parameter
        when you called :py:meth:`~Autocomplete.store`.

        :param phrase: One or more words or substrings.
        :param int limit: Limit size of the result set.
        :param dict boosts: A mapping of object id/object type to
            floating point multipliers.
        :returns: A list containing the object data for objects
            matching the search phrase.
        """
        cleaned = self.tokenize_title(phrase, stopwords=False)

        # Remove stopwords except for the last token, which may be a partially
        # typed string that just happens to match a stopword.
        last_token = len(cleaned) - 1
        cleaned = [c for i, c in enumerate(cleaned)
                   if (c not in self._stopwords) or (i == last_token)]

        if not cleaned:
            return

        all_boosts = self._load_saved_boosts()
        if PY3 and boosts:
            for key in boosts:
                all_boosts[encode(key)] = boosts[key]
        elif boosts:
            all_boosts.update(boosts)

        if len(cleaned) == 1 and not all_boosts:
            result_key = self.word_key(cleaned[0])
        else:
            result_key = self.get_cache_key(cleaned, all_boosts)
            if result_key not in self.database:
                self.database.zinterstore(
                    result_key,
                    list(map(self.word_key, cleaned)))
            self.database.expire(result_key, self._cache_timeout)

        results = self.database.ZSet(result_key)
        if all_boosts:
            for raw_id, score in results[0:0, True]:
                orig_score = score
                for identifier in raw_id.split(encode('\x01'), 1):
                    if identifier and identifier in all_boosts:
                        score *= 1 / all_boosts[identifier]

                if orig_score != score:
                    results[raw_id] = score

        for result in self._load_objects(results, limit, chunk_size):
            yield result

    def get_cache_key(self, phrases, boosts):
        if boosts:
            boost_key = '|'.join(sorted(
                '%s:%s' % (k, v) for k, v in boosts.items()))
        else:
            boost_key = ''
        phrase_key = '|'.join(phrases)
        return '%s:c:%s:%s' % (self.namespace, phrase_key, boost_key)

    def list_data(self):
        """
        Return all the data stored in the autocomplete index. If the data was
        stored as serialized JSON, then it will be de-serialized before being
        returned.

        :rtype: list
        """
        fn = (lambda v: json.loads(decode(v))) if self._use_json else decode
        return map(fn, self._data.values())

    def list_titles(self):
        """
        Return the titles of all objects stored in the autocomplete index.

        :rtype: list
        """
        return map(decode, self._title_data.values())

    def flush(self, batch_size=1000):
        """
        Delete all autocomplete indexes and metadata.
        """
        keys = self.database.keys(self.namespace + ':*')
        for i in range(0, len(keys), batch_size):
            self.database.delete(*keys[i:i + batch_size])
