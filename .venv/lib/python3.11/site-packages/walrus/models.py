from copy import deepcopy
import datetime
import json
import pickle
import re
import sys
import time
import uuid
from warnings import warn

from walrus.containers import Array
from walrus.containers import Hash
from walrus.containers import HyperLogLog
from walrus.containers import List
from walrus.containers import Set
from walrus.containers import ZSet
from walrus.query import ABSOLUTE
from walrus.query import CONTINUOUS
from walrus.query import Desc
from walrus.query import Executor
from walrus.query import FTS
from walrus.query import Node
from walrus.search import Tokenizer
from walrus.utils import basestring_type
from walrus.utils import decode
from walrus.utils import decode_dict_keys
from walrus.utils import encode
from walrus.utils import PY3
from walrus.utils import unicode_type


class Field(Node):
    """
    Named attribute on a model that will hold a value of the given
    type. Fields are declared as attributes on a model class.

    Example::

        walrus_db = Database()

        class User(Model):
            __database__ = walrus_db
            __namespace__ = 'my-app'

            # Use the user's email address as the primary key.
            # All primary key fields will also get a secondary
            # index, so there's no need to specify index=True.
            email = TextField(primary_key=True)

            # Store the user's interests in a free-form text
            # field. Also create a secondary full-text search
            # index on this field.
            interests = TextField(
                fts=True,
                stemmer=True,
                min_word_length=3)

        class Note(Model):
            __database__ = walrus_app
            __namespace__ = 'my-app'

            # A note is associated with a user. We will create a
            # secondary index on this field so we can efficiently
            # retrieve all notes created by a specific user.
            user_email = TextField(index=True)

            # Store the note content in a searchable text field. Use
            # the double-metaphone algorithm to index the content.
            content = TextField(
                fts=True,
                stemmer=True,
                metaphone=True)

            # Store the timestamp the note was created automatically.
            # Note that we do not call `now()`, but rather pass the
            # function itself.
            timestamp = DateTimeField(default=datetime.datetime.now)
    """
    _coerce = None

    def __init__(self, index=False, primary_key=False, default=None):
        """
        :param bool index: Use this field as an index. Indexed
            fields will support :py:meth:`Model.get` lookups.
        :param bool primary_key: Use this field as the primary key.
        """
        self._index = index or primary_key
        self._primary_key = primary_key
        self._default = default

    def _generate_key(self):
        raise NotImplementedError

    def db_value(self, value):
        if self._coerce:
            return self._coerce(value)
        return value

    def python_value(self, value):
        if self._coerce:
            return self._coerce(value)
        return value

    def add_to_class(self, model_class, name):
        self.model_class = model_class
        self.name = name
        setattr(model_class, name, self)

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return instance._data.get(self.name)
        return self

    def __set__(self, instance, value):
        instance._data[self.name] = value

    def get_index(self, op):
        indexes = self.get_indexes()
        for index in indexes:
            if op in index.operations:
                return index

        raise ValueError('Operation %s is not supported by an index.' % op)

    def get_indexes(self):
        """
        Return a list of secondary indexes to create for the
        field. For instance, a TextField might have a full-text
        search index, whereas an IntegerField would have a scalar
        index that supported range queries.
        """
        return [AbsoluteIndex(self)]


class _ScalarField(Field):
    def get_indexes(self):
        return [AbsoluteIndex(self), ContinuousIndex(self)]


class IntegerField(_ScalarField):
    """Store integer values."""
    _coerce = int

    def db_value(self, value):
        return 0 if value is None else int(value)


class AutoIncrementField(IntegerField):
    """Auto-incrementing primary key field."""
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        return super(AutoIncrementField, self).__init__(*args, **kwargs)

    def _generate_key(self):
        query_helper = self.model_class._query
        key = query_helper.make_key(self.name, '_sequence')
        return self.model_class.__database__.incr(key)


class FloatField(_ScalarField):
    """Store floating point values."""
    _coerce = float

    def db_value(self, value):
        return 0. if value is None else float(value)


class ByteField(Field):
    """Store arbitrary bytes."""
    def db_value(self, value):
        if isinstance(value, unicode_type):
            value = value.encode('utf-8')
        elif value is None:
            value = b''
        return value


class TextField(Field):
    """
    Store unicode strings, encoded as UTF-8. :py:class:`TextField`
    also supports full-text search through the optional ``fts``
    parameter.

    .. note:: If full-text search is enabled for the field, then
        the ``index`` argument is implied.

    :param bool fts: Enable simple full-text search.
    :param bool stemmer: Use porter stemmer to process words.
    :param bool metaphone: Use the double metaphone algorithm to
        process words.
    :param str stopwords_file: File containing stopwords, one per
        line. If not specified, the default stopwords will be used.
    :param int min_word_length: Minimum length (inclusive) of word
        to be included in search index.
    """
    def __init__(self, fts=False, stemmer=True, metaphone=False,
                 stopwords_file=None, min_word_length=None, *args, **kwargs):
        super(TextField, self).__init__(*args, **kwargs)
        self._fts = fts
        self._stemmer = stemmer
        self._metaphone = metaphone
        self._stopwords_file = stopwords_file
        self._min_word_length = min_word_length
        self._index = self._index or self._fts

    def db_value(self, value):
        return b'' if value is None else encode(value)

    def python_value(self, value):
        return decode(value)

    def get_indexes(self):
        indexes = super(TextField, self).get_indexes()
        if self._fts:
            indexes.append(FullTextIndex(
                self,
                self._stemmer,
                self._metaphone,
                self._stopwords_file,
                self._min_word_length))
        return indexes


class BooleanField(Field):
    """Store boolean values."""
    def db_value(self, value):
        return '1' if value else '0'

    def python_value(self, value):
        return decode(value) == '1'


class UUIDField(Field):
    """Store unique IDs. Can be used as primary key."""
    def __init__(self, **kwargs):
        kwargs['index'] = True
        super(UUIDField, self).__init__(**kwargs)

    def db_value(self, value):
        return encode(value.hex if value is not None else '')

    def python_value(self, value):
        return uuid.UUID(decode(value)) if value else None

    def _generate_key(self):
        return uuid.uuid4()


class DateTimeField(_ScalarField):
    """Store Python datetime objects."""
    def db_value(self, value):
        if value is None:
            return 0.

        timestamp = time.mktime(value.timetuple())
        micro = value.microsecond * (10 ** -6)
        return timestamp + micro

    def python_value(self, value):
        if not value:
            return None
        elif isinstance(value, (basestring_type, int, float)):
            return datetime.datetime.fromtimestamp(float(value))
        else:
            return value


class DateField(DateTimeField):
    """Store Python date objects."""
    def db_value(self, value):
        if value is None:
            return 0.
        return time.mktime(value.timetuple())

    def python_value(self, value):
        if not value:
            return None
        elif isinstance(value, (basestring_type, int, float)):
            return datetime.datetime.fromtimestamp(float(value)).date()
        else:
            return value


class JSONField(Field):
    """Store arbitrary JSON data."""
    def db_value(self, value):
        return encode(json.dumps(value))

    def python_value(self, value):
        return json.loads(decode(value))


class PickledField(Field):
    """Store arbitrary Python objects."""
    def db_value(self, value):
        return pickle.dumps(value, pickle.HIGHEST_PROTOCOL)

    def python_value(self, value):
        return pickle.loads(value)


class _ContainerField(Field):
    container_class = None

    def __init__(self, *args, **kwargs):
        super(_ContainerField, self).__init__(*args, **kwargs)
        if self._primary_key:
            raise ValueError('Container fields cannot be primary keys.')
        if self._index:
            raise ValueError('Container fields cannot be indexed.')

    def _get_container(self, instance):
        return self.container_class(
            self.model_class.__database__,
            self.__key__(instance))

    def __key__(self, instance):
        return self.model_class._query.make_key(
            'container',
            self.name,
            instance.get_hash_id())

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            if not instance.get_id():
                raise ValueError('Model must have a primary key before '
                                 'container attributes can be accessed.')
            return self._get_container(instance)
        return self

    def __set__(self, instance, instance_type=None):
        raise ValueError('Cannot set the value of a container field.')

    def _delete(self, instance):
        self._get_container(instance).clear()


class HashField(_ContainerField):
    """Store values in a Redis hash."""
    container_class = Hash


class ListField(_ContainerField):
    """Store values in a Redis list."""
    container_class = List


class SetField(_ContainerField):
    """Store values in a Redis set."""
    container_class = Set


class ZSetField(_ContainerField):
    """Store values in a Redis sorted set."""
    container_class = ZSet


class Query(object):
    def __init__(self, model_class):
        self.model_class = model_class

    @property
    def _base_key(self):
        model_name = self.model_class.__name__.lower()
        if self.model_class.__namespace__:
            return '%s|%s:' % (self.model_class.__namespace__, model_name)
        return '%s:' % model_name

    def make_key(self, *parts):
        """Generate a namespaced key for the given path."""
        separator = getattr(self.model_class, 'index_separator', '.')
        parts = map(decode, parts)
        return '%s%s' % (self._base_key, separator.join(map(str, parts)))

    def get_primary_hash_key(self, primary_key):
        pk_field = self.model_class._fields[self.model_class._primary_key]
        return self.make_key('id', pk_field.db_value(primary_key))

    def all_index(self):
        return self.model_class.__database__.Set(self.make_key('all'))


class BaseIndex(object):
    operations = None

    def __init__(self, field):
        self.field = field
        self.__database__ = self.field.model_class.__database__
        self.query_helper = self.field.model_class._query

    def field_value(self, instance):
        return self.field.db_value(getattr(instance, self.field.name))

    def get_key(self, value):
        raise NotImplementedError

    def store_instance(self, key, instance, value):
        raise NotImplementedError

    def delete_instance(self, key, instance, value):
        raise NotImplementedError

    def save(self, instance):
        value = self.field_value(instance)
        key = self.get_key(value)
        self.store_instance(key, instance, value)

    def remove(self, instance):
        value = self.field_value(instance)
        key = self.get_key(value)
        self.delete_instance(key, instance, value)


class AbsoluteIndex(BaseIndex):
    operations = ABSOLUTE

    def get_key(self, value):
        key = self.query_helper.make_key(
            self.field.name,
            'absolute',
            value)
        return self.__database__.Set(key)

    def store_instance(self, key, instance, value):
        key.add(instance.get_hash_id())

    def delete_instance(self, key, instance, value):
        key.remove(instance.get_hash_id())
        if len(key) == 0:
            key.clear()


class ContinuousIndex(BaseIndex):
    operations = CONTINUOUS

    def get_key(self, value):
        key = self.query_helper.make_key(
            self.field.name,
            'continuous')
        return self.__database__.ZSet(key)

    def store_instance(self, key, instance, value):
        key[instance.get_hash_id()] = value

    def delete_instance(self, key, instance, value):
        del key[instance.get_hash_id()]
        if len(key) == 0:
            key.clear()


class FullTextIndex(BaseIndex):
    operations = FTS

    def __init__(self, field, stemmer=True, metaphone=False,
                 stopwords_file=None, min_word_length=None):
        super(FullTextIndex, self).__init__(field)
        self.tokenizer = Tokenizer(
            stemmer=stemmer,
            metaphone=metaphone,
            stopwords_file=stopwords_file or 'stopwords.txt',
            min_word_length=min_word_length)

    def get_key(self, value):
        key = self.query_helper.make_key(
            self.field.name,
            'fts',
            value)
        return self.__database__.ZSet(key)

    def store_instance(self, key, instance, value):
        hash_id = instance.get_hash_id()
        for word, score in self.tokenizer.tokenize(value).items():
            key = self.get_key(word)
            key[hash_id] = -score

    def delete_instance(self, key, instance, value):
        hash_id = instance.get_hash_id()
        for word in self.tokenizer.tokenize(value):
            key = self.get_key(word)
            del key[hash_id]
            if len(key) == 0:
                key.clear()


class BaseModel(type):
    def __new__(cls, name, bases, attrs):
        if not bases:
            return super(BaseModel, cls).__new__(cls, name, bases, attrs)

        if 'database' in attrs:
            warn('"database" has been deprecated in favor of "__database__" '
                 'for Walrus models.', DeprecationWarning)
            attrs['__database__'] = attrs.pop('database')
        if 'namespace' in attrs:
            warn('"namespace" has been deprecated in favor of "__namespace__" '
                 'for Walrus models.', DeprecationWarning)
            attrs['__namespace__'] = attrs.pop('namespace')

        # Declarative base juju.
        ignore = set()
        primary_key = None

        for key, value in attrs.items():
            if isinstance(value, Field) and value._primary_key:
                primary_key = (key, value)

        for base in bases:
            for key, value in base.__dict__.items():
                if key in attrs:
                    continue
                if isinstance(value, Field):
                    if value._primary_key and primary_key:
                        ignore.add(key)
                    else:
                        if value._primary_key:
                            primary_key = (key, value)
                        attrs[key] = deepcopy(value)

        if not primary_key:
            attrs['_id'] = AutoIncrementField()
            primary_key = ('_id', attrs['_id'])

        model_class = super(BaseModel, cls).__new__(cls, name, bases, attrs)
        model_class._data = None

        defaults = {}
        fields = {}
        indexes = []
        for key, value in model_class.__dict__.items():
            if isinstance(value, Field) and key not in ignore:
                value.add_to_class(model_class, key)
                if value._index:
                    indexes.append(value)
                fields[key] = value
                if value._default is not None:
                    defaults[key] = value._default

        model_class._defaults = defaults
        model_class._fields = fields
        model_class._indexes = indexes
        model_class._primary_key = primary_key[0]
        model_class._query = Query(model_class)
        return model_class


def _with_metaclass(meta, base=object):
    return meta("NewBase", (base,), {'__database__': None,
                                     '__namespace__': None})


class Model(_with_metaclass(BaseModel)):
    """
    A collection of fields to be stored in the database. Walrus
    stores model instance data in hashes keyed by a combination of
    model name and primary key value. Instance attributes are
    automatically converted to values suitable for storage in Redis
    (i.e., datetime becomes timestamp), and vice-versa.

    Additionally, model fields can be ``indexed``, which allows
    filtering. There are three types of indexes:

    * Absolute
    * Scalar
    * Full-text search

    Absolute indexes are used for values like strings or UUIDs and
    support only equality and inequality checks.

    Scalar indexes are for numeric values as well as datetimes,
    and support equality, inequality, and greater or less-than.

    The final type of index, FullText, can only be used with the
    :py:class:`TextField`. FullText indexes allow search using
    the ``match()`` method. For more info, see :ref:`fts`.
    """
    #: **Required**: the :py:class:`Database` instance to use to
    #: persist model data.
    __database__ = None

    #: **Optional**: namespace to use for model data.
    __namespace__ = None

    #: **Required**: character to use as a delimiter for indexes, default "."
    index_separator = '.'

    def __init__(self, *args, **kwargs):
        self._data = {}
        self._load_default_dict()
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return '<%s: %s>' % (type(self).__name__, self.get_id())

    def _load_default_dict(self):
        for field_name, default in self._defaults.items():
            if callable(default):
                default = default()
            setattr(self, field_name, default)

    def incr(self, field, incr_by=1):
        """
        Increment the value stored in the given field by the specified
        amount. Any indexes will be updated at the time ``incr()`` is
        called.

        :param Field field: A field instance.
        :param incr_by: An ``int`` or ``float``.

        Example:

        .. code-block:: python

            # Retrieve a page counter object for the given URL.
            page_count = PageCounter.get(PageCounter.url == url)

            # Update the hit count, persisting to the database and
            # updating secondary indexes in one go.
            page_count.incr(PageCounter.hits)
        """
        model_hash = self.to_hash()

        # Remove the value from the index.
        for index in field.get_indexes():
            index.remove(self)

        if isinstance(incr_by, int):
            new_val = model_hash.incr(field.name, incr_by)
        else:
            new_val = model_hash.incr_float(field.name, incr_by)
        setattr(self, field.name, new_val)

        # Re-index the new value.
        for index in field.get_indexes():
            index.save(self)

        return new_val

    def get_id(self):
        """
        Return the primary key for the model instance. If the
        model is unsaved, then this value will be ``None``.
        """
        try:
            return getattr(self, self._primary_key)
        except KeyError:
            return None

    def get_hash_id(self):
        return self._query.get_primary_hash_key(self.get_id())

    def _get_data_dict(self):
        data = {}
        for name, field in self._fields.items():
            if name in self._data:
                data[name] = field.db_value(self._data[name])
        return data

    def to_hash(self):
        """
        Return a :py:class:`Hash` instance corresponding to the
        raw model data.
        """
        return self.__database__.Hash(self.get_hash_id())

    @classmethod
    def create(cls, **kwargs):
        """
        Create a new model instance and save it to the database.
        Values are passed in as keyword arguments.

        Example::

            user = User.create(first_name='Charlie', last_name='Leifer')
        """
        instance = cls(**kwargs)
        instance.save(_is_create=True)
        return instance

    @classmethod
    def all(cls):
        """
        Return an iterator that successively yields saved model
        instances. Models are saved in an unordered :py:class:`Set`,
        so the iterator will return them in arbitrary order.

        Example::

            for note in Note.all():
                print note.content

        To return models in sorted order, see :py:meth:`Model.query`.
        Example returning all records, sorted newest to oldest::

            for note in Note.query(order_by=Note.timestamp.desc()):
                print note.timestamp, note.content
        """
        for result in cls._query.all_index():
            yield cls.load(result, convert_key=False)

    @classmethod
    def query(cls, expression=None, order_by=None):
        """
        Return model instances matching the given expression (if
        specified). Additionally, matching instances can be returned
        sorted by field value.

        Example::

            # Get administrators sorted by username.
            admin_users = User.query(
                (User.admin == True),
                order_by=User.username)

            # List blog entries newest to oldest.
            entries = Entry.query(order_by=Entry.timestamp.desc())

            # Perform a complex filter.
            values = StatData.query(
                (StatData.timestamp < datetime.date.today()) &
                ((StatData.type == 'pv') | (StatData.type == 'cv')))

        :param expression: A boolean expression to filter by.
        :param order_by: A field whose value should be used to
            sort returned instances.
        """
        if expression is not None:
            executor = Executor(cls.__database__)
            result = executor.execute(expression)
        else:
            result = cls._query.all_index()

        if order_by is not None:
            desc = False
            if isinstance(order_by, Desc):
                desc = True
                order_by = order_by.node

            alpha = not isinstance(order_by, _ScalarField)
            result = cls.__database__.sort(
                result.key,
                by='*->%s' % order_by.name,
                alpha=alpha,
                desc=desc)
        elif isinstance(result, ZSet):
            result = result.iterator(reverse=True)

        for hash_id in result:
            yield cls.load(hash_id, convert_key=False)

    @classmethod
    def query_delete(cls, expression=None):
        """
        Delete model instances matching the given expression (if
        specified). If no expression is provided, then all model instances
        will be deleted.

        :param expression: A boolean expression to filter by.
        """
        if expression is not None:
            executor = Executor(cls.__database__)
            result = executor.execute(expression)
        else:
            result = cls._query.all_index()

        for hash_id in result:
            cls.load(hash_id, convert_key=False).delete()

    @classmethod
    def get(cls, expression):
        """
        Retrieve the model instance matching the given expression.
        If the number of matching results is not equal to one, then
        a ``ValueError`` will be raised.

        :param expression: A boolean expression to filter by.
        :returns: The matching :py:class:`Model` instance.
        :raises: ``ValueError`` if result set size is not 1.
        """
        executor = Executor(cls.__database__)
        result = executor.execute(expression)
        if len(result) != 1:
            raise ValueError('Got %s results, expected 1.' % len(result))
        return cls.load(result._first_or_any(), convert_key=False)

    @classmethod
    def load(cls, primary_key, convert_key=True):
        """
        Retrieve a model instance by primary key.

        :param primary_key: The primary key of the model instance.
        :returns: Corresponding :py:class:`Model` instance.
        :raises: ``KeyError`` if object with given primary key does
            not exist.
        """
        if convert_key:
            primary_key = cls._query.get_primary_hash_key(primary_key)
        if not cls.__database__.hash_exists(primary_key):
            raise KeyError('Object not found.')
        raw_data = cls.__database__.hgetall(primary_key)
        if PY3:
            raw_data = decode_dict_keys(raw_data)
        data = {}
        for name, field in cls._fields.items():
            if isinstance(field, _ContainerField):
                continue
            elif name in raw_data:
                data[name] = field.python_value(raw_data[name])
            else:
                data[name] = None

        return cls(**data)

    @classmethod
    def count(cls):
        """
        Return the number of objects in the given collection.
        """
        return len(cls._query.all_index())

    def delete(self, for_update=False):
        """
        Delete the given model instance.
        """
        hash_key = self.get_hash_id()
        try:
            original_instance = self.load(hash_key, convert_key=False)
        except KeyError:
            return

        # Remove from the `all` index.
        all_index = self._query.all_index()
        all_index.remove(hash_key)

        # Remove from the secondary indexes.
        for field in self._indexes:
            for index in field.get_indexes():
                index.remove(original_instance)

        if not for_update:
            for field in self._fields.values():
                if isinstance(field, _ContainerField):
                    field._delete(self)

        # Remove the object itself.
        self.__database__.delete(hash_key)

    def save(self, _is_create=False):
        """
        Save the given model instance. If the model does not have
        a primary key value, Walrus will call the primary key field's
        ``generate_key()`` method to attempt to generate a suitable
        value.
        """
        pk_field = self._fields[self._primary_key]
        if not self._data.get(self._primary_key):
            setattr(self, self._primary_key, pk_field._generate_key())
            require_delete = False
        else:
            require_delete = not _is_create

        if require_delete:
            self.delete(for_update=True)

        data = self._get_data_dict()
        hash_obj = self.to_hash()
        hash_obj.clear()
        hash_obj.update(data)

        all_index = self._query.all_index()
        all_index.add(self.get_hash_id())

        for field in self._indexes:
            for index in field.get_indexes():
                index.save(self)
