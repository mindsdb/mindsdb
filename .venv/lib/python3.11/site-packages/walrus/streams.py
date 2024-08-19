import datetime
import operator
import time

from walrus.containers import ConsumerGroup
from walrus.containers import ConsumerGroupStream
from walrus.utils import basestring_type
from walrus.utils import decode
from walrus.utils import decode_dict
from walrus.utils import make_python_attr


def id_to_datetime(ts):
    tsm, seq = ts.split(b'-', 1)
    return datetime.datetime.fromtimestamp(int(tsm) / 1000.), int(seq)

def datetime_to_id(dt, seq=0):
    tsm = time.mktime(dt.timetuple()) * 1000
    return '%s-%s' % (int(tsm + (dt.microsecond / 1000)), seq)


class Message(object):
    """
    A message stored in a Redis stream.

    When reading messages from a :py:class:`TimeSeries`, the usual 2-tuple of
    (message id, data) is unpacked into a :py:class:`Message` instance. The
    message instance provides convenient access to the message timestamp as a
    datetime. Additionally, the message data is UTF8-decoded for convenience.
    """
    __slots__ = ('stream', 'timestamp', 'sequence', 'data', 'message_id')

    def __init__(self, stream, message_id, data):
        self.stream = decode(stream)
        self.message_id = decode(message_id)
        self.data = decode_dict(data)
        self.timestamp, self.sequence = id_to_datetime(message_id)

    def __repr__(self):
        return '<Message %s %s: %s>' % (self.stream, self.message_id,
                                        self.data)


def normalize_id(message_id):
    if isinstance(message_id, basestring_type):
        return message_id
    elif isinstance(message_id, datetime.datetime):
        return datetime_to_id(message_id)
    elif isinstance(message_id, tuple):
        return datetime_to_id(*message_id)
    elif isinstance(message_id, Message):
        return message_id.message_id
    return message_id


def normalize_ids(id_list):
    return [normalize_id(id) for id in id_list]


def xread_to_messages(resp):
    if resp is None: return
    accum = []
    for stream, messages in resp:
        accum.extend(xrange_to_messages(stream, messages))
    # If multiple streams are present, sort them by timestamp.
    if len(resp) > 1:
        accum.sort(key=operator.attrgetter('message_id'))
    return accum


def xrange_to_messages(stream, resp):
    return [Message(stream, message_id, data) for message_id, data in resp]


class TimeSeriesStream(ConsumerGroupStream):
    """
    Helper for working with an individual stream within the context of a
    :py:class:`TimeSeries` consumer group. This object is exposed as an
    attribute on a :py:class:`TimeSeries` object using the stream key for the
    attribute name.

    This class should not be created directly. It will automatically be added
    to the ``TimeSeries`` object.

    For example::

        ts = db.time_series('events', ['stream-1', 'stream-2'])
        ts.stream_1  # TimeSeriesStream for "stream-1"
        ts.stream_2  # TimeSeriesStream for "stream-2"

    This class implements the same methods as :py:class:`ConsumerGroupStream`,
    with the following differences in behavior:

    * Anywhere an ID (or list of IDs) is accepted, this class will also accept
      a datetime, a 2-tuple of (datetime, sequence), a :py:class:`Message`, in
      addition to a regular bytestring ID.
    * Instead of returning a list of (message id, data) 2-tuples, this class
      returns a list of :py:class:`Message` objects.
    * Data is automatically UTF8 decoded when being read for convenience.
    """
    __slots__ = ('database', 'group', 'key', '_consumer')

    def ack(self, *id_list):
        return super(TimeSeriesStream, self).ack(*normalize_ids(id_list))

    def add(self, data, id='*', maxlen=None, approximate=True):
        db_id = super(TimeSeriesStream, self).add(data, normalize_id(id),
                                                  maxlen, approximate)
        return id_to_datetime(db_id)

    def claim(self, *id_list, **kwargs):
        resp = super(TimeSeriesStream, self).claim(*normalize_ids(id_list),
                                                   **kwargs)
        return xrange_to_messages(self.key, resp)

    def delete(self, *id_list):
        return super(TimeSeriesStream, self).delete(*normalize_ids(id_list))

    def get(self, id):
        id = normalize_id(id)
        messages = self.range(id, id, 1)
        if messages:
            return messages[0]

    def range(self, start='-', stop='+', count=None):
        resp = super(TimeSeriesStream, self).range(
            normalize_id(start),
            normalize_id(stop),
            count)
        return xrange_to_messages(self.key, resp)

    def pending(self, start='-', stop='+', count=1000, consumer=None,
                idle=None):
        start = normalize_id(start)
        stop = normalize_id(stop)
        resp = self.database.xpending_range(self.key, self.group, min=start,
                                            max=stop, count=count,
                                            consumername=consumer, idle=idle)
        return [(id_to_datetime(msg['message_id']), decode(msg['consumer']),
                 msg['time_since_delivered'], msg['times_delivered'])
                for msg in resp]

    def read(self, count=None, block=None):
        resp = super(TimeSeriesStream, self).read(count, block)
        if resp is not None:
            return xrange_to_messages(self.key, resp)

    def set_id(self, id='$'):
        return super(TimeSeriesStream, self).set_id(normalize_id(id))


class TimeSeries(ConsumerGroup):
    """
    :py:class:`TimeSeries` is a consumer-group that provides a higher level of
    abstraction, reading and writing message ids as datetimes, and returning
    messages using a convenient, lightweight :py:class:`Message` class.

    Rather than creating this class directly, use the
    :py:meth:`Database.time_series` method.

    Each registered stream within the group is exposed as a special attribute
    that provides stream-specific APIs within the context of the group. For
    more information see :py:class:`TimeSeriesStream`.

    Example::

        ts = db.time_series('groupname', ['stream-1', 'stream-2'])
        ts.stream_1  # TimeSeriesStream for "stream-1"
        ts.stream_2  # TimeSeriesStream for "stream-2"

    :param Database database: Redis client
    :param group: name of consumer group
    :param keys: stream identifier(s) to monitor. May be a single stream
        key, a list of stream keys, or a key-to-minimum id mapping. The
        minimum id for each stream should be considered an exclusive
        lower-bound. The '$' value can also be used to only read values
        added *after* our command started blocking.
    :param consumer: name for consumer within group
    :returns: a :py:class:`TimeSeries` instance
    """
    stream_key_class = TimeSeriesStream

    def read(self, count=None, block=None):
        """
        Read unseen messages from all streams in the consumer group. Wrapper
        for :py:class:`Database.xreadgroup` method.

        :param int count: limit number of messages returned
        :param int block: milliseconds to block, 0 for indefinitely.
        :returns: a list of :py:class:`Message` objects
        """
        resp = super(TimeSeries, self).read(count, block)
        return xread_to_messages(resp)

    def set_id(self, id='$'):
        return super(TimeSeries, self).set_id(normalize_id(id))
