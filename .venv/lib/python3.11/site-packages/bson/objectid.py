# Copyright 2009-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tools for working with MongoDB ObjectIds."""
from __future__ import annotations

import binascii
import calendar
import datetime
import os
import struct
import threading
import time
from random import SystemRandom
from typing import Any, NoReturn, Optional, Type, Union

from bson.errors import InvalidId
from bson.tz_util import utc

_MAX_COUNTER_VALUE = 0xFFFFFF


def _raise_invalid_id(oid: str) -> NoReturn:
    raise InvalidId(
        "%r is not a valid ObjectId, it must be a 12-byte input"
        " or a 24-character hex string" % oid
    )


def _random_bytes() -> bytes:
    """Get the 5-byte random field of an ObjectId."""
    return os.urandom(5)


class ObjectId:
    """A MongoDB ObjectId."""

    _pid = os.getpid()

    _inc = SystemRandom().randint(0, _MAX_COUNTER_VALUE)
    _inc_lock = threading.Lock()

    __random = _random_bytes()

    __slots__ = ("__id",)

    _type_marker = 7

    def __init__(self, oid: Optional[Union[str, ObjectId, bytes]] = None) -> None:
        """Initialize a new ObjectId.

        An ObjectId is a 12-byte unique identifier consisting of:

          - a 4-byte value representing the seconds since the Unix epoch,
          - a 5-byte random value,
          - a 3-byte counter, starting with a random value.

        By default, ``ObjectId()`` creates a new unique identifier. The
        optional parameter `oid` can be an :class:`ObjectId`, or any 12
        :class:`bytes`.

        For example, the 12 bytes b'foo-bar-quux' do not follow the ObjectId
        specification but they are acceptable input::

          >>> ObjectId(b'foo-bar-quux')
          ObjectId('666f6f2d6261722d71757578')

        `oid` can also be a :class:`str` of 24 hex digits::

          >>> ObjectId('0123456789ab0123456789ab')
          ObjectId('0123456789ab0123456789ab')

        Raises :class:`~bson.errors.InvalidId` if `oid` is not 12 bytes nor
        24 hex digits, or :class:`TypeError` if `oid` is not an accepted type.

        :param oid: a valid ObjectId.

        .. seealso:: The MongoDB documentation on  `ObjectIds <http://dochub.mongodb.org/core/objectids>`_.

        .. versionchanged:: 3.8
           :class:`~bson.objectid.ObjectId` now implements the `ObjectID
           specification version 0.2
           <https://github.com/mongodb/specifications/blob/master/source/
           objectid.rst>`_.
        """
        if oid is None:
            self.__generate()
        elif isinstance(oid, bytes) and len(oid) == 12:
            self.__id = oid
        else:
            self.__validate(oid)

    @classmethod
    def from_datetime(cls: Type[ObjectId], generation_time: datetime.datetime) -> ObjectId:
        """Create a dummy ObjectId instance with a specific generation time.

        This method is useful for doing range queries on a field
        containing :class:`ObjectId` instances.

        .. warning::
           It is not safe to insert a document containing an ObjectId
           generated using this method. This method deliberately
           eliminates the uniqueness guarantee that ObjectIds
           generally provide. ObjectIds generated with this method
           should be used exclusively in queries.

        `generation_time` will be converted to UTC. Naive datetime
        instances will be treated as though they already contain UTC.

        An example using this helper to get documents where ``"_id"``
        was generated before January 1, 2010 would be:

        >>> gen_time = datetime.datetime(2010, 1, 1)
        >>> dummy_id = ObjectId.from_datetime(gen_time)
        >>> result = collection.find({"_id": {"$lt": dummy_id}})

        :param generation_time: :class:`~datetime.datetime` to be used
            as the generation time for the resulting ObjectId.
        """
        offset = generation_time.utcoffset()
        if offset is not None:
            generation_time = generation_time - offset
        timestamp = calendar.timegm(generation_time.timetuple())
        oid = struct.pack(">I", int(timestamp)) + b"\x00\x00\x00\x00\x00\x00\x00\x00"
        return cls(oid)

    @classmethod
    def is_valid(cls: Type[ObjectId], oid: Any) -> bool:
        """Checks if a `oid` string is valid or not.

        :param oid: the object id to validate

        .. versionadded:: 2.3
        """
        if not oid:
            return False

        try:
            ObjectId(oid)
            return True
        except (InvalidId, TypeError):
            return False

    @classmethod
    def _random(cls) -> bytes:
        """Generate a 5-byte random number once per process."""
        pid = os.getpid()
        if pid != cls._pid:
            cls._pid = pid
            cls.__random = _random_bytes()
        return cls.__random

    def __generate(self) -> None:
        """Generate a new value for this ObjectId."""
        # 4 bytes current time
        oid = struct.pack(">I", int(time.time()))

        # 5 bytes random
        oid += ObjectId._random()

        # 3 bytes inc
        with ObjectId._inc_lock:
            oid += struct.pack(">I", ObjectId._inc)[1:4]
            ObjectId._inc = (ObjectId._inc + 1) % (_MAX_COUNTER_VALUE + 1)

        self.__id = oid

    def __validate(self, oid: Any) -> None:
        """Validate and use the given id for this ObjectId.

        Raises TypeError if id is not an instance of :class:`str`,
        :class:`bytes`, or ObjectId. Raises InvalidId if it is not a
        valid ObjectId.

        :param oid: a valid ObjectId
        """
        if isinstance(oid, ObjectId):
            self.__id = oid.binary
        elif isinstance(oid, str):
            if len(oid) == 24:
                try:
                    self.__id = bytes.fromhex(oid)
                except (TypeError, ValueError):
                    _raise_invalid_id(oid)
            else:
                _raise_invalid_id(oid)
        else:
            raise TypeError(f"id must be an instance of (bytes, str, ObjectId), not {type(oid)}")

    @property
    def binary(self) -> bytes:
        """12-byte binary representation of this ObjectId."""
        return self.__id

    @property
    def generation_time(self) -> datetime.datetime:
        """A :class:`datetime.datetime` instance representing the time of
        generation for this :class:`ObjectId`.

        The :class:`datetime.datetime` is timezone aware, and
        represents the generation time in UTC. It is precise to the
        second.
        """
        timestamp = struct.unpack(">I", self.__id[0:4])[0]
        return datetime.datetime.fromtimestamp(timestamp, utc)

    def __getstate__(self) -> bytes:
        """Return value of object for pickling.
        needed explicitly because __slots__() defined.
        """
        return self.__id

    def __setstate__(self, value: Any) -> None:
        """Explicit state set from pickling"""
        # Provide backwards compatibility with OIDs
        # pickled with pymongo-1.9 or older.
        if isinstance(value, dict):
            oid = value["_ObjectId__id"]
        else:
            oid = value
        # ObjectIds pickled in python 2.x used `str` for __id.
        # In python 3.x this has to be converted to `bytes`
        # by encoding latin-1.
        if isinstance(oid, str):
            self.__id = oid.encode("latin-1")
        else:
            self.__id = oid

    def __str__(self) -> str:
        return binascii.hexlify(self.__id).decode()

    def __repr__(self) -> str:
        return f"ObjectId('{self!s}')"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ObjectId):
            return self.__id == other.binary
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, ObjectId):
            return self.__id != other.binary
        return NotImplemented

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, ObjectId):
            return self.__id < other.binary
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if isinstance(other, ObjectId):
            return self.__id <= other.binary
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, ObjectId):
            return self.__id > other.binary
        return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, ObjectId):
            return self.__id >= other.binary
        return NotImplemented

    def __hash__(self) -> int:
        """Get a hash value for this :class:`ObjectId`."""
        return hash(self.__id)
