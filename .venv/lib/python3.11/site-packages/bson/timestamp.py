# Copyright 2010-2015 MongoDB, Inc.
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

"""Tools for representing MongoDB internal Timestamps."""
from __future__ import annotations

import calendar
import datetime
from typing import Any, Union

from bson._helpers import _getstate_slots, _setstate_slots
from bson.tz_util import utc

UPPERBOUND = 4294967296


class Timestamp:
    """MongoDB internal timestamps used in the opLog."""

    __slots__ = ("__time", "__inc")

    __getstate__ = _getstate_slots
    __setstate__ = _setstate_slots

    _type_marker = 17

    def __init__(self, time: Union[datetime.datetime, int], inc: int) -> None:
        """Create a new :class:`Timestamp`.

        This class is only for use with the MongoDB opLog. If you need
        to store a regular timestamp, please use a
        :class:`~datetime.datetime`.

        Raises :class:`TypeError` if `time` is not an instance of
        :class: `int` or :class:`~datetime.datetime`, or `inc` is not
        an instance of :class:`int`. Raises :class:`ValueError` if
        `time` or `inc` is not in [0, 2**32).

        :param time: time in seconds since epoch UTC, or a naive UTC
            :class:`~datetime.datetime`, or an aware
            :class:`~datetime.datetime`
        :param inc: the incrementing counter
        """
        if isinstance(time, datetime.datetime):
            offset = time.utcoffset()
            if offset is not None:
                time = time - offset
            time = int(calendar.timegm(time.timetuple()))
        if not isinstance(time, int):
            raise TypeError("time must be an instance of int")
        if not isinstance(inc, int):
            raise TypeError("inc must be an instance of int")
        if not 0 <= time < UPPERBOUND:
            raise ValueError("time must be contained in [0, 2**32)")
        if not 0 <= inc < UPPERBOUND:
            raise ValueError("inc must be contained in [0, 2**32)")

        self.__time = time
        self.__inc = inc

    @property
    def time(self) -> int:
        """Get the time portion of this :class:`Timestamp`."""
        return self.__time

    @property
    def inc(self) -> int:
        """Get the inc portion of this :class:`Timestamp`."""
        return self.__inc

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Timestamp):
            return self.__time == other.time and self.__inc == other.inc
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.time) ^ hash(self.inc)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, Timestamp):
            return (self.time, self.inc) < (other.time, other.inc)
        return NotImplemented

    def __le__(self, other: Any) -> bool:
        if isinstance(other, Timestamp):
            return (self.time, self.inc) <= (other.time, other.inc)
        return NotImplemented

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, Timestamp):
            return (self.time, self.inc) > (other.time, other.inc)
        return NotImplemented

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, Timestamp):
            return (self.time, self.inc) >= (other.time, other.inc)
        return NotImplemented

    def __repr__(self) -> str:
        return f"Timestamp({self.__time}, {self.__inc})"

    def as_datetime(self) -> datetime.datetime:
        """Return a :class:`~datetime.datetime` instance corresponding
        to the time portion of this :class:`Timestamp`.

        The returned datetime's timezone is UTC.
        """
        return datetime.datetime.fromtimestamp(self.__time, utc)
