# Copyright 2022-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Tools for representing the BSON datetime type.

.. versionadded:: 4.3
"""
from __future__ import annotations

import calendar
import datetime
import functools
from typing import Any, Union, cast

from bson.codec_options import DEFAULT_CODEC_OPTIONS, CodecOptions, DatetimeConversion
from bson.errors import InvalidBSON
from bson.tz_util import utc

EPOCH_AWARE = datetime.datetime.fromtimestamp(0, utc)
EPOCH_NAIVE = EPOCH_AWARE.replace(tzinfo=None)
_DATETIME_ERROR_SUGGESTION = (
    "(Consider Using CodecOptions(datetime_conversion=DATETIME_AUTO)"
    " or MongoClient(datetime_conversion='DATETIME_AUTO'))."
    " See: https://pymongo.readthedocs.io/en/stable/examples/datetimes.html#handling-out-of-range-datetimes"
)


class DatetimeMS:
    """Represents a BSON UTC datetime."""

    __slots__ = ("_value",)

    def __init__(self, value: Union[int, datetime.datetime]):
        """Represents a BSON UTC datetime.

        BSON UTC datetimes are defined as an int64 of milliseconds since the
        Unix epoch. The principal use of DatetimeMS is to represent
        datetimes outside the range of the Python builtin
        :class:`~datetime.datetime` class when
        encoding/decoding BSON.

        To decode UTC datetimes as a ``DatetimeMS``, `datetime_conversion` in
        :class:`~bson.codec_options.CodecOptions` must be set to 'datetime_ms' or
        'datetime_auto'. See :ref:`handling-out-of-range-datetimes` for
        details.

        :param value: An instance of :class:`datetime.datetime` to be
            represented as milliseconds since the Unix epoch, or int of
            milliseconds since the Unix epoch.
        """
        if isinstance(value, int):
            if not (-(2**63) <= value <= 2**63 - 1):
                raise OverflowError("Must be a 64-bit integer of milliseconds")
            self._value = value
        elif isinstance(value, datetime.datetime):
            self._value = _datetime_to_millis(value)
        else:
            raise TypeError(f"{type(value)} is not a valid type for DatetimeMS")

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return type(self).__name__ + "(" + str(self._value) + ")"

    def __lt__(self, other: Union[DatetimeMS, int]) -> bool:
        return self._value < other

    def __le__(self, other: Union[DatetimeMS, int]) -> bool:
        return self._value <= other

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DatetimeMS):
            return self._value == other._value
        return False

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, DatetimeMS):
            return self._value != other._value
        return True

    def __gt__(self, other: Union[DatetimeMS, int]) -> bool:
        return self._value > other

    def __ge__(self, other: Union[DatetimeMS, int]) -> bool:
        return self._value >= other

    _type_marker = 9

    def as_datetime(
        self, codec_options: CodecOptions[Any] = DEFAULT_CODEC_OPTIONS
    ) -> datetime.datetime:
        """Create a Python :class:`~datetime.datetime` from this DatetimeMS object.

        :param codec_options: A CodecOptions instance for specifying how the
            resulting DatetimeMS object will be formatted using ``tz_aware``
            and ``tz_info``. Defaults to
            :const:`~bson.codec_options.DEFAULT_CODEC_OPTIONS`.
        """
        return cast(datetime.datetime, _millis_to_datetime(self._value, codec_options))

    def __int__(self) -> int:
        return self._value


# Inclusive and exclusive min and max for timezones.
# Timezones are hashed by their offset, which is a timedelta
# and therefore there are more than 24 possible timezones.
@functools.lru_cache(maxsize=None)
def _min_datetime_ms(tz: datetime.timezone = datetime.timezone.utc) -> int:
    return _datetime_to_millis(datetime.datetime.min.replace(tzinfo=tz))


@functools.lru_cache(maxsize=None)
def _max_datetime_ms(tz: datetime.timezone = datetime.timezone.utc) -> int:
    return _datetime_to_millis(datetime.datetime.max.replace(tzinfo=tz))


def _millis_to_datetime(
    millis: int, opts: CodecOptions[Any]
) -> Union[datetime.datetime, DatetimeMS]:
    """Convert milliseconds since epoch UTC to datetime."""
    if (
        opts.datetime_conversion == DatetimeConversion.DATETIME
        or opts.datetime_conversion == DatetimeConversion.DATETIME_CLAMP
        or opts.datetime_conversion == DatetimeConversion.DATETIME_AUTO
    ):
        tz = opts.tzinfo or datetime.timezone.utc
        if opts.datetime_conversion == DatetimeConversion.DATETIME_CLAMP:
            millis = max(_min_datetime_ms(tz), min(millis, _max_datetime_ms(tz)))
        elif opts.datetime_conversion == DatetimeConversion.DATETIME_AUTO:
            if not (_min_datetime_ms(tz) <= millis <= _max_datetime_ms(tz)):
                return DatetimeMS(millis)

        diff = ((millis % 1000) + 1000) % 1000
        seconds = (millis - diff) // 1000
        micros = diff * 1000

        try:
            if opts.tz_aware:
                dt = EPOCH_AWARE + datetime.timedelta(seconds=seconds, microseconds=micros)
                if opts.tzinfo:
                    dt = dt.astimezone(tz)
                return dt
            else:
                return EPOCH_NAIVE + datetime.timedelta(seconds=seconds, microseconds=micros)
        except ArithmeticError as err:
            raise InvalidBSON(f"{err} {_DATETIME_ERROR_SUGGESTION}") from err

    elif opts.datetime_conversion == DatetimeConversion.DATETIME_MS:
        return DatetimeMS(millis)
    else:
        raise ValueError("datetime_conversion must be an element of DatetimeConversion")


def _datetime_to_millis(dtm: datetime.datetime) -> int:
    """Convert datetime to milliseconds since epoch UTC."""
    if dtm.utcoffset() is not None:
        dtm = dtm - dtm.utcoffset()  # type: ignore
    return int(calendar.timegm(dtm.timetuple()) * 1000 + dtm.microsecond // 1000)
