# Copyright 2013-present MongoDB, Inc.
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

"""Tools for representing MongoDB regular expressions."""
from __future__ import annotations

import re
from typing import Any, Generic, Pattern, Type, TypeVar, Union

from bson._helpers import _getstate_slots, _setstate_slots
from bson.son import RE_TYPE


def str_flags_to_int(str_flags: str) -> int:
    flags = 0
    if "i" in str_flags:
        flags |= re.IGNORECASE
    if "l" in str_flags:
        flags |= re.LOCALE
    if "m" in str_flags:
        flags |= re.MULTILINE
    if "s" in str_flags:
        flags |= re.DOTALL
    if "u" in str_flags:
        flags |= re.UNICODE
    if "x" in str_flags:
        flags |= re.VERBOSE

    return flags


_T = TypeVar("_T", str, bytes)


class Regex(Generic[_T]):
    """BSON regular expression data."""

    __slots__ = ("pattern", "flags")

    __getstate__ = _getstate_slots
    __setstate__ = _setstate_slots

    _type_marker = 11

    @classmethod
    def from_native(cls: Type[Regex[Any]], regex: Pattern[_T]) -> Regex[_T]:
        """Convert a Python regular expression into a ``Regex`` instance.

        Note that in Python 3, a regular expression compiled from a
        :class:`str` has the ``re.UNICODE`` flag set. If it is undesirable
        to store this flag in a BSON regular expression, unset it first::

          >>> pattern = re.compile('.*')
          >>> regex = Regex.from_native(pattern)
          >>> regex.flags ^= re.UNICODE
          >>> db.collection.insert_one({'pattern': regex})

        :param regex: A regular expression object from ``re.compile()``.

        .. warning::
           Python regular expressions use a different syntax and different
           set of flags than MongoDB, which uses `PCRE`_. A regular
           expression retrieved from the server may not compile in
           Python, or may match a different set of strings in Python than
           when used in a MongoDB query.

        .. _PCRE: http://www.pcre.org/
        """
        if not isinstance(regex, RE_TYPE):
            raise TypeError("regex must be a compiled regular expression, not %s" % type(regex))

        return Regex(regex.pattern, regex.flags)

    def __init__(self, pattern: _T, flags: Union[str, int] = 0) -> None:
        """BSON regular expression data.

        This class is useful to store and retrieve regular expressions that are
        incompatible with Python's regular expression dialect.

        :param pattern: string
        :param flags: an integer bitmask, or a string of flag
            characters like "im" for IGNORECASE and MULTILINE
        """
        if not isinstance(pattern, (str, bytes)):
            raise TypeError("pattern must be a string, not %s" % type(pattern))
        self.pattern: _T = pattern

        if isinstance(flags, str):
            self.flags = str_flags_to_int(flags)
        elif isinstance(flags, int):
            self.flags = flags
        else:
            raise TypeError("flags must be a string or int, not %s" % type(flags))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Regex):
            return self.pattern == other.pattern and self.flags == other.flags
        else:
            return NotImplemented

    __hash__ = None  # type: ignore

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __repr__(self) -> str:
        return f"Regex({self.pattern!r}, {self.flags!r})"

    def try_compile(self) -> Pattern[_T]:
        """Compile this :class:`Regex` as a Python regular expression.

        .. warning::
           Python regular expressions use a different syntax and different
           set of flags than MongoDB, which uses `PCRE`_. A regular
           expression retrieved from the server may not compile in
           Python, or may match a different set of strings in Python than
           when used in a MongoDB query. :meth:`try_compile()` may raise
           :exc:`re.error`.

        .. _PCRE: http://www.pcre.org/
        """
        return re.compile(self.pattern, self.flags)
