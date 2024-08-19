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

"""Timezone related utilities for BSON."""
from __future__ import annotations

from datetime import datetime, timedelta, tzinfo
from typing import Optional, Tuple, Union

ZERO: timedelta = timedelta(0)


class FixedOffset(tzinfo):
    """Fixed offset timezone, in minutes east from UTC.

    Implementation based from the Python `standard library documentation
    <http://docs.python.org/library/datetime.html#tzinfo-objects>`_.
    Defining __getinitargs__ enables pickling / copying.
    """

    def __init__(self, offset: Union[float, timedelta], name: str) -> None:
        if isinstance(offset, timedelta):
            self.__offset = offset
        else:
            self.__offset = timedelta(minutes=offset)
        self.__name = name

    def __getinitargs__(self) -> Tuple[timedelta, str]:
        return self.__offset, self.__name

    def utcoffset(self, dt: Optional[datetime]) -> timedelta:
        return self.__offset

    def tzname(self, dt: Optional[datetime]) -> str:
        return self.__name

    def dst(self, dt: Optional[datetime]) -> timedelta:
        return ZERO


utc: FixedOffset = FixedOffset(0, "UTC")
"""Fixed offset timezone representing UTC."""
