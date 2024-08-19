# Copyright 2018-present MongoDB, Inc.
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

"""Advanced options for MongoDB drivers implemented on top of PyMongo."""
from __future__ import annotations

from collections import namedtuple
from typing import Optional


class DriverInfo(namedtuple("DriverInfo", ["name", "version", "platform"])):
    """Info about a driver wrapping PyMongo.

    The MongoDB server logs PyMongo's name, version, and platform whenever
    PyMongo establishes a connection. A driver implemented on top of PyMongo
    can add its own info to this log message. Initialize with three strings
    like 'MyDriver', '1.2.3', 'some platform info'. Any of these strings may be
    None to accept PyMongo's default.
    """

    def __new__(
        cls, name: str, version: Optional[str] = None, platform: Optional[str] = None
    ) -> DriverInfo:
        self = super().__new__(cls, name, version, platform)
        for key, value in self._asdict().items():
            if value is not None and not isinstance(value, str):
                raise TypeError(
                    f"Wrong type for DriverInfo {key} option, value must be an instance of str"
                )

        return self
