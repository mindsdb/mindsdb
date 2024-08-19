# Copyright 2014-2015 MongoDB, Inc.
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

"""A BSON wrapper for long (int in python3)"""
from __future__ import annotations

from typing import Any


class Int64(int):
    """Representation of the BSON int64 type.

    This is necessary because every integral number is an :class:`int` in
    Python 3. Small integral numbers are encoded to BSON int32 by default,
    but Int64 numbers will always be encoded to BSON int64.

    :param value: the numeric value to represent
    """

    __slots__ = ()

    _type_marker = 18

    def __getstate__(self) -> Any:
        return {}

    def __setstate__(self, state: Any) -> None:
        pass
