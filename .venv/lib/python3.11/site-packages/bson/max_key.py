# Copyright 2010-present MongoDB, Inc.
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

"""Representation for the MongoDB internal MaxKey type."""
from __future__ import annotations

from typing import Any


class MaxKey:
    """MongoDB internal MaxKey type."""

    __slots__ = ()

    _type_marker = 127

    def __getstate__(self) -> Any:
        return {}

    def __setstate__(self, state: Any) -> None:
        pass

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MaxKey)

    def __hash__(self) -> int:
        return hash(self._type_marker)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __le__(self, other: Any) -> bool:
        return isinstance(other, MaxKey)

    def __lt__(self, dummy: Any) -> bool:
        return False

    def __ge__(self, dummy: Any) -> bool:
        return True

    def __gt__(self, other: Any) -> bool:
        return not isinstance(other, MaxKey)

    def __repr__(self) -> str:
        return "MaxKey()"
