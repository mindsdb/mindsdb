# Copyright 2021-present MongoDB, Inc.
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

"""Setstate and getstate functions for objects with __slots__, allowing
compatibility with default pickling protocol
"""
from __future__ import annotations

from typing import Any, Mapping


def _setstate_slots(self: Any, state: Any) -> None:
    for slot, value in state.items():
        setattr(self, slot, value)


def _mangle_name(name: str, prefix: str) -> str:
    if name.startswith("__"):
        prefix = "_" + prefix
    else:
        prefix = ""
    return prefix + name


def _getstate_slots(self: Any) -> Mapping[Any, Any]:
    prefix = self.__class__.__name__
    ret = {}
    for name in self.__slots__:
        mangled_name = _mangle_name(name, prefix)
        if hasattr(self, mangled_name):
            ret[mangled_name] = getattr(self, mangled_name)
    return ret
