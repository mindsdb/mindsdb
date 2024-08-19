# Copyright 2009-present MongoDB, Inc.
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

"""Tools for creating and manipulating SON, the Serialized Ocument Notation.

Regular dictionaries can be used instead of SON objects, but not when the order
of keys is important. A SON object can be used just like a normal Python
dictionary.
"""
from __future__ import annotations

import copy
import re
from collections.abc import Mapping as _Mapping
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Pattern,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

# This sort of sucks, but seems to be as good as it gets...
# This is essentially the same as re._pattern_type
RE_TYPE: Type[Pattern[Any]] = type(re.compile(""))

_Key = TypeVar("_Key")
_Value = TypeVar("_Value")
_T = TypeVar("_T")


class SON(Dict[_Key, _Value]):
    """SON data.

    A subclass of dict that maintains ordering of keys and provides a
    few extra niceties for dealing with SON. SON provides an API
    similar to collections.OrderedDict.
    """

    __keys: list[Any]

    def __init__(
        self,
        data: Optional[Union[Mapping[_Key, _Value], Iterable[Tuple[_Key, _Value]]]] = None,
        **kwargs: Any,
    ) -> None:
        self.__keys = []
        dict.__init__(self)
        self.update(data)
        self.update(kwargs)

    def __new__(cls: Type[SON[_Key, _Value]], *args: Any, **kwargs: Any) -> SON[_Key, _Value]:
        instance = super().__new__(cls, *args, **kwargs)  # type: ignore[type-var]
        instance.__keys = []
        return instance

    def __repr__(self) -> str:
        result = []
        for key in self.__keys:
            result.append(f"({key!r}, {self[key]!r})")
        return "SON([%s])" % ", ".join(result)

    def __setitem__(self, key: _Key, value: _Value) -> None:
        if key not in self.__keys:
            self.__keys.append(key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key: _Key) -> None:
        self.__keys.remove(key)
        dict.__delitem__(self, key)

    def copy(self) -> SON[_Key, _Value]:
        other: SON[_Key, _Value] = SON()
        other.update(self)
        return other

    # TODO this is all from UserDict.DictMixin. it could probably be made more
    # efficient.
    # second level definitions support higher levels
    def __iter__(self) -> Iterator[_Key]:
        yield from self.__keys

    def has_key(self, key: _Key) -> bool:
        return key in self.__keys

    def iterkeys(self) -> Iterator[_Key]:
        return self.__iter__()

    # fourth level uses definitions from lower levels
    def itervalues(self) -> Iterator[_Value]:
        for _, v in self.items():
            yield v

    def values(self) -> list[_Value]:  # type: ignore[override]
        return [v for _, v in self.items()]

    def clear(self) -> None:
        self.__keys = []
        super().clear()

    def setdefault(self, key: _Key, default: _Value) -> _Value:
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

    def pop(self, key: _Key, *args: Union[_Value, _T]) -> Union[_Value, _T]:
        if len(args) > 1:
            raise TypeError("pop expected at most 2 arguments, got " + repr(1 + len(args)))
        try:
            value = self[key]
        except KeyError:
            if args:
                return args[0]
            raise
        del self[key]
        return value

    def popitem(self) -> Tuple[_Key, _Value]:
        try:
            k, v = next(iter(self.items()))
        except StopIteration:
            raise KeyError("container is empty") from None
        del self[k]
        return (k, v)

    def update(self, other: Optional[Any] = None, **kwargs: _Value) -> None:  # type: ignore[override]
        # Make progressively weaker assumptions about "other"
        if other is None:
            pass
        elif hasattr(other, "items"):
            for k, v in other.items():
                self[k] = v
        elif hasattr(other, "keys"):
            for k in other:
                self[k] = other[k]
        else:
            for k, v in other:
                self[k] = v
        if kwargs:
            self.update(kwargs)

    def get(  # type: ignore[override]
        self, key: _Key, default: Optional[Union[_Value, _T]] = None
    ) -> Union[_Value, _T, None]:
        try:
            return self[key]
        except KeyError:
            return default

    def __eq__(self, other: Any) -> bool:
        """Comparison to another SON is order-sensitive while comparison to a
        regular dictionary is order-insensitive.
        """
        if isinstance(other, SON):
            return len(self) == len(other) and list(self.items()) == list(other.items())
        return cast(bool, self.to_dict() == other)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __len__(self) -> int:
        return len(self.__keys)

    def to_dict(self) -> dict[_Key, _Value]:
        """Convert a SON document to a normal Python dictionary instance.

        This is trickier than just *dict(...)* because it needs to be
        recursive.
        """

        def transform_value(value: Any) -> Any:
            if isinstance(value, list):
                return [transform_value(v) for v in value]
            elif isinstance(value, _Mapping):
                return {k: transform_value(v) for k, v in value.items()}
            else:
                return value

        return cast("dict[_Key, _Value]", transform_value(dict(self)))

    def __deepcopy__(self, memo: dict[int, SON[_Key, _Value]]) -> SON[_Key, _Value]:
        out: SON[_Key, _Value] = SON()
        val_id = id(self)
        if val_id in memo:
            return memo[val_id]
        memo[val_id] = out
        for k, v in self.items():
            if not isinstance(v, RE_TYPE):
                v = copy.deepcopy(v, memo)  # noqa: PLW2901
            out[k] = v
        return out
