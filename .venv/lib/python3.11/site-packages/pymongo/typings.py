# Copyright 2022-Present MongoDB, Inc.
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

"""Type aliases used by PyMongo"""
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from bson.typings import _DocumentOut, _DocumentType, _DocumentTypeArg

if TYPE_CHECKING:
    from pymongo.collation import Collation


# Common Shared Types.
_Address = Tuple[str, Optional[int]]
_CollationIn = Union[Mapping[str, Any], "Collation"]
_Pipeline = Sequence[Mapping[str, Any]]
ClusterTime = Mapping[str, Any]

_T = TypeVar("_T")


def strip_optional(elem: Optional[_T]) -> _T:
    """This function is to allow us to cast all of the elements of an iterator from Optional[_T] to _T
    while inside a list comprehension.
    """
    assert elem is not None
    return elem


__all__ = [
    "_DocumentOut",
    "_DocumentType",
    "_DocumentTypeArg",
    "_Address",
    "_CollationIn",
    "_Pipeline",
    "strip_optional",
]
