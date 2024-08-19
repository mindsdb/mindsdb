# Copyright 2009-2015 MongoDB, Inc.
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

"""Tools for manipulating DBRefs (references to MongoDB documents)."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping, Optional

from bson._helpers import _getstate_slots, _setstate_slots
from bson.son import SON


class DBRef:
    """A reference to a document stored in MongoDB."""

    __slots__ = "__collection", "__id", "__database", "__kwargs"
    __getstate__ = _getstate_slots
    __setstate__ = _setstate_slots
    # DBRef isn't actually a BSON "type" so this number was arbitrarily chosen.
    _type_marker = 100

    def __init__(
        self,
        collection: str,
        id: Any,
        database: Optional[str] = None,
        _extra: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a new :class:`DBRef`.

        Raises :class:`TypeError` if `collection` or `database` is not
        an instance of :class:`str`. `database` is optional and allows
        references to documents to work across databases. Any additional
        keyword arguments will create additional fields in the resultant
        embedded document.

        :param collection: name of the collection the document is stored in
        :param id: the value of the document's ``"_id"`` field
        :param database: name of the database to reference
        :param kwargs: additional keyword arguments will
            create additional, custom fields

        .. seealso:: The MongoDB documentation on `dbrefs <https://dochub.mongodb.org/core/dbrefs>`_.
        """
        if not isinstance(collection, str):
            raise TypeError("collection must be an instance of str")
        if database is not None and not isinstance(database, str):
            raise TypeError("database must be an instance of str")

        self.__collection = collection
        self.__id = id
        self.__database = database
        kwargs.update(_extra or {})
        self.__kwargs = kwargs

    @property
    def collection(self) -> str:
        """Get the name of this DBRef's collection."""
        return self.__collection

    @property
    def id(self) -> Any:
        """Get this DBRef's _id."""
        return self.__id

    @property
    def database(self) -> Optional[str]:
        """Get the name of this DBRef's database.

        Returns None if this DBRef doesn't specify a database.
        """
        return self.__database

    def __getattr__(self, key: Any) -> Any:
        try:
            return self.__kwargs[key]
        except KeyError:
            raise AttributeError(key) from None

    def as_doc(self) -> SON[str, Any]:
        """Get the SON document representation of this DBRef.

        Generally not needed by application developers
        """
        doc = SON([("$ref", self.collection), ("$id", self.id)])
        if self.database is not None:
            doc["$db"] = self.database
        doc.update(self.__kwargs)
        return doc

    def __repr__(self) -> str:
        extra = "".join([f", {k}={v!r}" for k, v in self.__kwargs.items()])
        if self.database is None:
            return f"DBRef({self.collection!r}, {self.id!r}{extra})"
        return f"DBRef({self.collection!r}, {self.id!r}, {self.database!r}{extra})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DBRef):
            us = (self.__database, self.__collection, self.__id, self.__kwargs)
            them = (other.__database, other.__collection, other.__id, other.__kwargs)
            return us == them
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        """Get a hash value for this :class:`DBRef`."""
        return hash(
            (self.__collection, self.__id, self.__database, tuple(sorted(self.__kwargs.items())))
        )

    def __deepcopy__(self, memo: Any) -> DBRef:
        """Support function for `copy.deepcopy()`."""
        return DBRef(
            deepcopy(self.__collection, memo),
            deepcopy(self.__id, memo),
            deepcopy(self.__database, memo),
            deepcopy(self.__kwargs, memo),
        )
