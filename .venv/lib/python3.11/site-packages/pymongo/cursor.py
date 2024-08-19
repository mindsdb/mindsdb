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

"""Cursor class to iterate over Mongo query results."""
from __future__ import annotations

import copy
import warnings
from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    List,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

from bson import RE_TYPE, _convert_raw_document_lists_to_streams
from bson.code import Code
from bson.son import SON
from pymongo import helpers
from pymongo.collation import validate_collation_or_none
from pymongo.common import (
    validate_is_document_type,
    validate_is_mapping,
)
from pymongo.errors import ConnectionFailure, InvalidOperation, OperationFailure
from pymongo.lock import _create_lock
from pymongo.message import (
    _CursorAddress,
    _GetMore,
    _OpMsg,
    _OpReply,
    _Query,
    _RawBatchGetMore,
    _RawBatchQuery,
)
from pymongo.response import PinnedResponse
from pymongo.typings import _Address, _CollationIn, _DocumentOut, _DocumentType
from pymongo.write_concern import validate_boolean

if TYPE_CHECKING:
    from _typeshed import SupportsItems

    from bson.codec_options import CodecOptions
    from pymongo.client_session import ClientSession
    from pymongo.collection import Collection
    from pymongo.pool import Connection
    from pymongo.read_preferences import _ServerMode


# These errors mean that the server has already killed the cursor so there is
# no need to send killCursors.
_CURSOR_CLOSED_ERRORS = frozenset(
    [
        43,  # CursorNotFound
        175,  # QueryPlanKilled
        237,  # CursorKilled
        # On a tailable cursor, the following errors mean the capped collection
        # rolled over.
        # MongoDB 2.6:
        # {'$err': 'Runner killed during getMore', 'code': 28617, 'ok': 0}
        28617,
        # MongoDB 3.0:
        # {'$err': 'getMore executor error: UnknownError no details available',
        #  'code': 17406, 'ok': 0}
        17406,
        # MongoDB 3.2 + 3.4:
        # {'ok': 0.0, 'errmsg': 'GetMore command executor error:
        #  CappedPositionLost: CollectionScan died due to failure to restore
        #  tailable cursor position. Last seen record id: RecordId(3)',
        #  'code': 96}
        96,
        # MongoDB 3.6+:
        # {'ok': 0.0, 'errmsg': 'errmsg: "CollectionScan died due to failure to
        #  restore tailable cursor position. Last seen record id: RecordId(3)"',
        #  'code': 136, 'codeName': 'CappedPositionLost'}
        136,
    ]
)

_QUERY_OPTIONS = {
    "tailable_cursor": 2,
    "secondary_okay": 4,
    "oplog_replay": 8,
    "no_timeout": 16,
    "await_data": 32,
    "exhaust": 64,
    "partial": 128,
}


class CursorType:
    NON_TAILABLE = 0
    """The standard cursor type."""

    TAILABLE = _QUERY_OPTIONS["tailable_cursor"]
    """The tailable cursor type.

    Tailable cursors are only for use with capped collections. They are not
    closed when the last data is retrieved but are kept open and the cursor
    location marks the final document position. If more data is received
    iteration of the cursor will continue from the last document received.
    """

    TAILABLE_AWAIT = TAILABLE | _QUERY_OPTIONS["await_data"]
    """A tailable cursor with the await option set.

    Creates a tailable cursor that will wait for a few seconds after returning
    the full result set so that it can capture and return additional data added
    during the query.
    """

    EXHAUST = _QUERY_OPTIONS["exhaust"]
    """An exhaust cursor.

    MongoDB will stream batched results to the client without waiting for the
    client to request each batch, reducing latency.
    """


class _ConnectionManager:
    """Used with exhaust cursors to ensure the connection is returned."""

    def __init__(self, conn: Connection, more_to_come: bool):
        self.conn: Optional[Connection] = conn
        self.more_to_come = more_to_come
        self.lock = _create_lock()

    def update_exhaust(self, more_to_come: bool) -> None:
        self.more_to_come = more_to_come

    def close(self) -> None:
        """Return this instance's connection to the connection pool."""
        if self.conn:
            self.conn.unpin()
            self.conn = None


_Sort = Union[
    Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]], Mapping[str, Any]
]
_Hint = Union[str, _Sort]


class Cursor(Generic[_DocumentType]):
    """A cursor / iterator over Mongo query results."""

    _query_class = _Query
    _getmore_class = _GetMore

    def __init__(
        self,
        collection: Collection[_DocumentType],
        filter: Optional[Mapping[str, Any]] = None,
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        skip: int = 0,
        limit: int = 0,
        no_cursor_timeout: bool = False,
        cursor_type: int = CursorType.NON_TAILABLE,
        sort: Optional[_Sort] = None,
        allow_partial_results: bool = False,
        oplog_replay: bool = False,
        batch_size: int = 0,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_Hint] = None,
        max_scan: Optional[int] = None,
        max_time_ms: Optional[int] = None,
        max: Optional[_Sort] = None,
        min: Optional[_Sort] = None,
        return_key: Optional[bool] = None,
        show_record_id: Optional[bool] = None,
        snapshot: Optional[bool] = None,
        comment: Optional[Any] = None,
        session: Optional[ClientSession] = None,
        allow_disk_use: Optional[bool] = None,
        let: Optional[bool] = None,
    ) -> None:
        """Create a new cursor.

        Should not be called directly by application developers - see
        :meth:`~pymongo.collection.Collection.find` instead.

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        # Initialize all attributes used in __del__ before possibly raising
        # an error to avoid attribute errors during garbage collection.
        self.__collection: Collection[_DocumentType] = collection
        self.__id: Any = None
        self.__exhaust = False
        self.__sock_mgr: Any = None
        self.__killed = False
        self.__session: Optional[ClientSession]

        if session:
            self.__session = session
            self.__explicit_session = True
        else:
            self.__session = None
            self.__explicit_session = False

        spec: Mapping[str, Any] = filter or {}
        validate_is_mapping("filter", spec)
        if not isinstance(skip, int):
            raise TypeError("skip must be an instance of int")
        if not isinstance(limit, int):
            raise TypeError("limit must be an instance of int")
        validate_boolean("no_cursor_timeout", no_cursor_timeout)
        if no_cursor_timeout and not self.__explicit_session:
            warnings.warn(
                "use an explicit session with no_cursor_timeout=True "
                "otherwise the cursor may still timeout after "
                "30 minutes, for more info see "
                "https://mongodb.com/docs/v4.4/reference/method/"
                "cursor.noCursorTimeout/"
                "#session-idle-timeout-overrides-nocursortimeout",
                UserWarning,
                stacklevel=2,
            )
        if cursor_type not in (
            CursorType.NON_TAILABLE,
            CursorType.TAILABLE,
            CursorType.TAILABLE_AWAIT,
            CursorType.EXHAUST,
        ):
            raise ValueError("not a valid value for cursor_type")
        validate_boolean("allow_partial_results", allow_partial_results)
        validate_boolean("oplog_replay", oplog_replay)
        if not isinstance(batch_size, int):
            raise TypeError("batch_size must be an integer")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        # Only set if allow_disk_use is provided by the user, else None.
        if allow_disk_use is not None:
            allow_disk_use = validate_boolean("allow_disk_use", allow_disk_use)

        if projection is not None:
            projection = helpers._fields_list_to_dict(projection, "projection")

        if let is not None:
            validate_is_document_type("let", let)

        self.__let = let
        self.__spec = spec
        self.__has_filter = filter is not None
        self.__projection = projection
        self.__skip = skip
        self.__limit = limit
        self.__batch_size = batch_size
        self.__ordering = sort and helpers._index_document(sort) or None
        self.__max_scan = max_scan
        self.__explain = False
        self.__comment = comment
        self.__max_time_ms = max_time_ms
        self.__max_await_time_ms: Optional[int] = None
        self.__max: Optional[Union[dict[Any, Any], _Sort]] = max
        self.__min: Optional[Union[dict[Any, Any], _Sort]] = min
        self.__collation = validate_collation_or_none(collation)
        self.__return_key = return_key
        self.__show_record_id = show_record_id
        self.__allow_disk_use = allow_disk_use
        self.__snapshot = snapshot
        self.__hint: Union[str, dict[str, Any], None]
        self.__set_hint(hint)

        # Exhaust cursor support
        if cursor_type == CursorType.EXHAUST:
            if self.__collection.database.client.is_mongos:
                raise InvalidOperation("Exhaust cursors are not supported by mongos")
            if limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            self.__exhaust = True

        # This is ugly. People want to be able to do cursor[5:5] and
        # get an empty result set (old behavior was an
        # exception). It's hard to do that right, though, because the
        # server uses limit(0) to mean 'no limit'. So we set __empty
        # in that case and check for it when iterating. We also unset
        # it anytime we change __limit.
        self.__empty = False

        self.__data: deque = deque()
        self.__address: Optional[_Address] = None
        self.__retrieved = 0

        self.__codec_options = collection.codec_options
        # Read preference is set when the initial find is sent.
        self.__read_preference: Optional[_ServerMode] = None
        self.__read_concern = collection.read_concern

        self.__query_flags = cursor_type
        if no_cursor_timeout:
            self.__query_flags |= _QUERY_OPTIONS["no_timeout"]
        if allow_partial_results:
            self.__query_flags |= _QUERY_OPTIONS["partial"]
        if oplog_replay:
            self.__query_flags |= _QUERY_OPTIONS["oplog_replay"]

        # The namespace to use for find/getMore commands.
        self.__dbname = collection.database.name
        self.__collname = collection.name

    @property
    def collection(self) -> Collection[_DocumentType]:
        """The :class:`~pymongo.collection.Collection` that this
        :class:`Cursor` is iterating.
        """
        return self.__collection

    @property
    def retrieved(self) -> int:
        """The number of documents retrieved so far."""
        return self.__retrieved

    def __del__(self) -> None:
        self.__die()

    def rewind(self) -> Cursor[_DocumentType]:
        """Rewind this cursor to its unevaluated state.

        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to
        be sent to the server, even if the resultant data has already been
        retrieved by this cursor.
        """
        self.close()
        self.__data = deque()
        self.__id = None
        self.__address = None
        self.__retrieved = 0
        self.__killed = False

        return self

    def clone(self) -> Cursor[_DocumentType]:
        """Get a clone of this cursor.

        Returns a new Cursor instance with options matching those that have
        been set on the current instance. The clone will be completely
        unevaluated, even if the current instance has been partially or
        completely evaluated.
        """
        return self._clone(True)

    def _clone(self, deepcopy: bool = True, base: Optional[Cursor] = None) -> Cursor:
        """Internal clone helper."""
        if not base:
            if self.__explicit_session:
                base = self._clone_base(self.__session)
            else:
                base = self._clone_base(None)

        values_to_clone = (
            "spec",
            "projection",
            "skip",
            "limit",
            "max_time_ms",
            "max_await_time_ms",
            "comment",
            "max",
            "min",
            "ordering",
            "explain",
            "hint",
            "batch_size",
            "max_scan",
            "query_flags",
            "collation",
            "empty",
            "show_record_id",
            "return_key",
            "allow_disk_use",
            "snapshot",
            "exhaust",
            "has_filter",
        )
        data = {
            k: v
            for k, v in self.__dict__.items()
            if k.startswith("_Cursor__") and k[9:] in values_to_clone
        }
        if deepcopy:
            data = self._deepcopy(data)
        base.__dict__.update(data)
        return base

    def _clone_base(self, session: Optional[ClientSession]) -> Cursor:
        """Creates an empty Cursor object for information to be copied into."""
        return self.__class__(self.__collection, session=session)

    def __die(self, synchronous: bool = False) -> None:
        """Closes this cursor."""
        try:
            already_killed = self.__killed
        except AttributeError:
            # __init__ did not run to completion (or at all).
            return

        self.__killed = True
        if self.__id and not already_killed:
            cursor_id = self.__id
            assert self.__address is not None
            address = _CursorAddress(self.__address, f"{self.__dbname}.{self.__collname}")
        else:
            # Skip killCursors.
            cursor_id = 0
            address = None
        self.__collection.database.client._cleanup_cursor(
            synchronous,
            cursor_id,
            address,
            self.__sock_mgr,
            self.__session,
            self.__explicit_session,
        )
        if not self.__explicit_session:
            self.__session = None
        self.__sock_mgr = None

    def close(self) -> None:
        """Explicitly close / kill this cursor."""
        self.__die(True)

    def __query_spec(self) -> Mapping[str, Any]:
        """Get the spec to use for a query."""
        operators: dict[str, Any] = {}
        if self.__ordering:
            operators["$orderby"] = self.__ordering
        if self.__explain:
            operators["$explain"] = True
        if self.__hint:
            operators["$hint"] = self.__hint
        if self.__let:
            operators["let"] = self.__let
        if self.__comment:
            operators["$comment"] = self.__comment
        if self.__max_scan:
            operators["$maxScan"] = self.__max_scan
        if self.__max_time_ms is not None:
            operators["$maxTimeMS"] = self.__max_time_ms
        if self.__max:
            operators["$max"] = self.__max
        if self.__min:
            operators["$min"] = self.__min
        if self.__return_key is not None:
            operators["$returnKey"] = self.__return_key
        if self.__show_record_id is not None:
            # This is upgraded to showRecordId for MongoDB 3.2+ "find" command.
            operators["$showDiskLoc"] = self.__show_record_id
        if self.__snapshot is not None:
            operators["$snapshot"] = self.__snapshot

        if operators:
            # Make a shallow copy so we can cleanly rewind or clone.
            spec = dict(self.__spec)

            # Allow-listed commands must be wrapped in $query.
            if "$query" not in spec:
                # $query has to come first
                spec = {"$query": spec}

            spec.update(operators)
            return spec
        # Have to wrap with $query if "query" is the first key.
        # We can't just use $query anytime "query" is a key as
        # that breaks commands like count and find_and_modify.
        # Checking spec.keys()[0] covers the case that the spec
        # was passed as an instance of SON or OrderedDict.
        elif "query" in self.__spec and (
            len(self.__spec) == 1 or next(iter(self.__spec)) == "query"
        ):
            return {"$query": self.__spec}

        return self.__spec

    def __check_okay_to_chain(self) -> None:
        """Check if it is okay to chain more options onto this cursor."""
        if self.__retrieved or self.__id is not None:
            raise InvalidOperation("cannot set options after executing query")

    def add_option(self, mask: int) -> Cursor[_DocumentType]:
        """Set arbitrary query flags using a bitmask.

        To set the tailable flag:
        cursor.add_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        if mask & _QUERY_OPTIONS["exhaust"]:
            if self.__limit:
                raise InvalidOperation("Can't use limit and exhaust together.")
            if self.__collection.database.client.is_mongos:
                raise InvalidOperation("Exhaust cursors are not supported by mongos")
            self.__exhaust = True

        self.__query_flags |= mask
        return self

    def remove_option(self, mask: int) -> Cursor[_DocumentType]:
        """Unset arbitrary query flags using a bitmask.

        To unset the tailable flag:
        cursor.remove_option(2)
        """
        if not isinstance(mask, int):
            raise TypeError("mask must be an int")
        self.__check_okay_to_chain()

        if mask & _QUERY_OPTIONS["exhaust"]:
            self.__exhaust = False

        self.__query_flags &= ~mask
        return self

    def allow_disk_use(self, allow_disk_use: bool) -> Cursor[_DocumentType]:
        """Specifies whether MongoDB can use temporary disk files while
        processing a blocking sort operation.

        Raises :exc:`TypeError` if `allow_disk_use` is not a boolean.

        .. note:: `allow_disk_use` requires server version **>= 4.4**

        :param allow_disk_use: if True, MongoDB may use temporary
            disk files to store data exceeding the system memory limit while
            processing a blocking sort operation.

        .. versionadded:: 3.11
        """
        if not isinstance(allow_disk_use, bool):
            raise TypeError("allow_disk_use must be a bool")
        self.__check_okay_to_chain()

        self.__allow_disk_use = allow_disk_use
        return self

    def limit(self, limit: int) -> Cursor[_DocumentType]:
        """Limits the number of results to be returned by this cursor.

        Raises :exc:`TypeError` if `limit` is not an integer. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor`
        has already been used. The last `limit` applied to this cursor
        takes precedence. A limit of ``0`` is equivalent to no limit.

        :param limit: the number of results to return

        .. seealso:: The MongoDB documentation on `limit <https://dochub.mongodb.org/core/limit>`_.
        """
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer")
        if self.__exhaust:
            raise InvalidOperation("Can't use limit and exhaust together.")
        self.__check_okay_to_chain()

        self.__empty = False
        self.__limit = limit
        return self

    def batch_size(self, batch_size: int) -> Cursor[_DocumentType]:
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :exc:`TypeError` if `batch_size` is not an integer.
        Raises :exc:`ValueError` if `batch_size` is less than ``0``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. The last `batch_size`
        applied to this cursor takes precedence.

        :param batch_size: The size of each batch of results requested.
        """
        if not isinstance(batch_size, int):
            raise TypeError("batch_size must be an integer")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")
        self.__check_okay_to_chain()

        self.__batch_size = batch_size
        return self

    def skip(self, skip: int) -> Cursor[_DocumentType]:
        """Skips the first `skip` results of this cursor.

        Raises :exc:`TypeError` if `skip` is not an integer. Raises
        :exc:`ValueError` if `skip` is less than ``0``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor` has
        already been used. The last `skip` applied to this cursor takes
        precedence.

        :param skip: the number of results to skip
        """
        if not isinstance(skip, int):
            raise TypeError("skip must be an integer")
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self.__check_okay_to_chain()

        self.__skip = skip
        return self

    def max_time_ms(self, max_time_ms: Optional[int]) -> Cursor[_DocumentType]:
        """Specifies a time limit for a query operation. If the specified
        time is exceeded, the operation will be aborted and
        :exc:`~pymongo.errors.ExecutionTimeout` is raised. If `max_time_ms`
        is ``None`` no limit is applied.

        Raises :exc:`TypeError` if `max_time_ms` is not an integer or ``None``.
        Raises :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor`
        has already been used.

        :param max_time_ms: the time limit after which the operation is aborted
        """
        if not isinstance(max_time_ms, int) and max_time_ms is not None:
            raise TypeError("max_time_ms must be an integer or None")
        self.__check_okay_to_chain()

        self.__max_time_ms = max_time_ms
        return self

    def max_await_time_ms(self, max_await_time_ms: Optional[int]) -> Cursor[_DocumentType]:
        """Specifies a time limit for a getMore operation on a
        :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` cursor. For all other
        types of cursor max_await_time_ms is ignored.

        Raises :exc:`TypeError` if `max_await_time_ms` is not an integer or
        ``None``. Raises :exc:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used.

        .. note:: `max_await_time_ms` requires server version **>= 3.2**

        :param max_await_time_ms: the time limit after which the operation is
            aborted

        .. versionadded:: 3.2
        """
        if not isinstance(max_await_time_ms, int) and max_await_time_ms is not None:
            raise TypeError("max_await_time_ms must be an integer or None")
        self.__check_okay_to_chain()

        # Ignore max_await_time_ms if not tailable or await_data is False.
        if self.__query_flags & CursorType.TAILABLE_AWAIT:
            self.__max_await_time_ms = max_await_time_ms

        return self

    @overload
    def __getitem__(self, index: int) -> _DocumentType:
        ...

    @overload
    def __getitem__(self, index: slice) -> Cursor[_DocumentType]:
        ...

    def __getitem__(self, index: Union[int, slice]) -> Union[_DocumentType, Cursor[_DocumentType]]:
        """Get a single document or a slice of documents from this cursor.

        .. warning:: A :class:`~Cursor` is not a Python :class:`list`. Each
          index access or slice requires that a new query be run using skip
          and limit. Do not iterate the cursor using index accesses.
          The following example is **extremely inefficient** and may return
          surprising results::

            cursor = db.collection.find()
            # Warning: This runs a new query for each document.
            # Don't do this!
            for idx in range(10):
                print(cursor[idx])

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used.

        To get a single document use an integral index, e.g.::

          >>> db.test.find()[50]

        An :class:`IndexError` will be raised if the index is negative
        or greater than the amount of documents in this cursor. Any
        limit previously applied to this cursor will be ignored.

        To get a slice of documents use a slice index, e.g.::

          >>> db.test.find()[20:25]

        This will return this cursor with a limit of ``5`` and skip of
        ``20`` applied.  Using a slice index will override any prior
        limits or skips applied to this cursor (including those
        applied through previous calls to this method). Raises
        :class:`IndexError` when the slice has a step, a negative
        start value, or a stop value less than or equal to the start
        value.

        :param index: An integer or slice index to be applied to this cursor
        """
        self.__check_okay_to_chain()
        self.__empty = False
        if isinstance(index, slice):
            if index.step is not None:
                raise IndexError("Cursor instances do not support slice steps")

            skip = 0
            if index.start is not None:
                if index.start < 0:
                    raise IndexError("Cursor instances do not support negative indices")
                skip = index.start

            if index.stop is not None:
                limit = index.stop - skip
                if limit < 0:
                    raise IndexError(
                        "stop index must be greater than start index for slice %r" % index
                    )
                if limit == 0:
                    self.__empty = True
            else:
                limit = 0

            self.__skip = skip
            self.__limit = limit
            return self

        if isinstance(index, int):
            if index < 0:
                raise IndexError("Cursor instances do not support negative indices")
            clone = self.clone()
            clone.skip(index + self.__skip)
            clone.limit(-1)  # use a hard limit
            clone.__query_flags &= ~CursorType.TAILABLE_AWAIT  # PYTHON-1371
            for doc in clone:
                return doc
            raise IndexError("no such item for Cursor instance")
        raise TypeError("index %r cannot be applied to Cursor instances" % index)

    def max_scan(self, max_scan: Optional[int]) -> Cursor[_DocumentType]:
        """**DEPRECATED** - Limit the number of documents to scan when
        performing the query.

        Raises :class:`~pymongo.errors.InvalidOperation` if this
        cursor has already been used. Only the last :meth:`max_scan`
        applied to this cursor has any effect.

        :param max_scan: the maximum number of documents to scan

        .. versionchanged:: 3.7
          Deprecated :meth:`max_scan`. Support for this option is deprecated in
          MongoDB 4.0. Use :meth:`max_time_ms` instead to limit server side
          execution time.
        """
        self.__check_okay_to_chain()
        self.__max_scan = max_scan
        return self

    def max(self, spec: _Sort) -> Cursor[_DocumentType]:
        """Adds ``max`` operator that specifies upper bound for specific index.

        When using ``max``, :meth:`~hint` should also be configured to ensure
        the query uses the expected index and starting in MongoDB 4.2
        :meth:`~hint` will be required.

        :param spec: a list of field, limit pairs specifying the exclusive
            upper bound for all keys of a specific index in order.

        .. versionchanged:: 3.8
           Deprecated cursors that use ``max`` without a :meth:`~hint`.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError("spec must be an instance of list or tuple")

        self.__check_okay_to_chain()
        self.__max = dict(spec)
        return self

    def min(self, spec: _Sort) -> Cursor[_DocumentType]:
        """Adds ``min`` operator that specifies lower bound for specific index.

        When using ``min``, :meth:`~hint` should also be configured to ensure
        the query uses the expected index and starting in MongoDB 4.2
        :meth:`~hint` will be required.

        :param spec: a list of field, limit pairs specifying the inclusive
            lower bound for all keys of a specific index in order.

        .. versionchanged:: 3.8
           Deprecated cursors that use ``min`` without a :meth:`~hint`.

        .. versionadded:: 2.7
        """
        if not isinstance(spec, (list, tuple)):
            raise TypeError("spec must be an instance of list or tuple")

        self.__check_okay_to_chain()
        self.__min = dict(spec)
        return self

    def sort(
        self, key_or_list: _Hint, direction: Optional[Union[int, str]] = None
    ) -> Cursor[_DocumentType]:
        """Sorts this cursor's results.

        Pass a field name and a direction, either
        :data:`~pymongo.ASCENDING` or :data:`~pymongo.DESCENDING`.::

            for doc in collection.find().sort('field', pymongo.ASCENDING):
                print(doc)

        To sort by multiple fields, pass a list of (key, direction) pairs.
        If just a name is given, :data:`~pymongo.ASCENDING` will be inferred::

            for doc in collection.find().sort([
                    'field1',
                    ('field2', pymongo.DESCENDING)]):
                print(doc)

        Text search results can be sorted by relevance::

            cursor = db.test.find(
                {'$text': {'$search': 'some words'}},
                {'score': {'$meta': 'textScore'}})

            # Sort by 'score' field.
            cursor.sort([('score', {'$meta': 'textScore'})])

            for doc in cursor:
                print(doc)

        For more advanced text search functionality, see MongoDB's
        `Atlas Search <https://docs.atlas.mongodb.com/atlas-search/>`_.

        Raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used. Only the last :meth:`sort` applied to this
        cursor has any effect.

        :param key_or_list: a single key or a list of (key, direction)
            pairs specifying the keys to sort on
        :param direction: only used if `key_or_list` is a single
            key, if not given :data:`~pymongo.ASCENDING` is assumed
        """
        self.__check_okay_to_chain()
        keys = helpers._index_list(key_or_list, direction)
        self.__ordering = helpers._index_document(keys)
        return self

    def distinct(self, key: str) -> list:
        """Get a list of distinct values for `key` among all documents
        in the result set of this query.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`str`.

        The :meth:`distinct` method obeys the
        :attr:`~pymongo.collection.Collection.read_preference` of the
        :class:`~pymongo.collection.Collection` instance on which
        :meth:`~pymongo.collection.Collection.find` was called.

        :param key: name of key for which we want to get the distinct values

        .. seealso:: :meth:`pymongo.collection.Collection.distinct`
        """
        options: dict[str, Any] = {}
        if self.__spec:
            options["query"] = self.__spec
        if self.__max_time_ms is not None:
            options["maxTimeMS"] = self.__max_time_ms
        if self.__comment:
            options["comment"] = self.__comment
        if self.__collation is not None:
            options["collation"] = self.__collation

        return self.__collection.distinct(key, session=self.__session, **options)

    def explain(self) -> _DocumentType:
        """Returns an explain plan record for this cursor.

        .. note:: This method uses the default verbosity mode of the
          `explain command
          <https://mongodb.com/docs/manual/reference/command/explain/>`_,
          ``allPlansExecution``. To use a different verbosity use
          :meth:`~pymongo.database.Database.command` to run the explain
          command directly.

        .. seealso:: The MongoDB documentation on `explain <https://dochub.mongodb.org/core/explain>`_.
        """
        c = self.clone()
        c.__explain = True

        # always use a hard limit for explains
        if c.__limit:
            c.__limit = -abs(c.__limit)
        return next(c)

    def __set_hint(self, index: Optional[_Hint]) -> None:
        if index is None:
            self.__hint = None
            return

        if isinstance(index, str):
            self.__hint = index
        else:
            self.__hint = helpers._index_document(index)

    def hint(self, index: Optional[_Hint]) -> Cursor[_DocumentType]:
        """Adds a 'hint', telling Mongo the proper index to use for the query.

        Judicious use of hints can greatly improve query
        performance. When doing a query on multiple fields (at least
        one of which is indexed) pass the indexed field as a hint to
        the query. Raises :class:`~pymongo.errors.OperationFailure` if the
        provided hint requires an index that does not exist on this collection,
        and raises :class:`~pymongo.errors.InvalidOperation` if this cursor has
        already been used.

        `index` should be an index as passed to
        :meth:`~pymongo.collection.Collection.create_index`
        (e.g. ``[('field', ASCENDING)]``) or the name of the index.
        If `index` is ``None`` any existing hint for this query is
        cleared. The last hint applied to this cursor takes precedence
        over all others.

        :param index: index to hint on (as an index specifier)
        """
        self.__check_okay_to_chain()
        self.__set_hint(index)
        return self

    def comment(self, comment: Any) -> Cursor[_DocumentType]:
        """Adds a 'comment' to the cursor.

        http://mongodb.com/docs/manual/reference/operator/comment/

        :param comment: A string to attach to the query to help interpret and
            trace the operation in the server logs and in profile data.

        .. versionadded:: 2.7
        """
        self.__check_okay_to_chain()
        self.__comment = comment
        return self

    def where(self, code: Union[str, Code]) -> Cursor[_DocumentType]:
        """Adds a `$where`_ clause to this query.

        The `code` argument must be an instance of :class:`str` or
        :class:`~bson.code.Code` containing a JavaScript expression.
        This expression will be evaluated for each document scanned.
        Only those documents for which the expression evaluates to
        *true* will be returned as results. The keyword *this* refers
        to the object currently being scanned. For example::

            # Find all documents where field "a" is less than "b" plus "c".
            for doc in db.test.find().where('this.a < (this.b + this.c)'):
                print(doc)

        Raises :class:`TypeError` if `code` is not an instance of
        :class:`str`. Raises :class:`~pymongo.errors.InvalidOperation` if this
        :class:`Cursor` has already been used. Only the last call to
        :meth:`where` applied to a :class:`Cursor` has any effect.

        .. note:: MongoDB 4.4 drops support for :class:`~bson.code.Code`
          with scope variables. Consider using `$expr`_ instead.

        :param code: JavaScript expression to use as a filter

        .. _$expr: https://mongodb.com/docs/manual/reference/operator/query/expr/
        .. _$where: https://mongodb.com/docs/manual/reference/operator/query/where/
        """
        self.__check_okay_to_chain()
        if not isinstance(code, Code):
            code = Code(code)

        # Avoid overwriting a filter argument that was given by the user
        # when updating the spec.
        spec: dict[str, Any]
        if self.__has_filter:
            spec = dict(self.__spec)
        else:
            spec = cast(dict, self.__spec)
        spec["$where"] = code
        self.__spec = spec
        return self

    def collation(self, collation: Optional[_CollationIn]) -> Cursor[_DocumentType]:
        """Adds a :class:`~pymongo.collation.Collation` to this query.

        Raises :exc:`TypeError` if `collation` is not an instance of
        :class:`~pymongo.collation.Collation` or a ``dict``. Raises
        :exc:`~pymongo.errors.InvalidOperation` if this :class:`Cursor` has
        already been used. Only the last collation applied to this cursor has
        any effect.

        :param collation: An instance of :class:`~pymongo.collation.Collation`.
        """
        self.__check_okay_to_chain()
        self.__collation = validate_collation_or_none(collation)
        return self

    def __send_message(self, operation: Union[_Query, _GetMore]) -> None:
        """Send a query or getmore operation and handles the response.

        If operation is ``None`` this is an exhaust cursor, which reads
        the next result batch off the exhaust socket instead of
        sending getMore messages to the server.

        Can raise ConnectionFailure.
        """
        client = self.__collection.database.client
        # OP_MSG is required to support exhaust cursors with encryption.
        if client._encrypter and self.__exhaust:
            raise InvalidOperation("exhaust cursors do not support auto encryption")

        try:
            response = client._run_operation(
                operation, self._unpack_response, address=self.__address
            )
        except OperationFailure as exc:
            if exc.code in _CURSOR_CLOSED_ERRORS or self.__exhaust:
                # Don't send killCursors because the cursor is already closed.
                self.__killed = True
            if exc.timeout:
                self.__die(False)
            else:
                self.close()
            # If this is a tailable cursor the error is likely
            # due to capped collection roll over. Setting
            # self.__killed to True ensures Cursor.alive will be
            # False. No need to re-raise.
            if (
                exc.code in _CURSOR_CLOSED_ERRORS
                and self.__query_flags & _QUERY_OPTIONS["tailable_cursor"]
            ):
                return
            raise
        except ConnectionFailure:
            self.__killed = True
            self.close()
            raise
        except Exception:
            self.close()
            raise

        self.__address = response.address
        if isinstance(response, PinnedResponse):
            if not self.__sock_mgr:
                self.__sock_mgr = _ConnectionManager(response.conn, response.more_to_come)

        cmd_name = operation.name
        docs = response.docs
        if response.from_command:
            if cmd_name != "explain":
                cursor = docs[0]["cursor"]
                self.__id = cursor["id"]
                if cmd_name == "find":
                    documents = cursor["firstBatch"]
                    # Update the namespace used for future getMore commands.
                    ns = cursor.get("ns")
                    if ns:
                        self.__dbname, self.__collname = ns.split(".", 1)
                else:
                    documents = cursor["nextBatch"]
                self.__data = deque(documents)
                self.__retrieved += len(documents)
            else:
                self.__id = 0
                self.__data = deque(docs)
                self.__retrieved += len(docs)
        else:
            assert isinstance(response.data, _OpReply)
            self.__id = response.data.cursor_id
            self.__data = deque(docs)
            self.__retrieved += response.data.number_returned

        if self.__id == 0:
            # Don't wait for garbage collection to call __del__, return the
            # socket and the session to the pool now.
            self.close()

        if self.__limit and self.__id and self.__limit <= self.__retrieved:
            self.close()

    def _unpack_response(
        self,
        response: Union[_OpReply, _OpMsg],
        cursor_id: Optional[int],
        codec_options: CodecOptions,
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> Sequence[_DocumentOut]:
        return response.unpack_response(cursor_id, codec_options, user_fields, legacy_response)

    def _read_preference(self) -> _ServerMode:
        if self.__read_preference is None:
            # Save the read preference for getMore commands.
            self.__read_preference = self.__collection._read_preference_for(self.session)
        return self.__read_preference

    def _refresh(self) -> int:
        """Refreshes the cursor with more data from Mongo.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        if not self.__session:
            self.__session = self.__collection.database.client._ensure_session()

        if self.__id is None:  # Query
            if (self.__min or self.__max) and not self.__hint:
                raise InvalidOperation(
                    "Passing a 'hint' is required when using the min/max query"
                    " option to ensure the query utilizes the correct index"
                )
            q = self._query_class(
                self.__query_flags,
                self.__collection.database.name,
                self.__collection.name,
                self.__skip,
                self.__query_spec(),
                self.__projection,
                self.__codec_options,
                self._read_preference(),
                self.__limit,
                self.__batch_size,
                self.__read_concern,
                self.__collation,
                self.__session,
                self.__collection.database.client,
                self.__allow_disk_use,
                self.__exhaust,
            )
            self.__send_message(q)
        elif self.__id:  # Get More
            if self.__limit:
                limit = self.__limit - self.__retrieved
                if self.__batch_size:
                    limit = min(limit, self.__batch_size)
            else:
                limit = self.__batch_size
            # Exhaust cursors don't send getMore messages.
            g = self._getmore_class(
                self.__dbname,
                self.__collname,
                limit,
                self.__id,
                self.__codec_options,
                self._read_preference(),
                self.__session,
                self.__collection.database.client,
                self.__max_await_time_ms,
                self.__sock_mgr,
                self.__exhaust,
                self.__comment,
            )
            self.__send_message(g)

        return len(self.__data)

    @property
    def alive(self) -> bool:
        """Does this cursor have the potential to return more data?

        This is mostly useful with `tailable cursors
        <https://www.mongodb.com/docs/manual/core/tailable-cursors/>`_
        since they will stop iterating even though they *may* return more
        results in the future.

        With regular cursors, simply use a for loop instead of :attr:`alive`::

            for doc in collection.find():
                print(doc)

        .. note:: Even if :attr:`alive` is True, :meth:`next` can raise
          :exc:`StopIteration`. :attr:`alive` can also be True while iterating
          a cursor from a failed server. In this case :attr:`alive` will
          return False after :meth:`next` fails to retrieve the next batch
          of results from the server.
        """
        return bool(len(self.__data) or (not self.__killed))

    @property
    def cursor_id(self) -> Optional[int]:
        """Returns the id of the cursor

        .. versionadded:: 2.2
        """
        return self.__id

    @property
    def address(self) -> Optional[tuple[str, Any]]:
        """The (host, port) of the server used, or None.

        .. versionchanged:: 3.0
           Renamed from "conn_id".
        """
        return self.__address

    @property
    def session(self) -> Optional[ClientSession]:
        """The cursor's :class:`~pymongo.client_session.ClientSession`, or None.

        .. versionadded:: 3.6
        """
        if self.__explicit_session:
            return self.__session
        return None

    def __iter__(self) -> Cursor[_DocumentType]:
        return self

    def next(self) -> _DocumentType:
        """Advance the cursor."""
        if self.__empty:
            raise StopIteration
        if len(self.__data) or self._refresh():
            return self.__data.popleft()
        else:
            raise StopIteration

    __next__ = next

    def __enter__(self) -> Cursor[_DocumentType]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def __copy__(self) -> Cursor[_DocumentType]:
        """Support function for `copy.copy()`.

        .. versionadded:: 2.4
        """
        return self._clone(deepcopy=False)

    def __deepcopy__(self, memo: Any) -> Any:
        """Support function for `copy.deepcopy()`.

        .. versionadded:: 2.4
        """
        return self._clone(deepcopy=True)

    @overload
    def _deepcopy(self, x: Iterable, memo: Optional[dict[int, Union[list, dict]]] = None) -> list:
        ...

    @overload
    def _deepcopy(
        self, x: SupportsItems, memo: Optional[dict[int, Union[list, dict]]] = None
    ) -> dict:
        ...

    def _deepcopy(
        self, x: Union[Iterable, SupportsItems], memo: Optional[dict[int, Union[list, dict]]] = None
    ) -> Union[list, dict]:
        """Deepcopy helper for the data dictionary or list.

        Regular expressions cannot be deep copied but as they are immutable we
        don't have to copy them when cloning.
        """
        y: Union[list, dict]
        iterator: Iterable[tuple[Any, Any]]
        if not hasattr(x, "items"):
            y, is_list, iterator = [], True, enumerate(x)
        else:
            y, is_list, iterator = {}, False, cast("SupportsItems", x).items()
        if memo is None:
            memo = {}
        val_id = id(x)
        if val_id in memo:
            return memo[val_id]
        memo[val_id] = y

        for key, value in iterator:
            if isinstance(value, (dict, list)) and not isinstance(value, SON):
                value = self._deepcopy(value, memo)  # noqa: PLW2901
            elif not isinstance(value, RE_TYPE):
                value = copy.deepcopy(value, memo)  # noqa: PLW2901

            if is_list:
                y.append(value)  # type: ignore[union-attr]
            else:
                if not isinstance(key, RE_TYPE):
                    key = copy.deepcopy(key, memo)  # noqa: PLW2901
                y[key] = value
        return y


class RawBatchCursor(Cursor, Generic[_DocumentType]):
    """A cursor / iterator over raw batches of BSON data from a query result."""

    _query_class = _RawBatchQuery
    _getmore_class = _RawBatchGetMore

    def __init__(self, collection: Collection[_DocumentType], *args: Any, **kwargs: Any) -> None:
        """Create a new cursor / iterator over raw batches of BSON data.

        Should not be called directly by application developers -
        see :meth:`~pymongo.collection.Collection.find_raw_batches`
        instead.

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        super().__init__(collection, *args, **kwargs)

    def _unpack_response(
        self,
        response: Union[_OpReply, _OpMsg],
        cursor_id: Optional[int],
        codec_options: CodecOptions[Mapping[str, Any]],
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> list[_DocumentOut]:
        raw_response = response.raw_response(cursor_id, user_fields=user_fields)
        if not legacy_response:
            # OP_MSG returns firstBatch/nextBatch documents as a BSON array
            # Re-assemble the array of documents into a document stream
            _convert_raw_document_lists_to_streams(raw_response[0])
        return cast(List["_DocumentOut"], raw_response)

    def explain(self) -> _DocumentType:
        """Returns an explain plan record for this cursor.

        .. seealso:: The MongoDB documentation on `explain <https://dochub.mongodb.org/core/explain>`_.
        """
        clone = self._clone(deepcopy=True, base=Cursor(self.collection))
        return clone.explain()

    def __getitem__(self, index: Any) -> NoReturn:
        raise InvalidOperation("Cannot call __getitem__ on RawBatchCursor")
