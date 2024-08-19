# Copyright 2014-present MongoDB, Inc.
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

"""CommandCursor class to iterate over command results."""
from __future__ import annotations

from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterator,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    Union,
)

from bson import CodecOptions, _convert_raw_document_lists_to_streams
from pymongo.cursor import _CURSOR_CLOSED_ERRORS, _ConnectionManager
from pymongo.errors import ConnectionFailure, InvalidOperation, OperationFailure
from pymongo.message import _CursorAddress, _GetMore, _OpMsg, _OpReply, _RawBatchGetMore
from pymongo.response import PinnedResponse
from pymongo.typings import _Address, _DocumentOut, _DocumentType

if TYPE_CHECKING:
    from pymongo.client_session import ClientSession
    from pymongo.collection import Collection
    from pymongo.pool import Connection


class CommandCursor(Generic[_DocumentType]):
    """A cursor / iterator over command cursors."""

    _getmore_class = _GetMore

    def __init__(
        self,
        collection: Collection[_DocumentType],
        cursor_info: Mapping[str, Any],
        address: Optional[_Address],
        batch_size: int = 0,
        max_await_time_ms: Optional[int] = None,
        session: Optional[ClientSession] = None,
        explicit_session: bool = False,
        comment: Any = None,
    ) -> None:
        """Create a new command cursor."""
        self.__sock_mgr: Any = None
        self.__collection: Collection[_DocumentType] = collection
        self.__id = cursor_info["id"]
        self.__data = deque(cursor_info["firstBatch"])
        self.__postbatchresumetoken: Optional[Mapping[str, Any]] = cursor_info.get(
            "postBatchResumeToken"
        )
        self.__address = address
        self.__batch_size = batch_size
        self.__max_await_time_ms = max_await_time_ms
        self.__session = session
        self.__explicit_session = explicit_session
        self.__killed = self.__id == 0
        self.__comment = comment
        if self.__killed:
            self.__end_session()

        if "ns" in cursor_info:  # noqa: SIM401
            self.__ns = cursor_info["ns"]
        else:
            self.__ns = collection.full_name

        self.batch_size(batch_size)

        if not isinstance(max_await_time_ms, int) and max_await_time_ms is not None:
            raise TypeError("max_await_time_ms must be an integer or None")

    def __del__(self) -> None:
        self.__die()

    def __die(self, synchronous: bool = False) -> None:
        """Closes this cursor."""
        already_killed = self.__killed
        self.__killed = True
        if self.__id and not already_killed:
            cursor_id = self.__id
            assert self.__address is not None
            address = _CursorAddress(self.__address, self.__ns)
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

    def __end_session(self) -> None:
        if self.__session and not self.__explicit_session:
            self.__session.end_session()
            self.__session = None

    def close(self) -> None:
        """Explicitly close / kill this cursor."""
        self.__die(True)

    def batch_size(self, batch_size: int) -> CommandCursor[_DocumentType]:
        """Limits the number of documents returned in one batch. Each batch
        requires a round trip to the server. It can be adjusted to optimize
        performance and limit data transfer.

        .. note:: batch_size can not override MongoDB's internal limits on the
           amount of data it will return to the client in a single batch (i.e
           if you set batch size to 1,000,000,000, MongoDB will currently only
           return 4-16MB of results per batch).

        Raises :exc:`TypeError` if `batch_size` is not an integer.
        Raises :exc:`ValueError` if `batch_size` is less than ``0``.

        :param batch_size: The size of each batch of results requested.
        """
        if not isinstance(batch_size, int):
            raise TypeError("batch_size must be an integer")
        if batch_size < 0:
            raise ValueError("batch_size must be >= 0")

        self.__batch_size = batch_size == 1 and 2 or batch_size
        return self

    def _has_next(self) -> bool:
        """Returns `True` if the cursor has documents remaining from the
        previous batch.
        """
        return len(self.__data) > 0

    @property
    def _post_batch_resume_token(self) -> Optional[Mapping[str, Any]]:
        """Retrieve the postBatchResumeToken from the response to a
        changeStream aggregate or getMore.
        """
        return self.__postbatchresumetoken

    def _maybe_pin_connection(self, conn: Connection) -> None:
        client = self.__collection.database.client
        if not client._should_pin_cursor(self.__session):
            return
        if not self.__sock_mgr:
            conn.pin_cursor()
            conn_mgr = _ConnectionManager(conn, False)
            # Ensure the connection gets returned when the entire result is
            # returned in the first batch.
            if self.__id == 0:
                conn_mgr.close()
            else:
                self.__sock_mgr = conn_mgr

    def __send_message(self, operation: _GetMore) -> None:
        """Send a getmore message and handle the response."""
        client = self.__collection.database.client
        try:
            response = client._run_operation(
                operation, self._unpack_response, address=self.__address
            )
        except OperationFailure as exc:
            if exc.code in _CURSOR_CLOSED_ERRORS:
                # Don't send killCursors because the cursor is already closed.
                self.__killed = True
            if exc.timeout:
                self.__die(False)
            else:
                # Return the session and pinned connection, if necessary.
                self.close()
            raise
        except ConnectionFailure:
            # Don't send killCursors because the cursor is already closed.
            self.__killed = True
            # Return the session and pinned connection, if necessary.
            self.close()
            raise
        except Exception:
            self.close()
            raise

        if isinstance(response, PinnedResponse):
            if not self.__sock_mgr:
                self.__sock_mgr = _ConnectionManager(response.conn, response.more_to_come)
        if response.from_command:
            cursor = response.docs[0]["cursor"]
            documents = cursor["nextBatch"]
            self.__postbatchresumetoken = cursor.get("postBatchResumeToken")
            self.__id = cursor["id"]
        else:
            documents = response.docs
            assert isinstance(response.data, _OpReply)
            self.__id = response.data.cursor_id

        if self.__id == 0:
            self.close()
        self.__data = deque(documents)

    def _unpack_response(
        self,
        response: Union[_OpReply, _OpMsg],
        cursor_id: Optional[int],
        codec_options: CodecOptions[Mapping[str, Any]],
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> Sequence[_DocumentOut]:
        return response.unpack_response(cursor_id, codec_options, user_fields, legacy_response)

    def _refresh(self) -> int:
        """Refreshes the cursor with more data from the server.

        Returns the length of self.__data after refresh. Will exit early if
        self.__data is already non-empty. Raises OperationFailure when the
        cursor cannot be refreshed due to an error on the query.
        """
        if len(self.__data) or self.__killed:
            return len(self.__data)

        if self.__id:  # Get More
            dbname, collname = self.__ns.split(".", 1)
            read_pref = self.__collection._read_preference_for(self.session)
            self.__send_message(
                self._getmore_class(
                    dbname,
                    collname,
                    self.__batch_size,
                    self.__id,
                    self.__collection.codec_options,
                    read_pref,
                    self.__session,
                    self.__collection.database.client,
                    self.__max_await_time_ms,
                    self.__sock_mgr,
                    False,
                    self.__comment,
                )
            )
        else:  # Cursor id is zero nothing else to return
            self.__die(True)

        return len(self.__data)

    @property
    def alive(self) -> bool:
        """Does this cursor have the potential to return more data?

        Even if :attr:`alive` is ``True``, :meth:`next` can raise
        :exc:`StopIteration`. Best to use a for loop::

            for doc in collection.aggregate(pipeline):
                print(doc)

        .. note:: :attr:`alive` can be True while iterating a cursor from
          a failed server. In this case :attr:`alive` will return False after
          :meth:`next` fails to retrieve the next batch of results from the
          server.
        """
        return bool(len(self.__data) or (not self.__killed))

    @property
    def cursor_id(self) -> int:
        """Returns the id of the cursor."""
        return self.__id

    @property
    def address(self) -> Optional[_Address]:
        """The (host, port) of the server used, or None.

        .. versionadded:: 3.0
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

    def __iter__(self) -> Iterator[_DocumentType]:
        return self

    def next(self) -> _DocumentType:
        """Advance the cursor."""
        # Block until a document is returnable.
        while self.alive:
            doc = self._try_next(True)
            if doc is not None:
                return doc

        raise StopIteration

    __next__ = next

    def _try_next(self, get_more_allowed: bool) -> Optional[_DocumentType]:
        """Advance the cursor blocking for at most one getMore command."""
        if not len(self.__data) and not self.__killed and get_more_allowed:
            self._refresh()
        if len(self.__data):
            return self.__data.popleft()
        else:
            return None

    def try_next(self) -> Optional[_DocumentType]:
        """Advance the cursor without blocking indefinitely.

        This method returns the next document without waiting
        indefinitely for data.

        If no document is cached locally then this method runs a single
        getMore command. If the getMore yields any documents, the next
        document is returned, otherwise, if the getMore returns no documents
        (because there is no additional data) then ``None`` is returned.

        :return: The next document or ``None`` when no document is available
          after running a single getMore or when the cursor is closed.

        .. versionadded:: 4.5
        """
        return self._try_next(get_more_allowed=True)

    def __enter__(self) -> CommandCursor[_DocumentType]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class RawBatchCommandCursor(CommandCursor, Generic[_DocumentType]):
    _getmore_class = _RawBatchGetMore

    def __init__(
        self,
        collection: Collection[_DocumentType],
        cursor_info: Mapping[str, Any],
        address: Optional[_Address],
        batch_size: int = 0,
        max_await_time_ms: Optional[int] = None,
        session: Optional[ClientSession] = None,
        explicit_session: bool = False,
        comment: Any = None,
    ) -> None:
        """Create a new cursor / iterator over raw batches of BSON data.

        Should not be called directly by application developers -
        see :meth:`~pymongo.collection.Collection.aggregate_raw_batches`
        instead.

        .. seealso:: The MongoDB documentation on `cursors <https://dochub.mongodb.org/core/cursors>`_.
        """
        assert not cursor_info.get("firstBatch")
        super().__init__(
            collection,
            cursor_info,
            address,
            batch_size,
            max_await_time_ms,
            session,
            explicit_session,
            comment,
        )

    def _unpack_response(  # type: ignore[override]
        self,
        response: Union[_OpReply, _OpMsg],
        cursor_id: Optional[int],
        codec_options: CodecOptions,
        user_fields: Optional[Mapping[str, Any]] = None,
        legacy_response: bool = False,
    ) -> list[Mapping[str, Any]]:
        raw_response = response.raw_response(cursor_id, user_fields=user_fields)
        if not legacy_response:
            # OP_MSG returns firstBatch/nextBatch documents as a BSON array
            # Re-assemble the array of documents into a document stream
            _convert_raw_document_lists_to_streams(raw_response[0])
        return raw_response  # type: ignore[return-value]

    def __getitem__(self, index: int) -> NoReturn:
        raise InvalidOperation("Cannot call __getitem__ on RawBatchCursor")
