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

"""Represent a response from the server."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Union

if TYPE_CHECKING:
    from datetime import timedelta

    from pymongo.message import _OpMsg, _OpReply
    from pymongo.pool import Connection
    from pymongo.typings import _Address, _DocumentOut


class Response:
    __slots__ = ("_data", "_address", "_request_id", "_duration", "_from_command", "_docs")

    def __init__(
        self,
        data: Union[_OpMsg, _OpReply],
        address: _Address,
        request_id: int,
        duration: Optional[timedelta],
        from_command: bool,
        docs: Sequence[Mapping[str, Any]],
    ):
        """Represent a response from the server.

        :param data: A network response message.
        :param address: (host, port) of the source server.
        :param request_id: The request id of this operation.
        :param duration: The duration of the operation.
        :param from_command: if the response is the result of a db command.
        """
        self._data = data
        self._address = address
        self._request_id = request_id
        self._duration = duration
        self._from_command = from_command
        self._docs = docs

    @property
    def data(self) -> Union[_OpMsg, _OpReply]:
        """Server response's raw BSON bytes."""
        return self._data

    @property
    def address(self) -> _Address:
        """(host, port) of the source server."""
        return self._address

    @property
    def request_id(self) -> int:
        """The request id of this operation."""
        return self._request_id

    @property
    def duration(self) -> Optional[timedelta]:
        """The duration of the operation."""
        return self._duration

    @property
    def from_command(self) -> bool:
        """If the response is a result from a db command."""
        return self._from_command

    @property
    def docs(self) -> Sequence[Mapping[str, Any]]:
        """The decoded document(s)."""
        return self._docs


class PinnedResponse(Response):
    __slots__ = ("_conn", "_more_to_come")

    def __init__(
        self,
        data: Union[_OpMsg, _OpReply],
        address: _Address,
        conn: Connection,
        request_id: int,
        duration: Optional[timedelta],
        from_command: bool,
        docs: list[_DocumentOut],
        more_to_come: bool,
    ):
        """Represent a response to an exhaust cursor's initial query.

        :param data:  A network response message.
        :param address: (host, port) of the source server.
        :param conn: The Connection used for the initial query.
        :param request_id: The request id of this operation.
        :param duration: The duration of the operation.
        :param from_command: If the response is the result of a db command.
        :param docs: List of documents.
        :param more_to_come: Bool indicating whether cursor is ready to be
            exhausted.
        """
        super().__init__(data, address, request_id, duration, from_command, docs)
        self._conn = conn
        self._more_to_come = more_to_come

    @property
    def conn(self) -> Connection:
        """The Connection used for the initial query.

        The server will send batches on this socket, without waiting for
        getMores from the client, until the result set is exhausted or there
        is an error.
        """
        return self._conn

    @property
    def more_to_come(self) -> bool:
        """If true, server is ready to send batches on the socket until the
        result set is exhausted or there is an error.
        """
        return self._more_to_come
