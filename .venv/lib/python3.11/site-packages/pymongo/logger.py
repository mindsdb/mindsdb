# Copyright 2023-present MongoDB, Inc.
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
from __future__ import annotations

import enum
import logging
import os
import warnings
from typing import Any

from bson import UuidRepresentation, json_util
from bson.json_util import JSONOptions, _truncate_documents
from pymongo.monitoring import ConnectionCheckOutFailedReason, ConnectionClosedReason


class _CommandStatusMessage(str, enum.Enum):
    STARTED = "Command started"
    SUCCEEDED = "Command succeeded"
    FAILED = "Command failed"


class _ServerSelectionStatusMessage(str, enum.Enum):
    STARTED = "Server selection started"
    SUCCEEDED = "Server selection succeeded"
    FAILED = "Server selection failed"
    WAITING = "Waiting for suitable server to become available"


class _ConnectionStatusMessage(str, enum.Enum):
    POOL_CREATED = "Connection pool created"
    POOL_READY = "Connection pool ready"
    POOL_CLOSED = "Connection pool closed"
    POOL_CLEARED = "Connection pool cleared"

    CONN_CREATED = "Connection created"
    CONN_READY = "Connection ready"
    CONN_CLOSED = "Connection closed"

    CHECKOUT_STARTED = "Connection checkout started"
    CHECKOUT_SUCCEEDED = "Connection checked out"
    CHECKOUT_FAILED = "Connection checkout failed"
    CHECKEDIN = "Connection checked in"


_DEFAULT_DOCUMENT_LENGTH = 1000
_SENSITIVE_COMMANDS = [
    "authenticate",
    "saslStart",
    "saslContinue",
    "getnonce",
    "createUser",
    "updateUser",
    "copydbgetnonce",
    "copydbsaslstart",
    "copydb",
]
_HELLO_COMMANDS = ["hello", "ismaster", "isMaster"]
_REDACTED_FAILURE_FIELDS = ["code", "codeName", "errorLabels"]
_DOCUMENT_NAMES = ["command", "reply", "failure"]
_JSON_OPTIONS = JSONOptions(uuid_representation=UuidRepresentation.STANDARD)
_COMMAND_LOGGER = logging.getLogger("pymongo.command")
_CONNECTION_LOGGER = logging.getLogger("pymongo.connection")
_SERVER_SELECTION_LOGGER = logging.getLogger("pymongo.serverSelection")
_CLIENT_LOGGER = logging.getLogger("pymongo.client")
_VERBOSE_CONNECTION_ERROR_REASONS = {
    ConnectionClosedReason.POOL_CLOSED: "Connection pool was closed",
    ConnectionCheckOutFailedReason.POOL_CLOSED: "Connection pool was closed",
    ConnectionClosedReason.STALE: "Connection pool was stale",
    ConnectionClosedReason.ERROR: "An error occurred while using the connection",
    ConnectionCheckOutFailedReason.CONN_ERROR: "An error occurred while trying to establish a new connection",
    ConnectionClosedReason.IDLE: "Connection was idle too long",
    ConnectionCheckOutFailedReason.TIMEOUT: "Connection exceeded the specified timeout",
}


def _debug_log(logger: logging.Logger, **fields: Any) -> None:
    logger.debug(LogMessage(**fields))


def _verbose_connection_error_reason(reason: str) -> str:
    return _VERBOSE_CONNECTION_ERROR_REASONS.get(reason, reason)


def _info_log(logger: logging.Logger, **fields: Any) -> None:
    logger.info(LogMessage(**fields))


def _log_or_warn(logger: logging.Logger, message: str) -> None:
    if logger.isEnabledFor(logging.INFO):
        logger.info(message)
    else:
        # stacklevel=4 ensures that the warning is for the user's code.
        warnings.warn(message, UserWarning, stacklevel=4)


class LogMessage:
    __slots__ = ("_kwargs", "_redacted")

    def __init__(self, **kwargs: Any):
        self._kwargs = kwargs
        self._redacted = False

    def __str__(self) -> str:
        self._redact()
        return "%s" % (
            json_util.dumps(
                self._kwargs, json_options=_JSON_OPTIONS, default=lambda o: o.__repr__()
            )
        )

    def _is_sensitive(self, doc_name: str) -> bool:
        is_speculative_authenticate = (
            self._kwargs.pop("speculative_authenticate", False)
            or "speculativeAuthenticate" in self._kwargs[doc_name]
        )
        is_sensitive_command = (
            "commandName" in self._kwargs and self._kwargs["commandName"] in _SENSITIVE_COMMANDS
        )

        is_sensitive_hello = (
            self._kwargs["commandName"] in _HELLO_COMMANDS and is_speculative_authenticate
        )

        return is_sensitive_command or is_sensitive_hello

    def _redact(self) -> None:
        if self._redacted:
            return
        self._kwargs = {k: v for k, v in self._kwargs.items() if v is not None}
        if "durationMS" in self._kwargs and hasattr(self._kwargs["durationMS"], "total_seconds"):
            self._kwargs["durationMS"] = self._kwargs["durationMS"].total_seconds() * 1000
        if "serviceId" in self._kwargs:
            self._kwargs["serviceId"] = str(self._kwargs["serviceId"])
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", _DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = _DEFAULT_DOCUMENT_LENGTH
        is_server_side_error = self._kwargs.pop("isServerSideError", False)

        for doc_name in _DOCUMENT_NAMES:
            doc = self._kwargs.get(doc_name)
            if doc:
                if doc_name == "failure" and is_server_side_error:
                    doc = {k: v for k, v in doc.items() if k in _REDACTED_FAILURE_FIELDS}
                if doc_name != "failure" and self._is_sensitive(doc_name):
                    doc = json_util.dumps({})
                else:
                    truncated_doc = _truncate_documents(doc, document_length)[0]
                    doc = json_util.dumps(
                        truncated_doc,
                        json_options=_JSON_OPTIONS,
                        default=lambda o: o.__repr__(),
                    )
                if len(doc) > document_length:
                    doc = (
                        doc.encode()[:document_length].decode("unicode-escape", "ignore")
                    ) + "..."
                self._kwargs[doc_name] = doc
        self._redacted = True
