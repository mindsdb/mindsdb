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

"""Bits and pieces used by the driver that don't really fit elsewhere."""
from __future__ import annotations

import sys
import traceback
from collections import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Container,
    Iterable,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

from pymongo import ASCENDING
from pymongo.errors import (
    CursorNotFound,
    DuplicateKeyError,
    ExecutionTimeout,
    NotPrimaryError,
    OperationFailure,
    WriteConcernError,
    WriteError,
    WTimeoutError,
    _wtimeout_error,
)
from pymongo.hello import HelloCompat

if TYPE_CHECKING:
    from pymongo.cursor import _Hint
    from pymongo.operations import _IndexList
    from pymongo.typings import _DocumentOut

# From the SDAM spec, the "node is shutting down" codes.
_SHUTDOWN_CODES: frozenset = frozenset(
    [
        11600,  # InterruptedAtShutdown
        91,  # ShutdownInProgress
    ]
)
# From the SDAM spec, the "not primary" error codes are combined with the
# "node is recovering" error codes (of which the "node is shutting down"
# errors are a subset).
_NOT_PRIMARY_CODES: frozenset = (
    frozenset(
        [
            10058,  # LegacyNotPrimary <=3.2 "not primary" error code
            10107,  # NotWritablePrimary
            13435,  # NotPrimaryNoSecondaryOk
            11602,  # InterruptedDueToReplStateChange
            13436,  # NotPrimaryOrSecondary
            189,  # PrimarySteppedDown
        ]
    )
    | _SHUTDOWN_CODES
)
# From the retryable writes spec.
_RETRYABLE_ERROR_CODES: frozenset = _NOT_PRIMARY_CODES | frozenset(
    [
        7,  # HostNotFound
        6,  # HostUnreachable
        89,  # NetworkTimeout
        9001,  # SocketException
        262,  # ExceededTimeLimit
        134,  # ReadConcernMajorityNotAvailableYet
    ]
)

# Server code raised when re-authentication is required
_REAUTHENTICATION_REQUIRED_CODE: int = 391

# Server code raised when authentication fails.
_AUTHENTICATION_FAILURE_CODE: int = 18

# Note - to avoid bugs from forgetting which if these is all lowercase and
# which are camelCase, and at the same time avoid having to add a test for
# every command, use all lowercase here and test against command_name.lower().
_SENSITIVE_COMMANDS: set = {
    "authenticate",
    "saslstart",
    "saslcontinue",
    "getnonce",
    "createuser",
    "updateuser",
    "copydbgetnonce",
    "copydbsaslstart",
    "copydb",
}


def _gen_index_name(keys: _IndexList) -> str:
    """Generate an index name from the set of fields it is over."""
    return "_".join(["{}_{}".format(*item) for item in keys])


def _index_list(
    key_or_list: _Hint, direction: Optional[Union[int, str]] = None
) -> Sequence[tuple[str, Union[int, str, Mapping[str, Any]]]]:
    """Helper to generate a list of (key, direction) pairs.

    Takes such a list, or a single key, or a single key and direction.
    """
    if direction is not None:
        if not isinstance(key_or_list, str):
            raise TypeError("Expected a string and a direction")
        return [(key_or_list, direction)]
    else:
        if isinstance(key_or_list, str):
            return [(key_or_list, ASCENDING)]
        elif isinstance(key_or_list, abc.ItemsView):
            return list(key_or_list)  # type: ignore[arg-type]
        elif isinstance(key_or_list, abc.Mapping):
            return list(key_or_list.items())
        elif not isinstance(key_or_list, (list, tuple)):
            raise TypeError("if no direction is specified, key_or_list must be an instance of list")
        values: list[tuple[str, int]] = []
        for item in key_or_list:
            if isinstance(item, str):
                item = (item, ASCENDING)  # noqa: PLW2901
            values.append(item)
        return values


def _index_document(index_list: _IndexList) -> dict[str, Any]:
    """Helper to generate an index specifying document.

    Takes a list of (key, direction) pairs.
    """
    if not isinstance(index_list, (list, tuple, abc.Mapping)):
        raise TypeError(
            "must use a dictionary or a list of (key, direction) pairs, not: " + repr(index_list)
        )
    if not len(index_list):
        raise ValueError("key_or_list must not be empty")

    index: dict[str, Any] = {}

    if isinstance(index_list, abc.Mapping):
        for key in index_list:
            value = index_list[key]
            _validate_index_key_pair(key, value)
            index[key] = value
    else:
        for item in index_list:
            if isinstance(item, str):
                item = (item, ASCENDING)  # noqa: PLW2901
            key, value = item
            _validate_index_key_pair(key, value)
            index[key] = value
    return index


def _validate_index_key_pair(key: Any, value: Any) -> None:
    if not isinstance(key, str):
        raise TypeError("first item in each key pair must be an instance of str")
    if not isinstance(value, (str, int, abc.Mapping)):
        raise TypeError(
            "second item in each key pair must be 1, -1, "
            "'2d', or another valid MongoDB index specifier."
        )


def _check_command_response(
    response: _DocumentOut,
    max_wire_version: Optional[int],
    allowable_errors: Optional[Container[Union[int, str]]] = None,
    parse_write_concern_error: bool = False,
) -> None:
    """Check the response to a command for errors."""
    if "ok" not in response:
        # Server didn't recognize our message as a command.
        raise OperationFailure(
            response.get("$err"),  # type: ignore[arg-type]
            response.get("code"),
            response,
            max_wire_version,
        )

    if parse_write_concern_error and "writeConcernError" in response:
        _error = response["writeConcernError"]
        _labels = response.get("errorLabels")
        if _labels:
            _error.update({"errorLabels": _labels})
        _raise_write_concern_error(_error)

    if response["ok"]:
        return

    details = response
    # Mongos returns the error details in a 'raw' object
    # for some errors.
    if "raw" in response:
        for shard in response["raw"].values():
            # Grab the first non-empty raw error from a shard.
            if shard.get("errmsg") and not shard.get("ok"):
                details = shard
                break

    errmsg = details["errmsg"]
    code = details.get("code")

    # For allowable errors, only check for error messages when the code is not
    # included.
    if allowable_errors:
        if code is not None:
            if code in allowable_errors:
                return
        elif errmsg in allowable_errors:
            return

    # Server is "not primary" or "recovering"
    if code is not None:
        if code in _NOT_PRIMARY_CODES:
            raise NotPrimaryError(errmsg, response)
    elif HelloCompat.LEGACY_ERROR in errmsg or "node is recovering" in errmsg:
        raise NotPrimaryError(errmsg, response)

    # Other errors
    # findAndModify with upsert can raise duplicate key error
    if code in (11000, 11001, 12582):
        raise DuplicateKeyError(errmsg, code, response, max_wire_version)
    elif code == 50:
        raise ExecutionTimeout(errmsg, code, response, max_wire_version)
    elif code == 43:
        raise CursorNotFound(errmsg, code, response, max_wire_version)

    raise OperationFailure(errmsg, code, response, max_wire_version)


def _raise_last_write_error(write_errors: list[Any]) -> NoReturn:
    # If the last batch had multiple errors only report
    # the last error to emulate continue_on_error.
    error = write_errors[-1]
    if error.get("code") == 11000:
        raise DuplicateKeyError(error.get("errmsg"), 11000, error)
    raise WriteError(error.get("errmsg"), error.get("code"), error)


def _raise_write_concern_error(error: Any) -> NoReturn:
    if _wtimeout_error(error):
        # Make sure we raise WTimeoutError
        raise WTimeoutError(error.get("errmsg"), error.get("code"), error)
    raise WriteConcernError(error.get("errmsg"), error.get("code"), error)


def _get_wce_doc(result: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """Return the writeConcernError or None."""
    wce = result.get("writeConcernError")
    if wce:
        # The server reports errorLabels at the top level but it's more
        # convenient to attach it to the writeConcernError doc itself.
        error_labels = result.get("errorLabels")
        if error_labels:
            # Copy to avoid changing the original document.
            wce = wce.copy()
            wce["errorLabels"] = error_labels
    return wce


def _check_write_command_response(result: Mapping[str, Any]) -> None:
    """Backward compatibility helper for write command error handling."""
    # Prefer write errors over write concern errors
    write_errors = result.get("writeErrors")
    if write_errors:
        _raise_last_write_error(write_errors)

    wce = _get_wce_doc(result)
    if wce:
        _raise_write_concern_error(wce)


def _fields_list_to_dict(
    fields: Union[Mapping[str, Any], Iterable[str]], option_name: str
) -> Mapping[str, Any]:
    """Takes a sequence of field names and returns a matching dictionary.

    ["a", "b"] becomes {"a": 1, "b": 1}

    and

    ["a.b.c", "d", "a.c"] becomes {"a.b.c": 1, "d": 1, "a.c": 1}
    """
    if isinstance(fields, abc.Mapping):
        return fields

    if isinstance(fields, (abc.Sequence, abc.Set)):
        if not all(isinstance(field, str) for field in fields):
            raise TypeError(f"{option_name} must be a list of key names, each an instance of str")
        return dict.fromkeys(fields, 1)

    raise TypeError(f"{option_name} must be a mapping or list of key names")


def _handle_exception() -> None:
    """Print exceptions raised by subscribers to stderr."""
    # Heavily influenced by logging.Handler.handleError.

    # See note here:
    # https://docs.python.org/3.4/library/sys.html#sys.__stderr__
    if sys.stderr:
        einfo = sys.exc_info()
        try:
            traceback.print_exception(einfo[0], einfo[1], einfo[2], None, sys.stderr)
        except OSError:
            pass
        finally:
            del einfo


# See https://mypy.readthedocs.io/en/stable/generics.html?#decorator-factories
F = TypeVar("F", bound=Callable[..., Any])


def _handle_reauth(func: F) -> F:
    def inner(*args: Any, **kwargs: Any) -> Any:
        no_reauth = kwargs.pop("no_reauth", False)
        from pymongo.message import _BulkWriteContext
        from pymongo.pool import Connection

        try:
            return func(*args, **kwargs)
        except OperationFailure as exc:
            if no_reauth:
                raise
            if exc.code == _REAUTHENTICATION_REQUIRED_CODE:
                # Look for an argument that either is a Connection
                # or has a connection attribute, so we can trigger
                # a reauth.
                conn = None
                for arg in args:
                    if isinstance(arg, Connection):
                        conn = arg
                        break
                    if isinstance(arg, _BulkWriteContext):
                        conn = arg.conn
                        break
                if conn:
                    conn.authenticate(reauthenticate=True)
                else:
                    raise
                return func(*args, **kwargs)
            raise

    return cast(F, inner)
