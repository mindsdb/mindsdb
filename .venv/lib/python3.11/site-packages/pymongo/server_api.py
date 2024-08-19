# Copyright 2020-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Support for MongoDB Stable API.

.. _versioned-api-ref:

MongoDB Stable API
=====================

Starting in MongoDB 5.0, applications can specify the server API version
to use when creating a :class:`~pymongo.mongo_client.MongoClient`. Doing so
ensures that the driver behaves in a manner compatible with that server API
version, regardless of the server's actual release version.

Declaring an API Version
````````````````````````

.. attention:: Stable API requires MongoDB >=5.0.

To configure MongoDB Stable API, pass the ``server_api`` keyword option to
:class:`~pymongo.mongo_client.MongoClient`::

    >>> from pymongo.mongo_client import MongoClient
    >>> from pymongo.server_api import ServerApi
    >>>
    >>> # Declare API version "1" for MongoClient "client"
    >>> server_api = ServerApi('1')
    >>> client = MongoClient(server_api=server_api)

The declared API version is applied to all commands run through ``client``,
including those sent through the generic
:meth:`~pymongo.database.Database.command` helper.

.. note:: Declaring an API version on the
   :class:`~pymongo.mongo_client.MongoClient` **and** specifying stable
   API options in :meth:`~pymongo.database.Database.command` command document
   is not supported and will lead to undefined behaviour.

To run any command without declaring a server API version or using a different
API version, create a separate :class:`~pymongo.mongo_client.MongoClient`
instance.

Strict Mode
```````````

Configuring ``strict`` mode will cause the MongoDB server to reject all
commands that are not part of the declared :attr:`ServerApi.version`. This
includes command options and aggregation pipeline stages.

For example::

    >>> server_api = ServerApi('1', strict=True)
    >>> client = MongoClient(server_api=server_api)
    >>> client.test.command('count', 'test')
    Traceback (most recent call last):
    ...
    pymongo.errors.OperationFailure: Provided apiStrict:true, but the command count is not in API Version 1, full error: {'ok': 0.0, 'errmsg': 'Provided apiStrict:true, but the command count is not in API Version 1', 'code': 323, 'codeName': 'APIStrictError'

Detecting API Deprecations
``````````````````````````

The ``deprecationErrors`` option can be used to enable command failures
when using functionality that is deprecated from the configured
:attr:`ServerApi.version`. For example::

    >>> server_api = ServerApi('1', deprecation_errors=True)
    >>> client = MongoClient(server_api=server_api)

Note that at the time of this writing, no deprecated APIs exist.

Classes
=======
"""
from __future__ import annotations

from typing import Any, MutableMapping, Optional


class ServerApiVersion:
    """An enum that defines values for :attr:`ServerApi.version`.

    .. versionadded:: 3.12
    """

    V1 = "1"
    """Server API version "1"."""


class ServerApi:
    """MongoDB Stable API."""

    def __init__(
        self, version: str, strict: Optional[bool] = None, deprecation_errors: Optional[bool] = None
    ):
        """Options to configure MongoDB Stable API.

        :param version: The API version string. Must be one of the values in
            :class:`ServerApiVersion`.
        :param strict: Set to ``True`` to enable API strict mode.
            Defaults to ``None`` which means "use the server's default".
        :param deprecation_errors: Set to ``True`` to enable
            deprecation errors. Defaults to ``None`` which means "use the
            server's default".

        .. versionadded:: 3.12
        """
        if version != ServerApiVersion.V1:
            raise ValueError(f"Unknown ServerApi version: {version}")
        if strict is not None and not isinstance(strict, bool):
            raise TypeError(
                "Wrong type for ServerApi strict, value must be an instance "
                f"of bool, not {type(strict)}"
            )
        if deprecation_errors is not None and not isinstance(deprecation_errors, bool):
            raise TypeError(
                "Wrong type for ServerApi deprecation_errors, value must be "
                f"an instance of bool, not {type(deprecation_errors)}"
            )
        self._version = version
        self._strict = strict
        self._deprecation_errors = deprecation_errors

    @property
    def version(self) -> str:
        """The API version setting.

        This value is sent to the server in the "apiVersion" field.
        """
        return self._version

    @property
    def strict(self) -> Optional[bool]:
        """The API strict mode setting.

        When set, this value is sent to the server in the "apiStrict" field.
        """
        return self._strict

    @property
    def deprecation_errors(self) -> Optional[bool]:
        """The API deprecation errors setting.

        When set, this value is sent to the server in the
        "apiDeprecationErrors" field.
        """
        return self._deprecation_errors


def _add_to_command(cmd: MutableMapping[str, Any], server_api: Optional[ServerApi]) -> None:
    """Internal helper which adds API versioning options to a command.

    :param cmd: The command.
    :param server_api: A :class:`ServerApi` or ``None``.
    """
    if not server_api:
        return
    cmd["apiVersion"] = server_api.version
    if server_api.strict is not None:
        cmd["apiStrict"] = server_api.strict
    if server_api.deprecation_errors is not None:
        cmd["apiDeprecationErrors"] = server_api.deprecation_errors
