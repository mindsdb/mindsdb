# Copyright 2020-present MongoDB, Inc.
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

"""MONGODB-AWS Authentication helpers."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Type

import bson
from bson.binary import Binary
from pymongo.errors import ConfigurationError, OperationFailure

if TYPE_CHECKING:
    from bson.typings import _ReadableBuffer
    from pymongo.auth import MongoCredential
    from pymongo.pool import Connection


def _authenticate_aws(credentials: MongoCredential, conn: Connection) -> None:
    """Authenticate using MONGODB-AWS."""
    try:
        import pymongo_auth_aws  # type:ignore[import]
    except ImportError as e:
        raise ConfigurationError(
            "MONGODB-AWS authentication requires pymongo-auth-aws: "
            "install with: python -m pip install 'pymongo[aws]'"
        ) from e

    # Delayed import.
    from pymongo_auth_aws.auth import (  # type:ignore[import]
        set_cached_credentials,
        set_use_cached_credentials,
    )

    set_use_cached_credentials(True)

    if conn.max_wire_version < 9:
        raise ConfigurationError("MONGODB-AWS authentication requires MongoDB version 4.4 or later")

    class AwsSaslContext(pymongo_auth_aws.AwsSaslContext):  # type: ignore
        # Dependency injection:
        def binary_type(self) -> Type[Binary]:
            """Return the bson.binary.Binary type."""
            return Binary

        def bson_encode(self, doc: Mapping[str, Any]) -> bytes:
            """Encode a dictionary to BSON."""
            return bson.encode(doc)

        def bson_decode(self, data: _ReadableBuffer) -> Mapping[str, Any]:
            """Decode BSON to a dictionary."""
            return bson.decode(data)

    try:
        ctx = AwsSaslContext(
            pymongo_auth_aws.AwsCredential(
                credentials.username,
                credentials.password,
                credentials.mechanism_properties.aws_session_token,
            )
        )
        client_payload = ctx.step(None)
        client_first = {"saslStart": 1, "mechanism": "MONGODB-AWS", "payload": client_payload}
        server_first = conn.command("$external", client_first)
        res = server_first
        # Limit how many times we loop to catch protocol / library issues
        for _ in range(10):
            client_payload = ctx.step(res["payload"])
            cmd = {
                "saslContinue": 1,
                "conversationId": server_first["conversationId"],
                "payload": client_payload,
            }
            res = conn.command("$external", cmd)
            if res["done"]:
                # SASL complete.
                break
    except pymongo_auth_aws.PyMongoAuthAwsError as exc:
        # Clear the cached credentials if we hit a failure in auth.
        set_cached_credentials(None)
        # Convert to OperationFailure and include pymongo-auth-aws version.
        raise OperationFailure(
            f"{exc} (pymongo-auth-aws version {pymongo_auth_aws.__version__})"
        ) from None
    except Exception:
        # Clear the cached credentials if we hit a failure in auth.
        set_cached_credentials(None)
        raise
