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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Tuple, Type, Union
from uuid import UUID

"""Tools for representing BSON binary data.
"""

BINARY_SUBTYPE = 0
"""BSON binary subtype for binary data.

This is the default subtype for binary data.
"""

FUNCTION_SUBTYPE = 1
"""BSON binary subtype for functions.
"""

OLD_BINARY_SUBTYPE = 2
"""Old BSON binary subtype for binary data.

This is the old default subtype, the current
default is :data:`BINARY_SUBTYPE`.
"""

OLD_UUID_SUBTYPE = 3
"""Old BSON binary subtype for a UUID.

:class:`uuid.UUID` instances will automatically be encoded
by :mod:`bson` using this subtype when using
:data:`UuidRepresentation.PYTHON_LEGACY`,
:data:`UuidRepresentation.JAVA_LEGACY`, or
:data:`UuidRepresentation.CSHARP_LEGACY`.

.. versionadded:: 2.1
"""

UUID_SUBTYPE = 4
"""BSON binary subtype for a UUID.

This is the standard BSON binary subtype for UUIDs.
:class:`uuid.UUID` instances will automatically be encoded
by :mod:`bson` using this subtype when using
:data:`UuidRepresentation.STANDARD`.
"""


if TYPE_CHECKING:
    from array import array as _array
    from mmap import mmap as _mmap


class UuidRepresentation:
    UNSPECIFIED = 0
    """An unspecified UUID representation.

    When configured, :class:`uuid.UUID` instances will **not** be
    automatically encoded to or decoded from :class:`~bson.binary.Binary`.
    When encoding a :class:`uuid.UUID` instance, an error will be raised.
    To encode a :class:`uuid.UUID` instance with this configuration, it must
    be wrapped in the :class:`~bson.binary.Binary` class by the application
    code. When decoding a BSON binary field with a UUID subtype, a
    :class:`~bson.binary.Binary` instance will be returned instead of a
    :class:`uuid.UUID` instance.

    See :ref:`unspecified-representation-details` for details.

    .. versionadded:: 3.11
    """

    STANDARD = UUID_SUBTYPE
    """The standard UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary, using RFC-4122 byte order with
    binary subtype :data:`UUID_SUBTYPE`.

    See :ref:`standard-representation-details` for details.

    .. versionadded:: 3.11
    """

    PYTHON_LEGACY = OLD_UUID_SUBTYPE
    """The Python legacy UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary, using RFC-4122 byte order with
    binary subtype :data:`OLD_UUID_SUBTYPE`.

    See :ref:`python-legacy-representation-details` for details.

    .. versionadded:: 3.11
    """

    JAVA_LEGACY = 5
    """The Java legacy UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary subtype :data:`OLD_UUID_SUBTYPE`,
    using the Java driver's legacy byte order.

    See :ref:`java-legacy-representation-details` for details.

    .. versionadded:: 3.11
    """

    CSHARP_LEGACY = 6
    """The C#/.net legacy UUID representation.

    :class:`uuid.UUID` instances will automatically be encoded to
    and decoded from BSON binary subtype :data:`OLD_UUID_SUBTYPE`,
    using the C# driver's legacy byte order.

    See :ref:`csharp-legacy-representation-details` for details.

    .. versionadded:: 3.11
    """


STANDARD = UuidRepresentation.STANDARD
"""An alias for :data:`UuidRepresentation.STANDARD`.

.. versionadded:: 3.0
"""

PYTHON_LEGACY = UuidRepresentation.PYTHON_LEGACY
"""An alias for :data:`UuidRepresentation.PYTHON_LEGACY`.

.. versionadded:: 3.0
"""

JAVA_LEGACY = UuidRepresentation.JAVA_LEGACY
"""An alias for :data:`UuidRepresentation.JAVA_LEGACY`.

.. versionchanged:: 3.6
   BSON binary subtype 4 is decoded using RFC-4122 byte order.
.. versionadded:: 2.3
"""

CSHARP_LEGACY = UuidRepresentation.CSHARP_LEGACY
"""An alias for :data:`UuidRepresentation.CSHARP_LEGACY`.

.. versionchanged:: 3.6
   BSON binary subtype 4 is decoded using RFC-4122 byte order.
.. versionadded:: 2.3
"""

ALL_UUID_SUBTYPES = (OLD_UUID_SUBTYPE, UUID_SUBTYPE)
ALL_UUID_REPRESENTATIONS = (
    UuidRepresentation.UNSPECIFIED,
    UuidRepresentation.STANDARD,
    UuidRepresentation.PYTHON_LEGACY,
    UuidRepresentation.JAVA_LEGACY,
    UuidRepresentation.CSHARP_LEGACY,
)
UUID_REPRESENTATION_NAMES = {
    UuidRepresentation.UNSPECIFIED: "UuidRepresentation.UNSPECIFIED",
    UuidRepresentation.STANDARD: "UuidRepresentation.STANDARD",
    UuidRepresentation.PYTHON_LEGACY: "UuidRepresentation.PYTHON_LEGACY",
    UuidRepresentation.JAVA_LEGACY: "UuidRepresentation.JAVA_LEGACY",
    UuidRepresentation.CSHARP_LEGACY: "UuidRepresentation.CSHARP_LEGACY",
}

MD5_SUBTYPE = 5
"""BSON binary subtype for an MD5 hash.
"""

COLUMN_SUBTYPE = 7
"""BSON binary subtype for columns.

.. versionadded:: 4.0
"""

SENSITIVE_SUBTYPE = 8
"""BSON binary subtype for sensitive data.

.. versionadded:: 4.5
"""


USER_DEFINED_SUBTYPE = 128
"""BSON binary subtype for any user defined structure.
"""


class Binary(bytes):
    """Representation of BSON binary data.

    This is necessary because we want to represent Python strings as
    the BSON string type. We need to wrap binary data so we can tell
    the difference between what should be considered binary data and
    what should be considered a string when we encode to BSON.

    Raises TypeError if `data` is not an instance of :class:`bytes`
    or `subtype` is not an instance of :class:`int`.
    Raises ValueError if `subtype` is not in [0, 256).

    .. note::
      Instances of Binary with subtype 0 will be decoded directly to :class:`bytes`.

    :param data: the binary data to represent. Can be any bytes-like type
        that implements the buffer protocol.
    :param subtype: the `binary subtype
        <https://bsonspec.org/spec.html>`_
        to use

    .. versionchanged:: 3.9
      Support any bytes-like type that implements the buffer protocol.
    """

    _type_marker = 5
    __subtype: int

    def __new__(
        cls: Type[Binary],
        data: Union[memoryview, bytes, _mmap, _array[Any]],
        subtype: int = BINARY_SUBTYPE,
    ) -> Binary:
        if not isinstance(subtype, int):
            raise TypeError("subtype must be an instance of int")
        if subtype >= 256 or subtype < 0:
            raise ValueError("subtype must be contained in [0, 256)")
        # Support any type that implements the buffer protocol.
        self = bytes.__new__(cls, memoryview(data).tobytes())
        self.__subtype = subtype
        return self

    @classmethod
    def from_uuid(
        cls: Type[Binary], uuid: UUID, uuid_representation: int = UuidRepresentation.STANDARD
    ) -> Binary:
        """Create a BSON Binary object from a Python UUID.

        Creates a :class:`~bson.binary.Binary` object from a
        :class:`uuid.UUID` instance. Assumes that the native
        :class:`uuid.UUID` instance uses the byte-order implied by the
        provided ``uuid_representation``.

        Raises :exc:`TypeError` if `uuid` is not an instance of
        :class:`~uuid.UUID`.

        :param uuid: A :class:`uuid.UUID` instance.
        :param uuid_representation: A member of
            :class:`~bson.binary.UuidRepresentation`. Default:
            :const:`~bson.binary.UuidRepresentation.STANDARD`.
            See :ref:`handling-uuid-data-example` for details.

        .. versionadded:: 3.11
        """
        if not isinstance(uuid, UUID):
            raise TypeError("uuid must be an instance of uuid.UUID")

        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError(
                "uuid_representation must be a value from bson.binary.UuidRepresentation"
            )

        if uuid_representation == UuidRepresentation.UNSPECIFIED:
            raise ValueError(
                "cannot encode native uuid.UUID with "
                "UuidRepresentation.UNSPECIFIED. UUIDs can be manually "
                "converted to bson.Binary instances using "
                "bson.Binary.from_uuid() or a different UuidRepresentation "
                "can be configured. See the documentation for "
                "UuidRepresentation for more information."
            )

        subtype = OLD_UUID_SUBTYPE
        if uuid_representation == UuidRepresentation.PYTHON_LEGACY:
            payload = uuid.bytes
        elif uuid_representation == UuidRepresentation.JAVA_LEGACY:
            from_uuid = uuid.bytes
            payload = from_uuid[0:8][::-1] + from_uuid[8:16][::-1]
        elif uuid_representation == UuidRepresentation.CSHARP_LEGACY:
            payload = uuid.bytes_le
        else:
            # uuid_representation == UuidRepresentation.STANDARD
            subtype = UUID_SUBTYPE
            payload = uuid.bytes

        return cls(payload, subtype)

    def as_uuid(self, uuid_representation: int = UuidRepresentation.STANDARD) -> UUID:
        """Create a Python UUID from this BSON Binary object.

        Decodes this binary object as a native :class:`uuid.UUID` instance
        with the provided ``uuid_representation``.

        Raises :exc:`ValueError` if this :class:`~bson.binary.Binary` instance
        does not contain a UUID.

        :param uuid_representation: A member of
            :class:`~bson.binary.UuidRepresentation`. Default:
            :const:`~bson.binary.UuidRepresentation.STANDARD`.
            See :ref:`handling-uuid-data-example` for details.

        .. versionadded:: 3.11
        """
        if self.subtype not in ALL_UUID_SUBTYPES:
            raise ValueError(f"cannot decode subtype {self.subtype} as a uuid")

        if uuid_representation not in ALL_UUID_REPRESENTATIONS:
            raise ValueError(
                "uuid_representation must be a value from bson.binary.UuidRepresentation"
            )

        if uuid_representation == UuidRepresentation.UNSPECIFIED:
            raise ValueError("uuid_representation cannot be UNSPECIFIED")
        elif uuid_representation == UuidRepresentation.PYTHON_LEGACY:
            if self.subtype == OLD_UUID_SUBTYPE:
                return UUID(bytes=self)
        elif uuid_representation == UuidRepresentation.JAVA_LEGACY:
            if self.subtype == OLD_UUID_SUBTYPE:
                return UUID(bytes=self[0:8][::-1] + self[8:16][::-1])
        elif uuid_representation == UuidRepresentation.CSHARP_LEGACY:
            if self.subtype == OLD_UUID_SUBTYPE:
                return UUID(bytes_le=self)
        else:
            # uuid_representation == UuidRepresentation.STANDARD
            if self.subtype == UUID_SUBTYPE:
                return UUID(bytes=self)

        raise ValueError(
            f"cannot decode subtype {self.subtype} to {UUID_REPRESENTATION_NAMES[uuid_representation]}"
        )

    @property
    def subtype(self) -> int:
        """Subtype of this binary data."""
        return self.__subtype

    def __getnewargs__(self) -> Tuple[bytes, int]:  # type: ignore[override]
        # Work around http://bugs.python.org/issue7382
        data = super().__getnewargs__()[0]
        if not isinstance(data, bytes):
            data = data.encode("latin-1")
        return data, self.__subtype

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Binary):
            return (self.__subtype, bytes(self)) == (other.subtype, bytes(other))
        # We don't return NotImplemented here because if we did then
        # Binary("foo") == "foo" would return True, since Binary is a
        # subclass of str...
        return False

    def __hash__(self) -> int:
        return super().__hash__() ^ hash(self.__subtype)

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __repr__(self) -> str:
        if self.__subtype == SENSITIVE_SUBTYPE:
            return f"<Binary(REDACTED, {self.__subtype})>"
        else:
            return f"Binary({bytes.__repr__(self)}, {self.__subtype})"
