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

"""Tools for using Python's :mod:`json` module with BSON documents.

This module provides two helper methods `dumps` and `loads` that wrap the
native :mod:`json` methods and provide explicit BSON conversion to and from
JSON. :class:`~bson.json_util.JSONOptions` provides a way to control how JSON
is emitted and parsed, with the default being the Relaxed Extended JSON format.
:mod:`~bson.json_util` can also generate Canonical or legacy `Extended JSON`_
when :const:`CANONICAL_JSON_OPTIONS` or :const:`LEGACY_JSON_OPTIONS` is
provided, respectively.

.. _Extended JSON: https://github.com/mongodb/specifications/blob/master/source/extended-json.rst

Example usage (deserialization):

.. doctest::

   >>> from bson.json_util import loads
   >>> loads(
   ...     '[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$scope": {}, "$code": "function x() { return 1; }"}}, {"bin": {"$type": "80", "$binary": "AQIDBA=="}}]'
   ... )
   [{'foo': [1, 2]}, {'bar': {'hello': 'world'}}, {'code': Code('function x() { return 1; }', {})}, {'bin': Binary(b'...', 128)}]

Example usage with :const:`RELAXED_JSON_OPTIONS` (the default):

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps
   >>> dumps(
   ...     [
   ...         {"foo": [1, 2]},
   ...         {"bar": {"hello": "world"}},
   ...         {"code": Code("function x() { return 1; }")},
   ...         {"bin": Binary(b"\x01\x02\x03\x04")},
   ...     ]
   ... )
   '[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }"}}, {"bin": {"$binary": {"base64": "AQIDBA==", "subType": "00"}}}]'

Example usage (with :const:`CANONICAL_JSON_OPTIONS`):

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps, CANONICAL_JSON_OPTIONS
   >>> dumps(
   ...     [
   ...         {"foo": [1, 2]},
   ...         {"bar": {"hello": "world"}},
   ...         {"code": Code("function x() { return 1; }")},
   ...         {"bin": Binary(b"\x01\x02\x03\x04")},
   ...     ],
   ...     json_options=CANONICAL_JSON_OPTIONS,
   ... )
   '[{"foo": [{"$numberInt": "1"}, {"$numberInt": "2"}]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }"}}, {"bin": {"$binary": {"base64": "AQIDBA==", "subType": "00"}}}]'

Example usage (with :const:`LEGACY_JSON_OPTIONS`):

.. doctest::

   >>> from bson import Binary, Code
   >>> from bson.json_util import dumps, LEGACY_JSON_OPTIONS
   >>> dumps(
   ...     [
   ...         {"foo": [1, 2]},
   ...         {"bar": {"hello": "world"}},
   ...         {"code": Code("function x() { return 1; }", {})},
   ...         {"bin": Binary(b"\x01\x02\x03\x04")},
   ...     ],
   ...     json_options=LEGACY_JSON_OPTIONS,
   ... )
   '[{"foo": [1, 2]}, {"bar": {"hello": "world"}}, {"code": {"$code": "function x() { return 1; }", "$scope": {}}}, {"bin": {"$binary": "AQIDBA==", "$type": "00"}}]'

Alternatively, you can manually pass the `default` to :func:`json.dumps`.
It won't handle :class:`~bson.binary.Binary` and :class:`~bson.code.Code`
instances (as they are extended strings you can't provide custom defaults),
but it will be faster as there is less recursion.

.. note::
   If your application does not need the flexibility offered by
   :class:`JSONOptions` and spends a large amount of time in the `json_util`
   module, look to
   `python-bsonjs <https://pypi.python.org/pypi/python-bsonjs>`_ for a nice
   performance improvement. `python-bsonjs` is a fast BSON to MongoDB
   Extended JSON converter for Python built on top of
   `libbson <https://github.com/mongodb/libbson>`_. `python-bsonjs` works best
   with PyMongo when using :class:`~bson.raw_bson.RawBSONDocument`.
"""
from __future__ import annotations

import base64
import datetime
import json
import math
import re
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

from bson.binary import ALL_UUID_SUBTYPES, UUID_SUBTYPE, Binary, UuidRepresentation
from bson.code import Code
from bson.codec_options import CodecOptions, DatetimeConversion
from bson.datetime_ms import (
    EPOCH_AWARE,
    DatetimeMS,
    _datetime_to_millis,
    _max_datetime_ms,
    _millis_to_datetime,
)
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.son import RE_TYPE
from bson.timestamp import Timestamp
from bson.tz_util import utc

_RE_OPT_TABLE = {
    "i": re.I,
    "l": re.L,
    "m": re.M,
    "s": re.S,
    "u": re.U,
    "x": re.X,
}


class DatetimeRepresentation:
    LEGACY = 0
    """Legacy MongoDB Extended JSON datetime representation.

    :class:`datetime.datetime` instances will be encoded to JSON in the
    format `{"$date": <dateAsMilliseconds>}`, where `dateAsMilliseconds` is
    a 64-bit signed integer giving the number of milliseconds since the Unix
    epoch UTC. This was the default encoding before PyMongo version 3.4.

    .. versionadded:: 3.4
    """

    NUMBERLONG = 1
    """NumberLong datetime representation.

    :class:`datetime.datetime` instances will be encoded to JSON in the
    format `{"$date": {"$numberLong": "<dateAsMilliseconds>"}}`,
    where `dateAsMilliseconds` is the string representation of a 64-bit signed
    integer giving the number of milliseconds since the Unix epoch UTC.

    .. versionadded:: 3.4
    """

    ISO8601 = 2
    """ISO-8601 datetime representation.

    :class:`datetime.datetime` instances greater than or equal to the Unix
    epoch UTC will be encoded to JSON in the format `{"$date": "<ISO-8601>"}`.
    :class:`datetime.datetime` instances before the Unix epoch UTC will be
    encoded as if the datetime representation is
    :const:`~DatetimeRepresentation.NUMBERLONG`.

    .. versionadded:: 3.4
    """


class JSONMode:
    LEGACY = 0
    """Legacy Extended JSON representation.

    In this mode, :func:`~bson.json_util.dumps` produces PyMongo's legacy
    non-standard JSON output. Consider using
    :const:`~bson.json_util.JSONMode.RELAXED` or
    :const:`~bson.json_util.JSONMode.CANONICAL` instead.

    .. versionadded:: 3.5
    """

    RELAXED = 1
    """Relaxed Extended JSON representation.

    In this mode, :func:`~bson.json_util.dumps` produces Relaxed Extended JSON,
    a mostly JSON-like format. Consider using this for things like a web API,
    where one is sending a document (or a projection of a document) that only
    uses ordinary JSON type primitives. In particular, the ``int``,
    :class:`~bson.int64.Int64`, and ``float`` numeric types are represented in
    the native JSON number format. This output is also the most human readable
    and is useful for debugging and documentation.

    .. seealso:: The specification for Relaxed `Extended JSON`_.

    .. versionadded:: 3.5
    """

    CANONICAL = 2
    """Canonical Extended JSON representation.

    In this mode, :func:`~bson.json_util.dumps` produces Canonical Extended
    JSON, a type preserving format. Consider using this for things like
    testing, where one has to precisely specify expected types in JSON. In
    particular, the ``int``, :class:`~bson.int64.Int64`, and ``float`` numeric
    types are encoded with type wrappers.

    .. seealso:: The specification for Canonical `Extended JSON`_.

    .. versionadded:: 3.5
    """


if TYPE_CHECKING:
    _BASE_CLASS = CodecOptions[MutableMapping[str, Any]]
else:
    _BASE_CLASS = CodecOptions

_INT32_MAX = 2**31


class JSONOptions(_BASE_CLASS):
    json_mode: int
    strict_number_long: bool
    datetime_representation: int
    strict_uuid: bool
    document_class: Type[MutableMapping[str, Any]]

    def __init__(self, *args: Any, **kwargs: Any):
        """Encapsulates JSON options for :func:`dumps` and :func:`loads`.

        :param strict_number_long: If ``True``, :class:`~bson.int64.Int64` objects
            are encoded to MongoDB Extended JSON's *Strict mode* type
            `NumberLong`, ie ``'{"$numberLong": "<number>" }'``. Otherwise they
            will be encoded as an `int`. Defaults to ``False``.
        :param datetime_representation: The representation to use when encoding
            instances of :class:`datetime.datetime`. Defaults to
            :const:`~DatetimeRepresentation.LEGACY`.
        :param strict_uuid: If ``True``, :class:`uuid.UUID` object are encoded to
            MongoDB Extended JSON's *Strict mode* type `Binary`. Otherwise it
            will be encoded as ``'{"$uuid": "<hex>" }'``. Defaults to ``False``.
        :param json_mode: The :class:`JSONMode` to use when encoding BSON types to
            Extended JSON. Defaults to :const:`~JSONMode.LEGACY`.
        :param document_class: BSON documents returned by :func:`loads` will be
            decoded to an instance of this class. Must be a subclass of
            :class:`collections.MutableMapping`. Defaults to :class:`dict`.
        :param uuid_representation: The :class:`~bson.binary.UuidRepresentation`
            to use when encoding and decoding instances of :class:`uuid.UUID`.
            Defaults to :const:`~bson.binary.UuidRepresentation.UNSPECIFIED`.
        :param tz_aware: If ``True``, MongoDB Extended JSON's *Strict mode* type
            `Date` will be decoded to timezone aware instances of
            :class:`datetime.datetime`. Otherwise they will be naive. Defaults
            to ``False``.
        :param tzinfo: A :class:`datetime.tzinfo` subclass that specifies the
            timezone from which :class:`~datetime.datetime` objects should be
            decoded. Defaults to :const:`~bson.tz_util.utc`.
        :param datetime_conversion: Specifies how UTC datetimes should be decoded
            within BSON. Valid options include 'datetime_ms' to return as a
            DatetimeMS, 'datetime' to return as a datetime.datetime and
            raising a ValueError for out-of-range values, 'datetime_auto' to
            return DatetimeMS objects when the underlying datetime is
            out-of-range and 'datetime_clamp' to clamp to the minimum and
            maximum possible datetimes. Defaults to 'datetime'. See
            :ref:`handling-out-of-range-datetimes` for details.
        :param args: arguments to :class:`~bson.codec_options.CodecOptions`
        :param kwargs: arguments to :class:`~bson.codec_options.CodecOptions`

        .. seealso:: The specification for Relaxed and Canonical `Extended JSON`_.

        .. versionchanged:: 4.0
           The default for `json_mode` was changed from :const:`JSONMode.LEGACY`
           to :const:`JSONMode.RELAXED`.
           The default for `uuid_representation` was changed from
           :const:`~bson.binary.UuidRepresentation.PYTHON_LEGACY` to
           :const:`~bson.binary.UuidRepresentation.UNSPECIFIED`.

        .. versionchanged:: 3.5
           Accepts the optional parameter `json_mode`.

        .. versionchanged:: 4.0
           Changed default value of `tz_aware` to False.
        """
        super().__init__()

    def __new__(
        cls: Type[JSONOptions],
        strict_number_long: Optional[bool] = None,
        datetime_representation: Optional[int] = None,
        strict_uuid: Optional[bool] = None,
        json_mode: int = JSONMode.RELAXED,
        *args: Any,
        **kwargs: Any,
    ) -> JSONOptions:
        kwargs["tz_aware"] = kwargs.get("tz_aware", False)
        if kwargs["tz_aware"]:
            kwargs["tzinfo"] = kwargs.get("tzinfo", utc)
        if datetime_representation not in (
            DatetimeRepresentation.LEGACY,
            DatetimeRepresentation.NUMBERLONG,
            DatetimeRepresentation.ISO8601,
            None,
        ):
            raise ValueError(
                "JSONOptions.datetime_representation must be one of LEGACY, "
                "NUMBERLONG, or ISO8601 from DatetimeRepresentation."
            )
        self = cast(JSONOptions, super().__new__(cls, *args, **kwargs))  # type:ignore[arg-type]
        if json_mode not in (JSONMode.LEGACY, JSONMode.RELAXED, JSONMode.CANONICAL):
            raise ValueError(
                "JSONOptions.json_mode must be one of LEGACY, RELAXED, "
                "or CANONICAL from JSONMode."
            )
        self.json_mode = json_mode
        if self.json_mode == JSONMode.RELAXED:
            if strict_number_long:
                raise ValueError("Cannot specify strict_number_long=True with JSONMode.RELAXED")
            if datetime_representation not in (None, DatetimeRepresentation.ISO8601):
                raise ValueError(
                    "datetime_representation must be DatetimeRepresentation."
                    "ISO8601 or omitted with JSONMode.RELAXED"
                )
            if strict_uuid not in (None, True):
                raise ValueError("Cannot specify strict_uuid=False with JSONMode.RELAXED")
            self.strict_number_long = False
            self.datetime_representation = DatetimeRepresentation.ISO8601
            self.strict_uuid = True
        elif self.json_mode == JSONMode.CANONICAL:
            if strict_number_long not in (None, True):
                raise ValueError("Cannot specify strict_number_long=False with JSONMode.RELAXED")
            if datetime_representation not in (None, DatetimeRepresentation.NUMBERLONG):
                raise ValueError(
                    "datetime_representation must be DatetimeRepresentation."
                    "NUMBERLONG or omitted with JSONMode.RELAXED"
                )
            if strict_uuid not in (None, True):
                raise ValueError("Cannot specify strict_uuid=False with JSONMode.RELAXED")
            self.strict_number_long = True
            self.datetime_representation = DatetimeRepresentation.NUMBERLONG
            self.strict_uuid = True
        else:  # JSONMode.LEGACY
            self.strict_number_long = False
            self.datetime_representation = DatetimeRepresentation.LEGACY
            self.strict_uuid = False
            if strict_number_long is not None:
                self.strict_number_long = strict_number_long
            if datetime_representation is not None:
                self.datetime_representation = datetime_representation
            if strict_uuid is not None:
                self.strict_uuid = strict_uuid
        return self

    def _arguments_repr(self) -> str:
        return (
            "strict_number_long={!r}, "
            "datetime_representation={!r}, "
            "strict_uuid={!r}, json_mode={!r}, {}".format(
                self.strict_number_long,
                self.datetime_representation,
                self.strict_uuid,
                self.json_mode,
                super()._arguments_repr(),
            )
        )

    def _options_dict(self) -> dict[Any, Any]:
        # TODO: PYTHON-2442 use _asdict() instead
        options_dict = super()._options_dict()
        options_dict.update(
            {
                "strict_number_long": self.strict_number_long,
                "datetime_representation": self.datetime_representation,
                "strict_uuid": self.strict_uuid,
                "json_mode": self.json_mode,
            }
        )
        return options_dict

    def with_options(self, **kwargs: Any) -> JSONOptions:
        """
        Make a copy of this JSONOptions, overriding some options::

            >>> from bson.json_util import CANONICAL_JSON_OPTIONS
            >>> CANONICAL_JSON_OPTIONS.tz_aware
            True
            >>> json_options = CANONICAL_JSON_OPTIONS.with_options(tz_aware=False, tzinfo=None)
            >>> json_options.tz_aware
            False

        .. versionadded:: 3.12
        """
        opts = self._options_dict()
        for opt in ("strict_number_long", "datetime_representation", "strict_uuid", "json_mode"):
            opts[opt] = kwargs.get(opt, getattr(self, opt))
        opts.update(kwargs)
        return JSONOptions(**opts)


LEGACY_JSON_OPTIONS: JSONOptions = JSONOptions(json_mode=JSONMode.LEGACY)
""":class:`JSONOptions` for encoding to PyMongo's legacy JSON format.

.. seealso:: The documentation for :const:`bson.json_util.JSONMode.LEGACY`.

.. versionadded:: 3.5
"""

CANONICAL_JSON_OPTIONS: JSONOptions = JSONOptions(json_mode=JSONMode.CANONICAL)
""":class:`JSONOptions` for Canonical Extended JSON.

.. seealso:: The documentation for :const:`bson.json_util.JSONMode.CANONICAL`.

.. versionadded:: 3.5
"""

RELAXED_JSON_OPTIONS: JSONOptions = JSONOptions(json_mode=JSONMode.RELAXED)
""":class:`JSONOptions` for Relaxed Extended JSON.

.. seealso:: The documentation for :const:`bson.json_util.JSONMode.RELAXED`.

.. versionadded:: 3.5
"""

DEFAULT_JSON_OPTIONS: JSONOptions = RELAXED_JSON_OPTIONS
"""The default :class:`JSONOptions` for JSON encoding/decoding.

The same as :const:`RELAXED_JSON_OPTIONS`.

.. versionchanged:: 4.0
   Changed from :const:`LEGACY_JSON_OPTIONS` to
   :const:`RELAXED_JSON_OPTIONS`.

.. versionadded:: 3.4
"""


def dumps(obj: Any, *args: Any, **kwargs: Any) -> str:
    """Helper function that wraps :func:`json.dumps`.

    Recursive function that handles all BSON types including
    :class:`~bson.binary.Binary` and :class:`~bson.code.Code`.

    :param json_options: A :class:`JSONOptions` instance used to modify the
        encoding of MongoDB Extended JSON types. Defaults to
        :const:`DEFAULT_JSON_OPTIONS`.

    .. versionchanged:: 4.0
       Now outputs MongoDB Relaxed Extended JSON by default (using
       :const:`DEFAULT_JSON_OPTIONS`).

    .. versionchanged:: 3.4
       Accepts optional parameter `json_options`. See :class:`JSONOptions`.
    """
    json_options = kwargs.pop("json_options", DEFAULT_JSON_OPTIONS)
    return json.dumps(_json_convert(obj, json_options), *args, **kwargs)


def loads(s: Union[str, bytes, bytearray], *args: Any, **kwargs: Any) -> Any:
    """Helper function that wraps :func:`json.loads`.

    Automatically passes the object_hook for BSON type conversion.

    Raises ``TypeError``, ``ValueError``, ``KeyError``, or
    :exc:`~bson.errors.InvalidId` on invalid MongoDB Extended JSON.

    :param json_options: A :class:`JSONOptions` instance used to modify the
        decoding of MongoDB Extended JSON types. Defaults to
        :const:`DEFAULT_JSON_OPTIONS`.

    .. versionchanged:: 4.0
       Now loads :class:`datetime.datetime` instances as naive by default. To
       load timezone aware instances utilize the `json_options` parameter.
       See :ref:`tz_aware_default_change` for an example.

    .. versionchanged:: 3.5
       Parses Relaxed and Canonical Extended JSON as well as PyMongo's legacy
       format. Now raises ``TypeError`` or ``ValueError`` when parsing JSON
       type wrappers with values of the wrong type or any extra keys.

    .. versionchanged:: 3.4
       Accepts optional parameter `json_options`. See :class:`JSONOptions`.
    """
    json_options = kwargs.pop("json_options", DEFAULT_JSON_OPTIONS)
    # Execution time optimization if json_options.document_class is dict
    if json_options.document_class is dict:
        kwargs["object_hook"] = lambda obj: object_hook(obj, json_options)
    else:
        kwargs["object_pairs_hook"] = lambda pairs: object_pairs_hook(pairs, json_options)
    return json.loads(s, *args, **kwargs)


def _json_convert(obj: Any, json_options: JSONOptions = DEFAULT_JSON_OPTIONS) -> Any:
    """Recursive helper method that converts BSON types so they can be
    converted into json.
    """
    if hasattr(obj, "items"):
        return {k: _json_convert(v, json_options) for k, v in obj.items()}
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes)):
        return [_json_convert(v, json_options) for v in obj]
    try:
        return default(obj, json_options)
    except TypeError:
        return obj


def object_pairs_hook(
    pairs: Sequence[Tuple[str, Any]], json_options: JSONOptions = DEFAULT_JSON_OPTIONS
) -> Any:
    return object_hook(json_options.document_class(pairs), json_options)  # type:ignore[call-arg]


def object_hook(dct: Mapping[str, Any], json_options: JSONOptions = DEFAULT_JSON_OPTIONS) -> Any:
    match = None
    for k in dct:
        if k in _PARSERS_SET:
            match = k
            break
    if match:
        return _PARSERS[match](dct, json_options)
    return dct


def _parse_legacy_regex(doc: Any, dummy0: Any) -> Any:
    pattern = doc["$regex"]
    # Check if this is the $regex query operator.
    if not isinstance(pattern, (str, bytes)):
        return doc
    flags = 0
    # PyMongo always adds $options but some other tools may not.
    for opt in doc.get("$options", ""):
        flags |= _RE_OPT_TABLE.get(opt, 0)
    return Regex(pattern, flags)


def _parse_legacy_uuid(doc: Any, json_options: JSONOptions) -> Union[Binary, uuid.UUID]:
    """Decode a JSON legacy $uuid to Python UUID."""
    if len(doc) != 1:
        raise TypeError(f"Bad $uuid, extra field(s): {doc}")
    if not isinstance(doc["$uuid"], str):
        raise TypeError(f"$uuid must be a string: {doc}")
    if json_options.uuid_representation == UuidRepresentation.UNSPECIFIED:
        return Binary.from_uuid(uuid.UUID(doc["$uuid"]))
    else:
        return uuid.UUID(doc["$uuid"])


def _binary_or_uuid(data: Any, subtype: int, json_options: JSONOptions) -> Union[Binary, uuid.UUID]:
    # special handling for UUID
    if subtype in ALL_UUID_SUBTYPES:
        uuid_representation = json_options.uuid_representation
        binary_value = Binary(data, subtype)
        if uuid_representation == UuidRepresentation.UNSPECIFIED:
            return binary_value
        if subtype == UUID_SUBTYPE:
            # Legacy behavior: use STANDARD with binary subtype 4.
            uuid_representation = UuidRepresentation.STANDARD
        elif uuid_representation == UuidRepresentation.STANDARD:
            # subtype == OLD_UUID_SUBTYPE
            # Legacy behavior: STANDARD is the same as PYTHON_LEGACY.
            uuid_representation = UuidRepresentation.PYTHON_LEGACY
        return binary_value.as_uuid(uuid_representation)

    if subtype == 0:
        return cast(uuid.UUID, data)
    return Binary(data, subtype)


def _parse_legacy_binary(doc: Any, json_options: JSONOptions) -> Union[Binary, uuid.UUID]:
    if isinstance(doc["$type"], int):
        doc["$type"] = "%02x" % doc["$type"]
    subtype = int(doc["$type"], 16)
    if subtype >= 0xFFFFFF80:  # Handle mongoexport values
        subtype = int(doc["$type"][6:], 16)
    data = base64.b64decode(doc["$binary"].encode())
    return _binary_or_uuid(data, subtype, json_options)


def _parse_canonical_binary(doc: Any, json_options: JSONOptions) -> Union[Binary, uuid.UUID]:
    binary = doc["$binary"]
    b64 = binary["base64"]
    subtype = binary["subType"]
    if not isinstance(b64, str):
        raise TypeError(f"$binary base64 must be a string: {doc}")
    if not isinstance(subtype, str) or len(subtype) > 2:
        raise TypeError(f"$binary subType must be a string at most 2 characters: {doc}")
    if len(binary) != 2:
        raise TypeError(f'$binary must include only "base64" and "subType" components: {doc}')

    data = base64.b64decode(b64.encode())
    return _binary_or_uuid(data, int(subtype, 16), json_options)


def _parse_canonical_datetime(
    doc: Any, json_options: JSONOptions
) -> Union[datetime.datetime, DatetimeMS]:
    """Decode a JSON datetime to python datetime.datetime."""
    dtm = doc["$date"]
    if len(doc) != 1:
        raise TypeError(f"Bad $date, extra field(s): {doc}")
    # mongoexport 2.6 and newer
    if isinstance(dtm, str):
        # Parse offset
        if dtm[-1] == "Z":
            dt = dtm[:-1]
            offset = "Z"
        elif dtm[-6] in ("+", "-") and dtm[-3] == ":":
            # (+|-)HH:MM
            dt = dtm[:-6]
            offset = dtm[-6:]
        elif dtm[-5] in ("+", "-"):
            # (+|-)HHMM
            dt = dtm[:-5]
            offset = dtm[-5:]
        elif dtm[-3] in ("+", "-"):
            # (+|-)HH
            dt = dtm[:-3]
            offset = dtm[-3:]
        else:
            dt = dtm
            offset = ""

        # Parse the optional factional seconds portion.
        dot_index = dt.rfind(".")
        microsecond = 0
        if dot_index != -1:
            microsecond = int(float(dt[dot_index:]) * 1000000)
            dt = dt[:dot_index]

        aware = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S").replace(
            microsecond=microsecond, tzinfo=utc
        )

        if offset and offset != "Z":
            if len(offset) == 6:
                hours, minutes = offset[1:].split(":")
                secs = int(hours) * 3600 + int(minutes) * 60
            elif len(offset) == 5:
                secs = int(offset[1:3]) * 3600 + int(offset[3:]) * 60
            elif len(offset) == 3:
                secs = int(offset[1:3]) * 3600
            if offset[0] == "-":
                secs *= -1
            aware = aware - datetime.timedelta(seconds=secs)

        if json_options.tz_aware:
            if json_options.tzinfo:
                aware = aware.astimezone(json_options.tzinfo)
            if json_options.datetime_conversion == DatetimeConversion.DATETIME_MS:
                return DatetimeMS(aware)
            return aware
        else:
            aware_tzinfo_none = aware.replace(tzinfo=None)
            if json_options.datetime_conversion == DatetimeConversion.DATETIME_MS:
                return DatetimeMS(aware_tzinfo_none)
            return aware_tzinfo_none
    return _millis_to_datetime(int(dtm), cast("CodecOptions[Any]", json_options))


def _parse_canonical_oid(doc: Any, dummy0: Any) -> ObjectId:
    """Decode a JSON ObjectId to bson.objectid.ObjectId."""
    if len(doc) != 1:
        raise TypeError(f"Bad $oid, extra field(s): {doc}")
    return ObjectId(doc["$oid"])


def _parse_canonical_symbol(doc: Any, dummy0: Any) -> str:
    """Decode a JSON symbol to Python string."""
    symbol = doc["$symbol"]
    if len(doc) != 1:
        raise TypeError(f"Bad $symbol, extra field(s): {doc}")
    return str(symbol)


def _parse_canonical_code(doc: Any, dummy0: Any) -> Code:
    """Decode a JSON code to bson.code.Code."""
    for key in doc:
        if key not in ("$code", "$scope"):
            raise TypeError(f"Bad $code, extra field(s): {doc}")
    return Code(doc["$code"], scope=doc.get("$scope"))


def _parse_canonical_regex(doc: Any, dummy0: Any) -> Regex[str]:
    """Decode a JSON regex to bson.regex.Regex."""
    regex = doc["$regularExpression"]
    if len(doc) != 1:
        raise TypeError(f"Bad $regularExpression, extra field(s): {doc}")
    if len(regex) != 2:
        raise TypeError(
            f'Bad $regularExpression must include only "pattern and "options" components: {doc}'
        )
    opts = regex["options"]
    if not isinstance(opts, str):
        raise TypeError(
            "Bad $regularExpression options, options must be string, was type %s" % (type(opts))
        )
    return Regex(regex["pattern"], opts)


def _parse_canonical_dbref(doc: Any, dummy0: Any) -> Any:
    """Decode a JSON DBRef to bson.dbref.DBRef."""
    if (
        isinstance(doc.get("$ref"), str)
        and "$id" in doc
        and isinstance(doc.get("$db"), (str, type(None)))
    ):
        return DBRef(doc.pop("$ref"), doc.pop("$id"), database=doc.pop("$db", None), **doc)
    return doc


def _parse_canonical_dbpointer(doc: Any, dummy0: Any) -> Any:
    """Decode a JSON (deprecated) DBPointer to bson.dbref.DBRef."""
    dbref = doc["$dbPointer"]
    if len(doc) != 1:
        raise TypeError(f"Bad $dbPointer, extra field(s): {doc}")
    if isinstance(dbref, DBRef):
        dbref_doc = dbref.as_doc()
        # DBPointer must not contain $db in its value.
        if dbref.database is not None:
            raise TypeError(f"Bad $dbPointer, extra field $db: {dbref_doc}")
        if not isinstance(dbref.id, ObjectId):
            raise TypeError(f"Bad $dbPointer, $id must be an ObjectId: {dbref_doc}")
        if len(dbref_doc) != 2:
            raise TypeError(f"Bad $dbPointer, extra field(s) in DBRef: {dbref_doc}")
        return dbref
    else:
        raise TypeError(f"Bad $dbPointer, expected a DBRef: {doc}")


def _parse_canonical_int32(doc: Any, dummy0: Any) -> int:
    """Decode a JSON int32 to python int."""
    i_str = doc["$numberInt"]
    if len(doc) != 1:
        raise TypeError(f"Bad $numberInt, extra field(s): {doc}")
    if not isinstance(i_str, str):
        raise TypeError(f"$numberInt must be string: {doc}")
    return int(i_str)


def _parse_canonical_int64(doc: Any, dummy0: Any) -> Int64:
    """Decode a JSON int64 to bson.int64.Int64."""
    l_str = doc["$numberLong"]
    if len(doc) != 1:
        raise TypeError(f"Bad $numberLong, extra field(s): {doc}")
    return Int64(l_str)


def _parse_canonical_double(doc: Any, dummy0: Any) -> float:
    """Decode a JSON double to python float."""
    d_str = doc["$numberDouble"]
    if len(doc) != 1:
        raise TypeError(f"Bad $numberDouble, extra field(s): {doc}")
    if not isinstance(d_str, str):
        raise TypeError(f"$numberDouble must be string: {doc}")
    return float(d_str)


def _parse_canonical_decimal128(doc: Any, dummy0: Any) -> Decimal128:
    """Decode a JSON decimal128 to bson.decimal128.Decimal128."""
    d_str = doc["$numberDecimal"]
    if len(doc) != 1:
        raise TypeError(f"Bad $numberDecimal, extra field(s): {doc}")
    if not isinstance(d_str, str):
        raise TypeError(f"$numberDecimal must be string: {doc}")
    return Decimal128(d_str)


def _parse_canonical_minkey(doc: Any, dummy0: Any) -> MinKey:
    """Decode a JSON MinKey to bson.min_key.MinKey."""
    if type(doc["$minKey"]) is not int or doc["$minKey"] != 1:  # noqa: E721
        raise TypeError(f"$minKey value must be 1: {doc}")
    if len(doc) != 1:
        raise TypeError(f"Bad $minKey, extra field(s): {doc}")
    return MinKey()


def _parse_canonical_maxkey(doc: Any, dummy0: Any) -> MaxKey:
    """Decode a JSON MaxKey to bson.max_key.MaxKey."""
    if type(doc["$maxKey"]) is not int or doc["$maxKey"] != 1:  # noqa: E721
        raise TypeError("$maxKey value must be 1: %s", (doc,))
    if len(doc) != 1:
        raise TypeError(f"Bad $minKey, extra field(s): {doc}")
    return MaxKey()


def _parse_binary(doc: Any, json_options: JSONOptions) -> Union[Binary, uuid.UUID]:
    if "$type" in doc:
        return _parse_legacy_binary(doc, json_options)
    else:
        return _parse_canonical_binary(doc, json_options)


def _parse_timestamp(doc: Any, dummy0: Any) -> Timestamp:
    tsp = doc["$timestamp"]
    return Timestamp(tsp["t"], tsp["i"])


_PARSERS: dict[str, Callable[[Any, JSONOptions], Any]] = {
    "$oid": _parse_canonical_oid,
    "$ref": _parse_canonical_dbref,
    "$date": _parse_canonical_datetime,
    "$regex": _parse_legacy_regex,
    "$minKey": _parse_canonical_minkey,
    "$maxKey": _parse_canonical_maxkey,
    "$binary": _parse_binary,
    "$code": _parse_canonical_code,
    "$uuid": _parse_legacy_uuid,
    "$undefined": lambda _, _1: None,
    "$numberLong": _parse_canonical_int64,
    "$timestamp": _parse_timestamp,
    "$numberDecimal": _parse_canonical_decimal128,
    "$dbPointer": _parse_canonical_dbpointer,
    "$regularExpression": _parse_canonical_regex,
    "$symbol": _parse_canonical_symbol,
    "$numberInt": _parse_canonical_int32,
    "$numberDouble": _parse_canonical_double,
}
_PARSERS_SET = set(_PARSERS)


def _encode_binary(data: bytes, subtype: int, json_options: JSONOptions) -> Any:
    if json_options.json_mode == JSONMode.LEGACY:
        return {"$binary": base64.b64encode(data).decode(), "$type": "%02x" % subtype}
    return {"$binary": {"base64": base64.b64encode(data).decode(), "subType": "%02x" % subtype}}


def _encode_datetimems(obj: Any, json_options: JSONOptions) -> dict:
    if (
        json_options.datetime_representation == DatetimeRepresentation.ISO8601
        and 0 <= int(obj) <= _max_datetime_ms()
    ):
        return _encode_datetime(obj.as_datetime(), json_options)
    elif json_options.datetime_representation == DatetimeRepresentation.LEGACY:
        return {"$date": str(int(obj))}
    return {"$date": {"$numberLong": str(int(obj))}}


def _encode_code(obj: Code, json_options: JSONOptions) -> dict:
    if obj.scope is None:
        return {"$code": str(obj)}
    else:
        return {"$code": str(obj), "$scope": _json_convert(obj.scope, json_options)}


def _encode_int64(obj: Int64, json_options: JSONOptions) -> Any:
    if json_options.strict_number_long:
        return {"$numberLong": str(obj)}
    else:
        return int(obj)


def _encode_noop(obj: Any, dummy0: Any) -> Any:
    return obj


def _encode_regex(obj: Any, json_options: JSONOptions) -> dict:
    flags = ""
    if obj.flags & re.IGNORECASE:
        flags += "i"
    if obj.flags & re.LOCALE:
        flags += "l"
    if obj.flags & re.MULTILINE:
        flags += "m"
    if obj.flags & re.DOTALL:
        flags += "s"
    if obj.flags & re.UNICODE:
        flags += "u"
    if obj.flags & re.VERBOSE:
        flags += "x"
    if isinstance(obj.pattern, str):
        pattern = obj.pattern
    else:
        pattern = obj.pattern.decode("utf-8")
    if json_options.json_mode == JSONMode.LEGACY:
        return {"$regex": pattern, "$options": flags}
    return {"$regularExpression": {"pattern": pattern, "options": flags}}


def _encode_int(obj: int, json_options: JSONOptions) -> Any:
    if json_options.json_mode == JSONMode.CANONICAL:
        if -_INT32_MAX <= obj < _INT32_MAX:
            return {"$numberInt": str(obj)}
        return {"$numberLong": str(obj)}
    return obj


def _encode_float(obj: float, json_options: JSONOptions) -> Any:
    if json_options.json_mode != JSONMode.LEGACY:
        if math.isnan(obj):
            return {"$numberDouble": "NaN"}
        elif math.isinf(obj):
            representation = "Infinity" if obj > 0 else "-Infinity"
            return {"$numberDouble": representation}
        elif json_options.json_mode == JSONMode.CANONICAL:
            # repr() will return the shortest string guaranteed to produce the
            # original value, when float() is called on it.
            return {"$numberDouble": str(repr(obj))}
    return obj


def _encode_datetime(obj: datetime.datetime, json_options: JSONOptions) -> dict:
    if json_options.datetime_representation == DatetimeRepresentation.ISO8601:
        if not obj.tzinfo:
            obj = obj.replace(tzinfo=utc)
            assert obj.tzinfo is not None
        if obj >= EPOCH_AWARE:
            off = obj.tzinfo.utcoffset(obj)
            if (off.days, off.seconds, off.microseconds) == (0, 0, 0):  # type: ignore
                tz_string = "Z"
            else:
                tz_string = obj.strftime("%z")
            millis = int(obj.microsecond / 1000)
            fracsecs = ".%03d" % (millis,) if millis else ""
            return {
                "$date": "{}{}{}".format(obj.strftime("%Y-%m-%dT%H:%M:%S"), fracsecs, tz_string)
            }

    millis = _datetime_to_millis(obj)
    if json_options.datetime_representation == DatetimeRepresentation.LEGACY:
        return {"$date": millis}
    return {"$date": {"$numberLong": str(millis)}}


def _encode_bytes(obj: bytes, json_options: JSONOptions) -> dict:
    return _encode_binary(obj, 0, json_options)


def _encode_binary_obj(obj: Binary, json_options: JSONOptions) -> dict:
    return _encode_binary(obj, obj.subtype, json_options)


def _encode_uuid(obj: uuid.UUID, json_options: JSONOptions) -> dict:
    if json_options.strict_uuid:
        binval = Binary.from_uuid(obj, uuid_representation=json_options.uuid_representation)
        return _encode_binary(binval, binval.subtype, json_options)
    else:
        return {"$uuid": obj.hex}


def _encode_objectid(obj: ObjectId, dummy0: Any) -> dict:
    return {"$oid": str(obj)}


def _encode_timestamp(obj: Timestamp, dummy0: Any) -> dict:
    return {"$timestamp": {"t": obj.time, "i": obj.inc}}


def _encode_decimal128(obj: Timestamp, dummy0: Any) -> dict:
    return {"$numberDecimal": str(obj)}


def _encode_dbref(obj: DBRef, json_options: JSONOptions) -> dict:
    return _json_convert(obj.as_doc(), json_options=json_options)


def _encode_minkey(dummy0: Any, dummy1: Any) -> dict:
    return {"$minKey": 1}


def _encode_maxkey(dummy0: Any, dummy1: Any) -> dict:
    return {"$maxKey": 1}


# Encoders for BSON types
# Each encoder function's signature is:
#   - obj: a Python data type, e.g. a Python int for _encode_int
#   - json_options: a JSONOptions
_ENCODERS: dict[Type, Callable[[Any, JSONOptions], Any]] = {
    bool: _encode_noop,
    bytes: _encode_bytes,
    datetime.datetime: _encode_datetime,
    DatetimeMS: _encode_datetimems,
    float: _encode_float,
    int: _encode_int,
    str: _encode_noop,
    type(None): _encode_noop,
    uuid.UUID: _encode_uuid,
    Binary: _encode_binary_obj,
    Int64: _encode_int64,
    Code: _encode_code,
    DBRef: _encode_dbref,
    MaxKey: _encode_maxkey,
    MinKey: _encode_minkey,
    ObjectId: _encode_objectid,
    Regex: _encode_regex,
    RE_TYPE: _encode_regex,
    Timestamp: _encode_timestamp,
    Decimal128: _encode_decimal128,
}

# Map each _type_marker to its encoder for faster lookup.
_MARKERS: dict[int, Callable[[Any, JSONOptions], Any]] = {}
for _typ in _ENCODERS:
    if hasattr(_typ, "_type_marker"):
        _MARKERS[_typ._type_marker] = _ENCODERS[_typ]

_BUILT_IN_TYPES = tuple(t for t in _ENCODERS)


def default(obj: Any, json_options: JSONOptions = DEFAULT_JSON_OPTIONS) -> Any:
    # First see if the type is already cached. KeyError will only ever
    # happen once per subtype.
    try:
        return _ENCODERS[type(obj)](obj, json_options)
    except KeyError:
        pass

    # Second, fall back to trying _type_marker. This has to be done
    # before the loop below since users could subclass one of our
    # custom types that subclasses a python built-in (e.g. Binary)
    if hasattr(obj, "_type_marker"):
        marker = obj._type_marker
        if marker in _MARKERS:
            func = _MARKERS[marker]
            # Cache this type for faster subsequent lookup.
            _ENCODERS[type(obj)] = func
            return func(obj, json_options)

    # Third, test each base type. This will only happen once for
    # a subtype of a supported base type.
    for base in _BUILT_IN_TYPES:
        if isinstance(obj, base):
            func = _ENCODERS[base]
            # Cache this type for faster subsequent lookup.
            _ENCODERS[type(obj)] = func
            return func(obj, json_options)

    raise TypeError("%r is not JSON serializable" % obj)


def _get_str_size(obj: Any) -> int:
    return len(obj)


def _get_datetime_size(obj: datetime.datetime) -> int:
    return 5 + len(str(obj.time()))


def _get_regex_size(obj: Regex) -> int:
    return 18 + len(obj.pattern)


def _get_dbref_size(obj: DBRef) -> int:
    return 34 + len(obj.collection)


_CONSTANT_SIZE_TABLE: dict[Any, int] = {
    ObjectId: 28,
    int: 11,
    Int64: 11,
    Decimal128: 11,
    Timestamp: 14,
    MinKey: 8,
    MaxKey: 8,
}

_VARIABLE_SIZE_TABLE: dict[Any, Callable[[Any], int]] = {
    str: _get_str_size,
    bytes: _get_str_size,
    datetime.datetime: _get_datetime_size,
    Regex: _get_regex_size,
    DBRef: _get_dbref_size,
}


def get_size(obj: Any, max_size: int, current_size: int = 0) -> int:
    """Recursively finds size of objects"""
    if current_size >= max_size:
        return current_size

    obj_type = type(obj)

    # Check to see if the obj has a constant size estimate
    try:
        return _CONSTANT_SIZE_TABLE[obj_type]
    except KeyError:
        pass

    # Check to see if the obj has a variable but simple size estimate
    try:
        return _VARIABLE_SIZE_TABLE[obj_type](obj)
    except KeyError:
        pass

    # Special cases that require recursion
    if obj_type == Code:
        if obj.scope:
            current_size += (
                5 + get_size(obj.scope, max_size, current_size) + len(obj) - len(obj.scope)
            )
        else:
            current_size += 5 + len(obj)
    elif obj_type == dict:
        for k, v in obj.items():
            current_size += get_size(k, max_size, current_size)
            current_size += get_size(v, max_size, current_size)
            if current_size >= max_size:
                return current_size
    elif hasattr(obj, "__iter__"):
        for i in obj:
            current_size += get_size(i, max_size, current_size)
            if current_size >= max_size:
                return current_size
    return current_size


def _truncate_documents(obj: Any, max_length: int) -> Tuple[Any, int]:
    """Recursively truncate documents as needed to fit inside max_length characters."""
    if max_length <= 0:
        return None, 0
    remaining = max_length
    if hasattr(obj, "items"):
        truncated: Any = {}
        for k, v in obj.items():
            truncated_v, remaining = _truncate_documents(v, remaining)
            if truncated_v:
                truncated[k] = truncated_v
            if remaining <= 0:
                break
        return truncated, remaining
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes)):
        truncated: Any = []  # type:ignore[no-redef]
        for v in obj:
            truncated_v, remaining = _truncate_documents(v, remaining)
            if truncated_v:
                truncated.append(truncated_v)
            if remaining <= 0:
                break
        return truncated, remaining
    else:
        return _truncate(obj, remaining)


def _truncate(obj: Any, remaining: int) -> Tuple[Any, int]:
    size = get_size(obj, remaining)

    if size <= remaining:
        return obj, remaining - size
    else:
        try:
            truncated = obj[:remaining]
        except TypeError:
            truncated = obj
        return truncated, remaining - size
