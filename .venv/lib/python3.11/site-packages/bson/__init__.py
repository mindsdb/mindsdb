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

"""BSON (Binary JSON) encoding and decoding.

The mapping from Python types to BSON types is as follows:

=======================================  =============  ===================
Python Type                              BSON Type      Supported Direction
=======================================  =============  ===================
None                                     null           both
bool                                     boolean        both
int [#int]_                              int32 / int64  py -> bson
`bson.int64.Int64`                       int64          both
float                                    number (real)  both
str                                      string         both
list                                     array          both
dict / `SON`                             object         both
datetime.datetime [#dt]_ [#dt2]_         date           both
`bson.regex.Regex`                       regex          both
compiled re [#re]_                       regex          py -> bson
`bson.binary.Binary`                     binary         both
`bson.objectid.ObjectId`                 oid            both
`bson.dbref.DBRef`                       dbref          both
None                                     undefined      bson -> py
`bson.code.Code`                         code           both
str                                      symbol         bson -> py
bytes [#bytes]_                          binary         both
=======================================  =============  ===================

.. [#int] A Python int will be saved as a BSON int32 or BSON int64 depending
   on its size. A BSON int32 will always decode to a Python int. A BSON
   int64 will always decode to a :class:`~bson.int64.Int64`.
.. [#dt] datetime.datetime instances will be rounded to the nearest
   millisecond when saved
.. [#dt2] all datetime.datetime instances are treated as *naive*. clients
   should always use UTC.
.. [#re] :class:`~bson.regex.Regex` instances and regular expression
   objects from ``re.compile()`` are both saved as BSON regular expressions.
   BSON regular expressions are decoded as :class:`~bson.regex.Regex`
   instances.
.. [#bytes] The bytes type is encoded as BSON binary with
   subtype 0. It will be decoded back to bytes.
"""
from __future__ import annotations

import datetime
import itertools
import os
import re
import struct
import sys
import uuid
from codecs import utf_8_decode as _utf_8_decode
from codecs import utf_8_encode as _utf_8_encode
from collections import abc as _abc
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Generator,
    Iterator,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

from bson.binary import (
    ALL_UUID_SUBTYPES,
    CSHARP_LEGACY,
    JAVA_LEGACY,
    OLD_UUID_SUBTYPE,
    STANDARD,
    UUID_SUBTYPE,
    Binary,
    UuidRepresentation,
)
from bson.code import Code
from bson.codec_options import (
    DEFAULT_CODEC_OPTIONS,
    CodecOptions,
    DatetimeConversion,
    _raw_document_class,
)
from bson.datetime_ms import (
    EPOCH_AWARE,
    EPOCH_NAIVE,
    DatetimeMS,
    _datetime_to_millis,
    _millis_to_datetime,
)
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.errors import InvalidBSON, InvalidDocument, InvalidStringData
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.son import RE_TYPE, SON
from bson.timestamp import Timestamp
from bson.tz_util import utc

# Import some modules for type-checking only.
if TYPE_CHECKING:
    from bson.raw_bson import RawBSONDocument
    from bson.typings import _DocumentType, _ReadableBuffer

try:
    from bson import _cbson  # type: ignore[attr-defined]

    _USE_C = True
except ImportError:
    _USE_C = False

__all__ = [
    "ALL_UUID_SUBTYPES",
    "CSHARP_LEGACY",
    "JAVA_LEGACY",
    "OLD_UUID_SUBTYPE",
    "STANDARD",
    "UUID_SUBTYPE",
    "Binary",
    "UuidRepresentation",
    "Code",
    "DEFAULT_CODEC_OPTIONS",
    "CodecOptions",
    "DBRef",
    "Decimal128",
    "InvalidBSON",
    "InvalidDocument",
    "InvalidStringData",
    "Int64",
    "MaxKey",
    "MinKey",
    "ObjectId",
    "Regex",
    "RE_TYPE",
    "SON",
    "Timestamp",
    "utc",
    "EPOCH_AWARE",
    "EPOCH_NAIVE",
    "BSONNUM",
    "BSONSTR",
    "BSONOBJ",
    "BSONARR",
    "BSONBIN",
    "BSONUND",
    "BSONOID",
    "BSONBOO",
    "BSONDAT",
    "BSONNUL",
    "BSONRGX",
    "BSONREF",
    "BSONCOD",
    "BSONSYM",
    "BSONCWS",
    "BSONINT",
    "BSONTIM",
    "BSONLON",
    "BSONDEC",
    "BSONMIN",
    "BSONMAX",
    "get_data_and_view",
    "gen_list_name",
    "encode",
    "decode",
    "decode_all",
    "decode_iter",
    "decode_file_iter",
    "is_valid",
    "BSON",
    "has_c",
    "DatetimeConversion",
    "DatetimeMS",
]

BSONNUM = b"\x01"  # Floating point
BSONSTR = b"\x02"  # UTF-8 string
BSONOBJ = b"\x03"  # Embedded document
BSONARR = b"\x04"  # Array
BSONBIN = b"\x05"  # Binary
BSONUND = b"\x06"  # Undefined
BSONOID = b"\x07"  # ObjectId
BSONBOO = b"\x08"  # Boolean
BSONDAT = b"\x09"  # UTC Datetime
BSONNUL = b"\x0A"  # Null
BSONRGX = b"\x0B"  # Regex
BSONREF = b"\x0C"  # DBRef
BSONCOD = b"\x0D"  # Javascript code
BSONSYM = b"\x0E"  # Symbol
BSONCWS = b"\x0F"  # Javascript code with scope
BSONINT = b"\x10"  # 32bit int
BSONTIM = b"\x11"  # Timestamp
BSONLON = b"\x12"  # 64bit int
BSONDEC = b"\x13"  # Decimal128
BSONMIN = b"\xFF"  # Min key
BSONMAX = b"\x7F"  # Max key


_UNPACK_FLOAT_FROM = struct.Struct("<d").unpack_from
_UNPACK_INT = struct.Struct("<i").unpack
_UNPACK_INT_FROM = struct.Struct("<i").unpack_from
_UNPACK_LENGTH_SUBTYPE_FROM = struct.Struct("<iB").unpack_from
_UNPACK_LONG_FROM = struct.Struct("<q").unpack_from
_UNPACK_TIMESTAMP_FROM = struct.Struct("<II").unpack_from


def get_data_and_view(data: Any) -> Tuple[Any, memoryview]:
    if isinstance(data, (bytes, bytearray)):
        return data, memoryview(data)
    view = memoryview(data)
    return view.tobytes(), view


def _raise_unknown_type(element_type: int, element_name: str) -> NoReturn:
    """Unknown type helper."""
    raise InvalidBSON(
        "Detected unknown BSON type {!r} for fieldname '{}'. Are "
        "you using the latest driver version?".format(chr(element_type).encode(), element_name)
    )


def _get_int(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[int, int]:
    """Decode a BSON int32 to python int."""
    return _UNPACK_INT_FROM(data, position)[0], position + 4


def _get_c_string(data: Any, view: Any, position: int, opts: CodecOptions[Any]) -> Tuple[str, int]:
    """Decode a BSON 'C' string to python str."""
    end = data.index(b"\x00", position)
    return _utf_8_decode(view[position:end], opts.unicode_decode_error_handler, True)[0], end + 1


def _get_float(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[float, int]:
    """Decode a BSON double to python float."""
    return _UNPACK_FLOAT_FROM(data, position)[0], position + 8


def _get_string(
    data: Any, view: Any, position: int, obj_end: int, opts: CodecOptions[Any], dummy: Any
) -> Tuple[str, int]:
    """Decode a BSON string to python str."""
    length = _UNPACK_INT_FROM(data, position)[0]
    position += 4
    if length < 1 or obj_end - position < length:
        raise InvalidBSON("invalid string length")
    end = position + length - 1
    if data[end] != 0:
        raise InvalidBSON("invalid end of string")
    return _utf_8_decode(view[position:end], opts.unicode_decode_error_handler, True)[0], end + 1


def _get_object_size(data: Any, position: int, obj_end: int) -> Tuple[int, int]:
    """Validate and return a BSON document's size."""
    try:
        obj_size = _UNPACK_INT_FROM(data, position)[0]
    except struct.error as exc:
        raise InvalidBSON(str(exc)) from None
    end = position + obj_size - 1
    if data[end] != 0:
        raise InvalidBSON("bad eoo")
    if end >= obj_end:
        raise InvalidBSON("invalid object length")
    # If this is the top-level document, validate the total size too.
    if position == 0 and obj_size != obj_end:
        raise InvalidBSON("invalid object length")
    return obj_size, end


def _get_object(
    data: Any, view: Any, position: int, obj_end: int, opts: CodecOptions[Any], dummy: Any
) -> Tuple[Any, int]:
    """Decode a BSON subdocument to opts.document_class or bson.dbref.DBRef."""
    obj_size, end = _get_object_size(data, position, obj_end)
    if _raw_document_class(opts.document_class):
        return (opts.document_class(data[position : end + 1], opts), position + obj_size)

    obj = _elements_to_dict(data, view, position + 4, end, opts)

    position += obj_size
    # If DBRef validation fails, return a normal doc.
    if (
        isinstance(obj.get("$ref"), str)
        and "$id" in obj
        and isinstance(obj.get("$db"), (str, type(None)))
    ):
        return (DBRef(obj.pop("$ref"), obj.pop("$id", None), obj.pop("$db", None), obj), position)
    return obj, position


def _get_array(
    data: Any, view: Any, position: int, obj_end: int, opts: CodecOptions[Any], element_name: str
) -> Tuple[Any, int]:
    """Decode a BSON array to python list."""
    size = _UNPACK_INT_FROM(data, position)[0]
    end = position + size - 1
    if data[end] != 0:
        raise InvalidBSON("bad eoo")

    position += 4
    end -= 1
    result: list[Any] = []

    # Avoid doing global and attribute lookups in the loop.
    append = result.append
    index = data.index
    getter = _ELEMENT_GETTER
    decoder_map = opts.type_registry._decoder_map

    while position < end:
        element_type = data[position]
        # Just skip the keys.
        position = index(b"\x00", position) + 1
        try:
            value, position = getter[element_type](
                data, view, position, obj_end, opts, element_name
            )
        except KeyError:
            _raise_unknown_type(element_type, element_name)

        if decoder_map:
            custom_decoder = decoder_map.get(type(value))
            if custom_decoder is not None:
                value = custom_decoder(value)

        append(value)

    if position != end + 1:
        raise InvalidBSON("bad array length")
    return result, position + 1


def _get_binary(
    data: Any, _view: Any, position: int, obj_end: int, opts: CodecOptions[Any], dummy1: Any
) -> Tuple[Union[Binary, uuid.UUID], int]:
    """Decode a BSON binary to bson.binary.Binary or python UUID."""
    length, subtype = _UNPACK_LENGTH_SUBTYPE_FROM(data, position)
    position += 5
    if subtype == 2:
        length2 = _UNPACK_INT_FROM(data, position)[0]
        position += 4
        if length2 != length - 4:
            raise InvalidBSON("invalid binary (st 2) - lengths don't match!")
        length = length2
    end = position + length
    if length < 0 or end > obj_end:
        raise InvalidBSON("bad binary object length")

    # Convert UUID subtypes to native UUIDs.
    if subtype in ALL_UUID_SUBTYPES:
        uuid_rep = opts.uuid_representation
        binary_value = Binary(data[position:end], subtype)
        if (
            (uuid_rep == UuidRepresentation.UNSPECIFIED)
            or (subtype == UUID_SUBTYPE and uuid_rep != STANDARD)
            or (subtype == OLD_UUID_SUBTYPE and uuid_rep == STANDARD)
        ):
            return binary_value, end
        return binary_value.as_uuid(uuid_rep), end

    # Decode subtype 0 to 'bytes'.
    if subtype == 0:
        value = data[position:end]
    else:
        value = Binary(data[position:end], subtype)

    return value, end


def _get_oid(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[ObjectId, int]:
    """Decode a BSON ObjectId to bson.objectid.ObjectId."""
    end = position + 12
    return ObjectId(data[position:end]), end


def _get_boolean(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[bool, int]:
    """Decode a BSON true/false to python True/False."""
    end = position + 1
    boolean_byte = data[position:end]
    if boolean_byte == b"\x00":
        return False, end
    elif boolean_byte == b"\x01":
        return True, end
    raise InvalidBSON("invalid boolean value: %r" % boolean_byte)


def _get_date(
    data: Any, _view: Any, position: int, dummy0: int, opts: CodecOptions[Any], dummy1: Any
) -> Tuple[Union[datetime.datetime, DatetimeMS], int]:
    """Decode a BSON datetime to python datetime.datetime."""
    return _millis_to_datetime(_UNPACK_LONG_FROM(data, position)[0], opts), position + 8


def _get_code(
    data: Any, view: Any, position: int, obj_end: int, opts: CodecOptions[Any], element_name: str
) -> Tuple[Code, int]:
    """Decode a BSON code to bson.code.Code."""
    code, position = _get_string(data, view, position, obj_end, opts, element_name)
    return Code(code), position


def _get_code_w_scope(
    data: Any, view: Any, position: int, _obj_end: int, opts: CodecOptions[Any], element_name: str
) -> Tuple[Code, int]:
    """Decode a BSON code_w_scope to bson.code.Code."""
    code_end = position + _UNPACK_INT_FROM(data, position)[0]
    code, position = _get_string(data, view, position + 4, code_end, opts, element_name)
    scope, position = _get_object(data, view, position, code_end, opts, element_name)
    if position != code_end:
        raise InvalidBSON("scope outside of javascript code boundaries")
    return Code(code, scope), position


def _get_regex(
    data: Any, view: Any, position: int, dummy0: Any, opts: CodecOptions[Any], dummy1: Any
) -> Tuple[Regex[Any], int]:
    """Decode a BSON regex to bson.regex.Regex or a python pattern object."""
    pattern, position = _get_c_string(data, view, position, opts)
    bson_flags, position = _get_c_string(data, view, position, opts)
    bson_re = Regex(pattern, bson_flags)
    return bson_re, position


def _get_ref(
    data: Any, view: Any, position: int, obj_end: int, opts: CodecOptions[Any], element_name: str
) -> Tuple[DBRef, int]:
    """Decode (deprecated) BSON DBPointer to bson.dbref.DBRef."""
    collection, position = _get_string(data, view, position, obj_end, opts, element_name)
    oid, position = _get_oid(data, view, position, obj_end, opts, element_name)
    return DBRef(collection, oid), position


def _get_timestamp(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[Timestamp, int]:
    """Decode a BSON timestamp to bson.timestamp.Timestamp."""
    inc, timestamp = _UNPACK_TIMESTAMP_FROM(data, position)
    return Timestamp(timestamp, inc), position + 8


def _get_int64(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[Int64, int]:
    """Decode a BSON int64 to bson.int64.Int64."""
    return Int64(_UNPACK_LONG_FROM(data, position)[0]), position + 8


def _get_decimal128(
    data: Any, _view: Any, position: int, dummy0: Any, dummy1: Any, dummy2: Any
) -> Tuple[Decimal128, int]:
    """Decode a BSON decimal128 to bson.decimal128.Decimal128."""
    end = position + 16
    return Decimal128.from_bid(data[position:end]), end


# Each decoder function's signature is:
#   - data: bytes
#   - view: memoryview that references `data`
#   - position: int, beginning of object in 'data' to decode
#   - obj_end: int, end of object to decode in 'data' if variable-length type
#   - opts: a CodecOptions
_ELEMENT_GETTER: dict[int, Callable[..., Tuple[Any, int]]] = {
    ord(BSONNUM): _get_float,
    ord(BSONSTR): _get_string,
    ord(BSONOBJ): _get_object,
    ord(BSONARR): _get_array,
    ord(BSONBIN): _get_binary,
    ord(BSONUND): lambda u, v, w, x, y, z: (None, w),  # noqa: ARG005 # Deprecated undefined
    ord(BSONOID): _get_oid,
    ord(BSONBOO): _get_boolean,
    ord(BSONDAT): _get_date,
    ord(BSONNUL): lambda u, v, w, x, y, z: (None, w),  # noqa: ARG005
    ord(BSONRGX): _get_regex,
    ord(BSONREF): _get_ref,  # Deprecated DBPointer
    ord(BSONCOD): _get_code,
    ord(BSONSYM): _get_string,  # Deprecated symbol
    ord(BSONCWS): _get_code_w_scope,
    ord(BSONINT): _get_int,
    ord(BSONTIM): _get_timestamp,
    ord(BSONLON): _get_int64,
    ord(BSONDEC): _get_decimal128,
    ord(BSONMIN): lambda u, v, w, x, y, z: (MinKey(), w),  # noqa: ARG005
    ord(BSONMAX): lambda u, v, w, x, y, z: (MaxKey(), w),  # noqa: ARG005
}


if _USE_C:

    def _element_to_dict(
        data: Any,
        view: Any,  # noqa: ARG001
        position: int,
        obj_end: int,
        opts: CodecOptions[Any],
        raw_array: bool = False,
    ) -> Tuple[str, Any, int]:
        return cast(
            "Tuple[str, Any, int]",
            _cbson._element_to_dict(data, position, obj_end, opts, raw_array),
        )

else:

    def _element_to_dict(
        data: Any,
        view: Any,
        position: int,
        obj_end: int,
        opts: CodecOptions[Any],
        raw_array: bool = False,
    ) -> Tuple[str, Any, int]:
        """Decode a single key, value pair."""
        element_type = data[position]
        position += 1
        element_name, position = _get_c_string(data, view, position, opts)
        if raw_array and element_type == ord(BSONARR):
            _, end = _get_object_size(data, position, len(data))
            return element_name, view[position : end + 1], end + 1
        try:
            value, position = _ELEMENT_GETTER[element_type](
                data, view, position, obj_end, opts, element_name
            )
        except KeyError:
            _raise_unknown_type(element_type, element_name)

        if opts.type_registry._decoder_map:
            custom_decoder = opts.type_registry._decoder_map.get(type(value))
            if custom_decoder is not None:
                value = custom_decoder(value)

        return element_name, value, position


_T = TypeVar("_T", bound=MutableMapping[str, Any])


def _raw_to_dict(
    data: Any,
    position: int,
    obj_end: int,
    opts: CodecOptions[RawBSONDocument],
    result: _T,
    raw_array: bool = False,
) -> _T:
    data, view = get_data_and_view(data)
    return cast(
        _T, _elements_to_dict(data, view, position, obj_end, opts, result, raw_array=raw_array)
    )


def _elements_to_dict(
    data: Any,
    view: Any,
    position: int,
    obj_end: int,
    opts: CodecOptions[Any],
    result: Any = None,
    raw_array: bool = False,
) -> Any:
    """Decode a BSON document into result."""
    if result is None:
        result = opts.document_class()
    end = obj_end - 1
    while position < end:
        key, value, position = _element_to_dict(
            data, view, position, obj_end, opts, raw_array=raw_array
        )
        result[key] = value
    if position != obj_end:
        raise InvalidBSON("bad object or element length")
    return result


def _bson_to_dict(data: Any, opts: CodecOptions[_DocumentType]) -> _DocumentType:
    """Decode a BSON string to document_class."""
    data, view = get_data_and_view(data)
    try:
        if _raw_document_class(opts.document_class):
            return opts.document_class(data, opts)  # type:ignore[call-arg]
        _, end = _get_object_size(data, 0, len(data))
        return cast("_DocumentType", _elements_to_dict(data, view, 4, end, opts))
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        raise InvalidBSON(str(exc_value)).with_traceback(exc_tb) from None


if _USE_C:
    _bson_to_dict = _cbson._bson_to_dict


_PACK_FLOAT = struct.Struct("<d").pack
_PACK_INT = struct.Struct("<i").pack
_PACK_LENGTH_SUBTYPE = struct.Struct("<iB").pack
_PACK_LONG = struct.Struct("<q").pack
_PACK_TIMESTAMP = struct.Struct("<II").pack
_LIST_NAMES = tuple((str(i) + "\x00").encode("utf8") for i in range(1000))


def gen_list_name() -> Generator[bytes, None, None]:
    """Generate "keys" for encoded lists in the sequence
    b"0\x00", b"1\x00", b"2\x00", ...

    The first 1000 keys are returned from a pre-built cache. All
    subsequent keys are generated on the fly.
    """
    yield from _LIST_NAMES

    counter = itertools.count(1000)
    while True:
        yield (str(next(counter)) + "\x00").encode("utf8")


def _make_c_string_check(string: Union[str, bytes]) -> bytes:
    """Make a 'C' string, checking for embedded NUL characters."""
    if isinstance(string, bytes):
        if b"\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not contain a NUL character")
        try:
            _utf_8_decode(string, None, True)
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData(
                "strings in documents must be valid UTF-8: %r" % string
            ) from None
    else:
        if "\x00" in string:
            raise InvalidDocument("BSON keys / regex patterns must not contain a NUL character")
        return _utf_8_encode(string)[0] + b"\x00"


def _make_c_string(string: Union[str, bytes]) -> bytes:
    """Make a 'C' string."""
    if isinstance(string, bytes):
        try:
            _utf_8_decode(string, None, True)
            return string + b"\x00"
        except UnicodeError:
            raise InvalidStringData(
                "strings in documents must be valid UTF-8: %r" % string
            ) from None
    else:
        return _utf_8_encode(string)[0] + b"\x00"


def _make_name(string: str) -> bytes:
    """Make a 'C' string suitable for a BSON key."""
    if "\x00" in string:
        raise InvalidDocument("BSON keys must not contain a NUL character")
    return _utf_8_encode(string)[0] + b"\x00"


def _encode_float(name: bytes, value: float, dummy0: Any, dummy1: Any) -> bytes:
    """Encode a float."""
    return b"\x01" + name + _PACK_FLOAT(value)


def _encode_bytes(name: bytes, value: bytes, dummy0: Any, dummy1: Any) -> bytes:
    """Encode a python bytes."""
    # Python3 special case. Store 'bytes' as BSON binary subtype 0.
    return b"\x05" + name + _PACK_INT(len(value)) + b"\x00" + value


def _encode_mapping(name: bytes, value: Any, check_keys: bool, opts: CodecOptions[Any]) -> bytes:
    """Encode a mapping type."""
    if _raw_document_class(value):
        return b"\x03" + name + cast(bytes, value.raw)
    data = b"".join([_element_to_bson(key, val, check_keys, opts) for key, val in value.items()])
    return b"\x03" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_dbref(name: bytes, value: DBRef, check_keys: bool, opts: CodecOptions[Any]) -> bytes:
    """Encode bson.dbref.DBRef."""
    buf = bytearray(b"\x03" + name + b"\x00\x00\x00\x00")
    begin = len(buf) - 4

    buf += _name_value_to_bson(b"$ref\x00", value.collection, check_keys, opts)
    buf += _name_value_to_bson(b"$id\x00", value.id, check_keys, opts)
    if value.database is not None:
        buf += _name_value_to_bson(b"$db\x00", value.database, check_keys, opts)
    for key, val in value._DBRef__kwargs.items():
        buf += _element_to_bson(key, val, check_keys, opts)

    buf += b"\x00"
    buf[begin : begin + 4] = _PACK_INT(len(buf) - begin)
    return bytes(buf)


def _encode_list(
    name: bytes, value: Sequence[Any], check_keys: bool, opts: CodecOptions[Any]
) -> bytes:
    """Encode a list/tuple."""
    lname = gen_list_name()
    data = b"".join([_name_value_to_bson(next(lname), item, check_keys, opts) for item in value])
    return b"\x04" + name + _PACK_INT(len(data) + 5) + data + b"\x00"


def _encode_text(name: bytes, value: str, dummy0: Any, dummy1: Any) -> bytes:
    """Encode a python str."""
    bvalue = _utf_8_encode(value)[0]
    return b"\x02" + name + _PACK_INT(len(bvalue) + 1) + bvalue + b"\x00"


def _encode_binary(name: bytes, value: Binary, dummy0: Any, dummy1: Any) -> bytes:
    """Encode bson.binary.Binary."""
    subtype = value.subtype
    if subtype == 2:
        value = _PACK_INT(len(value)) + value  # type: ignore
    return b"\x05" + name + _PACK_LENGTH_SUBTYPE(len(value), subtype) + value


def _encode_uuid(name: bytes, value: uuid.UUID, dummy: Any, opts: CodecOptions[Any]) -> bytes:
    """Encode uuid.UUID."""
    uuid_representation = opts.uuid_representation
    binval = Binary.from_uuid(value, uuid_representation=uuid_representation)
    return _encode_binary(name, binval, dummy, opts)


def _encode_objectid(name: bytes, value: ObjectId, dummy: Any, dummy1: Any) -> bytes:
    """Encode bson.objectid.ObjectId."""
    return b"\x07" + name + value.binary


def _encode_bool(name: bytes, value: bool, dummy0: Any, dummy1: Any) -> bytes:
    """Encode a python boolean (True/False)."""
    return b"\x08" + name + (value and b"\x01" or b"\x00")


def _encode_datetime(name: bytes, value: datetime.datetime, dummy0: Any, dummy1: Any) -> bytes:
    """Encode datetime.datetime."""
    millis = _datetime_to_millis(value)
    return b"\x09" + name + _PACK_LONG(millis)


def _encode_datetime_ms(name: bytes, value: DatetimeMS, dummy0: Any, dummy1: Any) -> bytes:
    """Encode datetime.datetime."""
    millis = int(value)
    return b"\x09" + name + _PACK_LONG(millis)


def _encode_none(name: bytes, dummy0: Any, dummy1: Any, dummy2: Any) -> bytes:
    """Encode python None."""
    return b"\x0A" + name


def _encode_regex(name: bytes, value: Regex[Any], dummy0: Any, dummy1: Any) -> bytes:
    """Encode a python regex or bson.regex.Regex."""
    flags = value.flags
    # Python 3 common case
    if flags == re.UNICODE:
        return b"\x0B" + name + _make_c_string_check(value.pattern) + b"u\x00"
    elif flags == 0:
        return b"\x0B" + name + _make_c_string_check(value.pattern) + b"\x00"
    else:
        sflags = b""
        if flags & re.IGNORECASE:
            sflags += b"i"
        if flags & re.LOCALE:
            sflags += b"l"
        if flags & re.MULTILINE:
            sflags += b"m"
        if flags & re.DOTALL:
            sflags += b"s"
        if flags & re.UNICODE:
            sflags += b"u"
        if flags & re.VERBOSE:
            sflags += b"x"
        sflags += b"\x00"
        return b"\x0B" + name + _make_c_string_check(value.pattern) + sflags


def _encode_code(name: bytes, value: Code, dummy: Any, opts: CodecOptions[Any]) -> bytes:
    """Encode bson.code.Code."""
    cstring = _make_c_string(value)
    cstrlen = len(cstring)
    if value.scope is None:
        return b"\x0D" + name + _PACK_INT(cstrlen) + cstring
    scope = _dict_to_bson(value.scope, False, opts, False)
    full_length = _PACK_INT(8 + cstrlen + len(scope))
    return b"\x0F" + name + full_length + _PACK_INT(cstrlen) + cstring + scope


def _encode_int(name: bytes, value: int, dummy0: Any, dummy1: Any) -> bytes:
    """Encode a python int."""
    if -2147483648 <= value <= 2147483647:
        return b"\x10" + name + _PACK_INT(value)
    else:
        try:
            return b"\x12" + name + _PACK_LONG(value)
        except struct.error:
            raise OverflowError("BSON can only handle up to 8-byte ints") from None


def _encode_timestamp(name: bytes, value: Any, dummy0: Any, dummy1: Any) -> bytes:
    """Encode bson.timestamp.Timestamp."""
    return b"\x11" + name + _PACK_TIMESTAMP(value.inc, value.time)


def _encode_long(name: bytes, value: Any, dummy0: Any, dummy1: Any) -> bytes:
    """Encode a bson.int64.Int64."""
    try:
        return b"\x12" + name + _PACK_LONG(value)
    except struct.error:
        raise OverflowError("BSON can only handle up to 8-byte ints") from None


def _encode_decimal128(name: bytes, value: Decimal128, dummy0: Any, dummy1: Any) -> bytes:
    """Encode bson.decimal128.Decimal128."""
    return b"\x13" + name + value.bid


def _encode_minkey(name: bytes, dummy0: Any, dummy1: Any, dummy2: Any) -> bytes:
    """Encode bson.min_key.MinKey."""
    return b"\xFF" + name


def _encode_maxkey(name: bytes, dummy0: Any, dummy1: Any, dummy2: Any) -> bytes:
    """Encode bson.max_key.MaxKey."""
    return b"\x7F" + name


# Each encoder function's signature is:
#   - name: utf-8 bytes
#   - value: a Python data type, e.g. a Python int for _encode_int
#   - check_keys: bool, whether to check for invalid names
#   - opts: a CodecOptions
_ENCODERS = {
    bool: _encode_bool,
    bytes: _encode_bytes,
    datetime.datetime: _encode_datetime,
    DatetimeMS: _encode_datetime_ms,
    dict: _encode_mapping,
    float: _encode_float,
    int: _encode_int,
    list: _encode_list,
    str: _encode_text,
    tuple: _encode_list,
    type(None): _encode_none,
    uuid.UUID: _encode_uuid,
    Binary: _encode_binary,
    Int64: _encode_long,
    Code: _encode_code,
    DBRef: _encode_dbref,
    MaxKey: _encode_maxkey,
    MinKey: _encode_minkey,
    ObjectId: _encode_objectid,
    Regex: _encode_regex,
    RE_TYPE: _encode_regex,
    SON: _encode_mapping,
    Timestamp: _encode_timestamp,
    Decimal128: _encode_decimal128,
    # Special case. This will never be looked up directly.
    _abc.Mapping: _encode_mapping,
}

# Map each _type_marker to its encoder for faster lookup.
_MARKERS = {}
for _typ in _ENCODERS:
    if hasattr(_typ, "_type_marker"):
        _MARKERS[_typ._type_marker] = _ENCODERS[_typ]


_BUILT_IN_TYPES = tuple(t for t in _ENCODERS)


def _name_value_to_bson(
    name: bytes,
    value: Any,
    check_keys: bool,
    opts: CodecOptions[Any],
    in_custom_call: bool = False,
    in_fallback_call: bool = False,
) -> bytes:
    """Encode a single name, value pair."""

    was_integer_overflow = False

    # First see if the type is already cached. KeyError will only ever
    # happen once per subtype.
    try:
        return _ENCODERS[type(value)](name, value, check_keys, opts)  # type: ignore
    except KeyError:
        pass
    except OverflowError:
        if not isinstance(value, int):
            raise

        # Give the fallback_encoder a chance
        was_integer_overflow = True

    # Second, fall back to trying _type_marker. This has to be done
    # before the loop below since users could subclass one of our
    # custom types that subclasses a python built-in (e.g. Binary)
    marker = getattr(value, "_type_marker", None)
    if isinstance(marker, int) and marker in _MARKERS:
        func = _MARKERS[marker]
        # Cache this type for faster subsequent lookup.
        _ENCODERS[type(value)] = func
        return func(name, value, check_keys, opts)  # type: ignore

    # Third, check if a type encoder is registered for this type.
    # Note that subtypes of registered custom types are not auto-encoded.
    if not in_custom_call and opts.type_registry._encoder_map:
        custom_encoder = opts.type_registry._encoder_map.get(type(value))
        if custom_encoder is not None:
            return _name_value_to_bson(
                name, custom_encoder(value), check_keys, opts, in_custom_call=True
            )

    # Fourth, test each base type. This will only happen once for
    # a subtype of a supported base type. Unlike in the C-extensions, this
    # is done after trying the custom type encoder because checking for each
    # subtype is expensive.
    for base in _BUILT_IN_TYPES:
        if not was_integer_overflow and isinstance(value, base):
            func = _ENCODERS[base]
            # Cache this type for faster subsequent lookup.
            _ENCODERS[type(value)] = func
            return func(name, value, check_keys, opts)  # type: ignore

    # As a last resort, try using the fallback encoder, if the user has
    # provided one.
    fallback_encoder = opts.type_registry._fallback_encoder
    if not in_fallback_call and fallback_encoder is not None:
        return _name_value_to_bson(
            name, fallback_encoder(value), check_keys, opts, in_fallback_call=True
        )

    if was_integer_overflow:
        raise OverflowError("BSON can only handle up to 8-byte ints")
    raise InvalidDocument(f"cannot encode object: {value!r}, of type: {type(value)!r}")


def _element_to_bson(key: Any, value: Any, check_keys: bool, opts: CodecOptions[Any]) -> bytes:
    """Encode a single key, value pair."""
    if not isinstance(key, str):
        raise InvalidDocument(f"documents must have only string keys, key was {key!r}")
    if check_keys:
        if key.startswith("$"):
            raise InvalidDocument(f"key {key!r} must not start with '$'")
        if "." in key:
            raise InvalidDocument(f"key {key!r} must not contain '.'")

    name = _make_name(key)
    return _name_value_to_bson(name, value, check_keys, opts)


def _dict_to_bson(
    doc: Any, check_keys: bool, opts: CodecOptions[Any], top_level: bool = True
) -> bytes:
    """Encode a document to BSON."""
    if _raw_document_class(doc):
        return cast(bytes, doc.raw)
    try:
        elements = []
        if top_level and "_id" in doc:
            elements.append(_name_value_to_bson(b"_id\x00", doc["_id"], check_keys, opts))
        for key, value in doc.items():
            if not top_level or key != "_id":
                elements.append(_element_to_bson(key, value, check_keys, opts))
    except AttributeError:
        raise TypeError(f"encoder expected a mapping type but got: {doc!r}") from None

    encoded = b"".join(elements)
    return _PACK_INT(len(encoded) + 5) + encoded + b"\x00"


if _USE_C:
    _dict_to_bson = _cbson._dict_to_bson


_CODEC_OPTIONS_TYPE_ERROR = TypeError("codec_options must be an instance of CodecOptions")


def encode(
    document: Mapping[str, Any],
    check_keys: bool = False,
    codec_options: CodecOptions[Any] = DEFAULT_CODEC_OPTIONS,
) -> bytes:
    """Encode a document to BSON.

    A document can be any mapping type (like :class:`dict`).

    Raises :class:`TypeError` if `document` is not a mapping type,
    or contains keys that are not instances of :class:`str`. Raises
    :class:`~bson.errors.InvalidDocument` if `document` cannot be
    converted to :class:`BSON`.

    :param document: mapping type representing a document
    :param check_keys: check if keys start with '$' or
        contain '.', raising :class:`~bson.errors.InvalidDocument` in
        either case
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionadded:: 3.9
    """
    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    return _dict_to_bson(document, check_keys, codec_options)


@overload
def decode(data: _ReadableBuffer, codec_options: None = None) -> dict[str, Any]:
    ...


@overload
def decode(data: _ReadableBuffer, codec_options: CodecOptions[_DocumentType]) -> _DocumentType:
    ...


def decode(
    data: _ReadableBuffer, codec_options: Optional[CodecOptions[_DocumentType]] = None
) -> Union[dict[str, Any], _DocumentType]:
    """Decode BSON to a document.

    By default, returns a BSON document represented as a Python
    :class:`dict`. To use a different :class:`MutableMapping` class,
    configure a :class:`~bson.codec_options.CodecOptions`::

        >>> import collections  # From Python standard library.
        >>> import bson
        >>> from bson.codec_options import CodecOptions
        >>> data = bson.encode({'a': 1})
        >>> decoded_doc = bson.decode(data)
        <type 'dict'>
        >>> options = CodecOptions(document_class=collections.OrderedDict)
        >>> decoded_doc = bson.decode(data, codec_options=options)
        >>> type(decoded_doc)
        <class 'collections.OrderedDict'>

    :param data: the BSON to decode. Any bytes-like object that implements
        the buffer protocol.
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionadded:: 3.9
    """
    opts: CodecOptions[Any] = codec_options or DEFAULT_CODEC_OPTIONS
    if not isinstance(opts, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    return cast("Union[dict[str, Any], _DocumentType]", _bson_to_dict(data, opts))


def _decode_all(data: _ReadableBuffer, opts: CodecOptions[_DocumentType]) -> list[_DocumentType]:
    """Decode a BSON data to multiple documents."""
    data, view = get_data_and_view(data)
    data_len = len(data)
    docs: list[_DocumentType] = []
    position = 0
    end = data_len - 1
    use_raw = _raw_document_class(opts.document_class)
    try:
        while position < end:
            obj_size = _UNPACK_INT_FROM(data, position)[0]
            if data_len - position < obj_size:
                raise InvalidBSON("invalid object size")
            obj_end = position + obj_size - 1
            if data[obj_end] != 0:
                raise InvalidBSON("bad eoo")
            if use_raw:
                docs.append(opts.document_class(data[position : obj_end + 1], opts))  # type: ignore
            else:
                docs.append(_elements_to_dict(data, view, position + 4, obj_end, opts))
            position += obj_size
        return docs
    except InvalidBSON:
        raise
    except Exception:
        # Change exception type to InvalidBSON but preserve traceback.
        _, exc_value, exc_tb = sys.exc_info()
        raise InvalidBSON(str(exc_value)).with_traceback(exc_tb) from None


if _USE_C:
    _decode_all = _cbson._decode_all


@overload
def decode_all(data: _ReadableBuffer, codec_options: None = None) -> list[dict[str, Any]]:
    ...


@overload
def decode_all(
    data: _ReadableBuffer, codec_options: CodecOptions[_DocumentType]
) -> list[_DocumentType]:
    ...


def decode_all(
    data: _ReadableBuffer, codec_options: Optional[CodecOptions[_DocumentType]] = None
) -> Union[list[dict[str, Any]], list[_DocumentType]]:
    """Decode BSON data to multiple documents.

    `data` must be a bytes-like object implementing the buffer protocol that
    provides concatenated, valid, BSON-encoded documents.

    :param data: BSON data
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.9
       Supports bytes-like objects that implement the buffer protocol.

    .. versionchanged:: 3.0
       Removed `compile_re` option: PyMongo now always represents BSON regular
       expressions as :class:`~bson.regex.Regex` objects. Use
       :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
       BSON regular expression to a Python regular expression object.

       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.
    """
    if codec_options is None:
        return _decode_all(data, DEFAULT_CODEC_OPTIONS)

    if not isinstance(codec_options, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    return _decode_all(data, codec_options)


def _decode_selective(
    rawdoc: Any, fields: Any, codec_options: CodecOptions[_DocumentType]
) -> _DocumentType:
    if _raw_document_class(codec_options.document_class):
        # If document_class is RawBSONDocument, use vanilla dictionary for
        # decoding command response.
        doc: _DocumentType = {}  # type:ignore[assignment]
    else:
        # Else, use the specified document_class.
        doc = codec_options.document_class()
    for key, value in rawdoc.items():
        if key in fields:
            if fields[key] == 1:
                doc[key] = _bson_to_dict(rawdoc.raw, codec_options)[key]  # type:ignore[index]
            else:
                doc[key] = _decode_selective(  # type:ignore[index]
                    value, fields[key], codec_options
                )
        else:
            doc[key] = value  # type:ignore[index]
    return doc


def _array_of_documents_to_buffer(view: memoryview) -> bytes:
    # Extract the raw bytes of each document.
    position = 0
    _, end = _get_object_size(view, position, len(view))
    position += 4
    buffers: list[memoryview] = []
    append = buffers.append
    while position < end - 1:
        # Just skip the keys.
        while view[position] != 0:
            position += 1
        position += 1
        obj_size, _ = _get_object_size(view, position, end)
        append(view[position : position + obj_size])
        position += obj_size
    if position != end:
        raise InvalidBSON("bad object or element length")
    return b"".join(buffers)


if _USE_C:
    _array_of_documents_to_buffer = _cbson._array_of_documents_to_buffer


def _convert_raw_document_lists_to_streams(document: Any) -> None:
    """Convert raw array of documents to a stream of BSON documents."""
    cursor = document.get("cursor")
    if not cursor:
        return
    for key in ("firstBatch", "nextBatch"):
        batch = cursor.get(key)
        if not batch:
            continue
        data = _array_of_documents_to_buffer(batch)
        if data:
            cursor[key] = [data]
        else:
            cursor[key] = []


def _decode_all_selective(
    data: Any, codec_options: CodecOptions[_DocumentType], fields: Any
) -> list[_DocumentType]:
    """Decode BSON data to a single document while using user-provided
    custom decoding logic.

    `data` must be a string representing a valid, BSON-encoded document.

    :param data: BSON data
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions` with user-specified type
        decoders. If no decoders are found, this method is the same as
        ``decode_all``.
    :param fields: Map of document namespaces where data that needs
        to be custom decoded lives or None. For example, to custom decode a
        list of objects in 'field1.subfield1', the specified value should be
        ``{'field1': {'subfield1': 1}}``. If ``fields``  is an empty map or
        None, this method is the same as ``decode_all``.

    :return: Single-member list containing the decoded document.

    .. versionadded:: 3.8
    """
    if not codec_options.type_registry._decoder_map:
        return decode_all(data, codec_options)

    if not fields:
        return decode_all(data, codec_options.with_options(type_registry=None))

    # Decode documents for internal use.
    from bson.raw_bson import RawBSONDocument

    internal_codec_options: CodecOptions[RawBSONDocument] = codec_options.with_options(
        document_class=RawBSONDocument, type_registry=None
    )
    _doc = _bson_to_dict(data, internal_codec_options)
    return [
        _decode_selective(
            _doc,
            fields,
            codec_options,
        )
    ]


@overload
def decode_iter(data: bytes, codec_options: None = None) -> Iterator[dict[str, Any]]:
    ...


@overload
def decode_iter(data: bytes, codec_options: CodecOptions[_DocumentType]) -> Iterator[_DocumentType]:
    ...


def decode_iter(
    data: bytes, codec_options: Optional[CodecOptions[_DocumentType]] = None
) -> Union[Iterator[dict[str, Any]], Iterator[_DocumentType]]:
    """Decode BSON data to multiple documents as a generator.

    Works similarly to the decode_all function, but yields one document at a
    time.

    `data` must be a string of concatenated, valid, BSON-encoded
    documents.

    :param data: BSON data
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionadded:: 2.8
    """
    opts = codec_options or DEFAULT_CODEC_OPTIONS
    if not isinstance(opts, CodecOptions):
        raise _CODEC_OPTIONS_TYPE_ERROR

    position = 0
    end = len(data) - 1
    while position < end:
        obj_size = _UNPACK_INT_FROM(data, position)[0]
        elements = data[position : position + obj_size]
        position += obj_size

        yield _bson_to_dict(elements, opts)  # type:ignore[misc, type-var]


@overload
def decode_file_iter(
    file_obj: Union[BinaryIO, IO[bytes]], codec_options: None = None
) -> Iterator[dict[str, Any]]:
    ...


@overload
def decode_file_iter(
    file_obj: Union[BinaryIO, IO[bytes]], codec_options: CodecOptions[_DocumentType]
) -> Iterator[_DocumentType]:
    ...


def decode_file_iter(
    file_obj: Union[BinaryIO, IO[bytes]],
    codec_options: Optional[CodecOptions[_DocumentType]] = None,
) -> Union[Iterator[dict[str, Any]], Iterator[_DocumentType]]:
    """Decode bson data from a file to multiple documents as a generator.

    Works similarly to the decode_all function, but reads from the file object
    in chunks and parses bson in chunks, yielding one document at a time.

    :param file_obj: A file object containing BSON data.
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions`.

    .. versionchanged:: 3.0
       Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
       `codec_options`.

    .. versionadded:: 2.8
    """
    opts = codec_options or DEFAULT_CODEC_OPTIONS
    while True:
        # Read size of next object.
        size_data: Any = file_obj.read(4)
        if not size_data:
            break  # Finished with file normally.
        elif len(size_data) != 4:
            raise InvalidBSON("cut off in middle of objsize")
        obj_size = _UNPACK_INT_FROM(size_data, 0)[0] - 4
        elements = size_data + file_obj.read(max(0, obj_size))
        yield _bson_to_dict(elements, opts)  # type:ignore[type-var, arg-type, misc]


def is_valid(bson: bytes) -> bool:
    """Check that the given string represents valid :class:`BSON` data.

    Raises :class:`TypeError` if `bson` is not an instance of
    :class:`bytes`. Returns ``True``
    if `bson` is valid :class:`BSON`, ``False`` otherwise.

    :param bson: the data to be validated
    """
    if not isinstance(bson, bytes):
        raise TypeError("BSON data must be an instance of a subclass of bytes")

    try:
        _bson_to_dict(bson, DEFAULT_CODEC_OPTIONS)
        return True
    except Exception:
        return False


class BSON(bytes):
    """BSON (Binary JSON) data.

    .. warning:: Using this class to encode and decode BSON adds a performance
       cost. For better performance use the module level functions
       :func:`encode` and :func:`decode` instead.
    """

    @classmethod
    def encode(
        cls: Type[BSON],
        document: Mapping[str, Any],
        check_keys: bool = False,
        codec_options: CodecOptions[Any] = DEFAULT_CODEC_OPTIONS,
    ) -> BSON:
        """Encode a document to a new :class:`BSON` instance.

        A document can be any mapping type (like :class:`dict`).

        Raises :class:`TypeError` if `document` is not a mapping type,
        or contains keys that are not instances of
        :class:`str'. Raises :class:`~bson.errors.InvalidDocument`
        if `document` cannot be converted to :class:`BSON`.

        :param document: mapping type representing a document
        :param check_keys: check if keys start with '$' or
            contain '.', raising :class:`~bson.errors.InvalidDocument` in
            either case
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Replaced `uuid_subtype` option with `codec_options`.
        """
        return cls(encode(document, check_keys, codec_options))

    def decode(  # type:ignore[override]
        self, codec_options: CodecOptions[Any] = DEFAULT_CODEC_OPTIONS
    ) -> dict[str, Any]:
        """Decode this BSON data.

        By default, returns a BSON document represented as a Python
        :class:`dict`. To use a different :class:`MutableMapping` class,
        configure a :class:`~bson.codec_options.CodecOptions`::

            >>> import collections  # From Python standard library.
            >>> import bson
            >>> from bson.codec_options import CodecOptions
            >>> data = bson.BSON.encode({'a': 1})
            >>> decoded_doc = bson.BSON(data).decode()
            <type 'dict'>
            >>> options = CodecOptions(document_class=collections.OrderedDict)
            >>> decoded_doc = bson.BSON(data).decode(codec_options=options)
            >>> type(decoded_doc)
            <class 'collections.OrderedDict'>

        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`.

        .. versionchanged:: 3.0
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.

           Replaced `as_class`, `tz_aware`, and `uuid_subtype` options with
           `codec_options`.
        """
        return decode(self, codec_options)


def has_c() -> bool:
    """Is the C extension installed?"""
    return _USE_C


def _after_fork() -> None:
    """Releases the ObjectID lock child."""
    if ObjectId._inc_lock.locked():
        ObjectId._inc_lock.release()


if hasattr(os, "register_at_fork"):
    # This will run in the same thread as the fork was called.
    # If we fork in a critical region on the same thread, it should break.
    # This is fine since we would never call fork directly from a critical region.
    os.register_at_fork(after_in_child=_after_fork)
