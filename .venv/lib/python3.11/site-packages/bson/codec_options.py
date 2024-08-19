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

"""Tools for specifying BSON codec options."""
from __future__ import annotations

import abc
import datetime
import enum
from collections.abc import MutableMapping as _MutableMapping
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterable,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from bson.binary import (
    ALL_UUID_REPRESENTATIONS,
    UUID_REPRESENTATION_NAMES,
    UuidRepresentation,
)
from bson.typings import _DocumentType

_RAW_BSON_DOCUMENT_MARKER = 101


def _raw_document_class(document_class: Any) -> bool:
    """Determine if a document_class is a RawBSONDocument class."""
    marker = getattr(document_class, "_type_marker", None)
    return marker == _RAW_BSON_DOCUMENT_MARKER


class TypeEncoder(abc.ABC):
    """Base class for defining type codec classes which describe how a
    custom type can be transformed to one of the types BSON understands.

    Codec classes must implement the ``python_type`` attribute, and the
    ``transform_python`` method to support encoding.

    See :ref:`custom-type-type-codec` documentation for an example.
    """

    @abc.abstractproperty
    def python_type(self) -> Any:
        """The Python type to be converted into something serializable."""

    @abc.abstractmethod
    def transform_python(self, value: Any) -> Any:
        """Convert the given Python object into something serializable."""


class TypeDecoder(abc.ABC):
    """Base class for defining type codec classes which describe how a
    BSON type can be transformed to a custom type.

    Codec classes must implement the ``bson_type`` attribute, and the
    ``transform_bson`` method to support decoding.

    See :ref:`custom-type-type-codec` documentation for an example.
    """

    @abc.abstractproperty
    def bson_type(self) -> Any:
        """The BSON type to be converted into our own type."""

    @abc.abstractmethod
    def transform_bson(self, value: Any) -> Any:
        """Convert the given BSON value into our own type."""


class TypeCodec(TypeEncoder, TypeDecoder):
    """Base class for defining type codec classes which describe how a
    custom type can be transformed to/from one of the types :mod:`bson`
    can already encode/decode.

    Codec classes must implement the ``python_type`` attribute, and the
    ``transform_python`` method to support encoding, as well as the
    ``bson_type`` attribute, and the ``transform_bson`` method to support
    decoding.

    See :ref:`custom-type-type-codec` documentation for an example.
    """


_Codec = Union[TypeEncoder, TypeDecoder, TypeCodec]
_Fallback = Callable[[Any], Any]


class TypeRegistry:
    """Encapsulates type codecs used in encoding and / or decoding BSON, as
    well as the fallback encoder. Type registries cannot be modified after
    instantiation.

    ``TypeRegistry`` can be initialized with an iterable of type codecs, and
    a callable for the fallback encoder::

      >>> from bson.codec_options import TypeRegistry
      >>> type_registry = TypeRegistry([Codec1, Codec2, Codec3, ...],
      ...                              fallback_encoder)

    See :ref:`custom-type-type-registry` documentation for an example.

    :param type_codecs: iterable of type codec instances. If
        ``type_codecs`` contains multiple codecs that transform a single
        python or BSON type, the transformation specified by the type codec
        occurring last prevails. A TypeError will be raised if one or more
        type codecs modify the encoding behavior of a built-in :mod:`bson`
        type.
    :param fallback_encoder: callable that accepts a single,
        unencodable python value and transforms it into a type that
        :mod:`bson` can encode. See :ref:`fallback-encoder-callable`
        documentation for an example.
    """

    def __init__(
        self,
        type_codecs: Optional[Iterable[_Codec]] = None,
        fallback_encoder: Optional[_Fallback] = None,
    ) -> None:
        self.__type_codecs = list(type_codecs or [])
        self._fallback_encoder = fallback_encoder
        self._encoder_map: dict[Any, Any] = {}
        self._decoder_map: dict[Any, Any] = {}

        if self._fallback_encoder is not None:
            if not callable(fallback_encoder):
                raise TypeError("fallback_encoder %r is not a callable" % (fallback_encoder))

        for codec in self.__type_codecs:
            is_valid_codec = False
            if isinstance(codec, TypeEncoder):
                self._validate_type_encoder(codec)
                is_valid_codec = True
                self._encoder_map[codec.python_type] = codec.transform_python
            if isinstance(codec, TypeDecoder):
                is_valid_codec = True
                self._decoder_map[codec.bson_type] = codec.transform_bson
            if not is_valid_codec:
                raise TypeError(
                    f"Expected an instance of {TypeEncoder.__name__}, {TypeDecoder.__name__}, or {TypeCodec.__name__}, got {codec!r} instead"
                )

    def _validate_type_encoder(self, codec: _Codec) -> None:
        from bson import _BUILT_IN_TYPES

        for pytype in _BUILT_IN_TYPES:
            if issubclass(cast(TypeCodec, codec).python_type, pytype):
                err_msg = (
                    "TypeEncoders cannot change how built-in types are "
                    f"encoded (encoder {codec} transforms type {pytype})"
                )
                raise TypeError(err_msg)

    def __repr__(self) -> str:
        return "{}(type_codecs={!r}, fallback_encoder={!r})".format(
            self.__class__.__name__,
            self.__type_codecs,
            self._fallback_encoder,
        )

    def __eq__(self, other: Any) -> Any:
        if not isinstance(other, type(self)):
            return NotImplemented
        return (
            (self._decoder_map == other._decoder_map)
            and (self._encoder_map == other._encoder_map)
            and (self._fallback_encoder == other._fallback_encoder)
        )


class DatetimeConversion(int, enum.Enum):
    """Options for decoding BSON datetimes."""

    DATETIME = 1
    """Decode a BSON UTC datetime as a :class:`datetime.datetime`.

    BSON UTC datetimes that cannot be represented as a
    :class:`~datetime.datetime` will raise an :class:`OverflowError`
    or a :class:`ValueError`.

    .. versionadded 4.3
    """

    DATETIME_CLAMP = 2
    """Decode a BSON UTC datetime as a :class:`datetime.datetime`, clamping
    to :attr:`~datetime.datetime.min` and :attr:`~datetime.datetime.max`.

    .. versionadded 4.3
    """

    DATETIME_MS = 3
    """Decode a BSON UTC datetime as a :class:`~bson.datetime_ms.DatetimeMS`
    object.

    .. versionadded 4.3
    """

    DATETIME_AUTO = 4
    """Decode a BSON UTC datetime as a :class:`datetime.datetime` if possible,
    and a :class:`~bson.datetime_ms.DatetimeMS` if not.

    .. versionadded 4.3
    """


class _BaseCodecOptions(NamedTuple):
    document_class: Type[Mapping[str, Any]]
    tz_aware: bool
    uuid_representation: int
    unicode_decode_error_handler: str
    tzinfo: Optional[datetime.tzinfo]
    type_registry: TypeRegistry
    datetime_conversion: Optional[DatetimeConversion]


if TYPE_CHECKING:

    class CodecOptions(Tuple[_DocumentType], Generic[_DocumentType]):
        document_class: Type[_DocumentType]
        tz_aware: bool
        uuid_representation: int
        unicode_decode_error_handler: Optional[str]
        tzinfo: Optional[datetime.tzinfo]
        type_registry: TypeRegistry
        datetime_conversion: Optional[int]

        def __new__(
            cls: Type[CodecOptions[_DocumentType]],
            document_class: Optional[Type[_DocumentType]] = ...,
            tz_aware: bool = ...,
            uuid_representation: Optional[int] = ...,
            unicode_decode_error_handler: Optional[str] = ...,
            tzinfo: Optional[datetime.tzinfo] = ...,
            type_registry: Optional[TypeRegistry] = ...,
            datetime_conversion: Optional[int] = ...,
        ) -> CodecOptions[_DocumentType]:
            ...

        # CodecOptions API
        def with_options(self, **kwargs: Any) -> CodecOptions[Any]:
            ...

        def _arguments_repr(self) -> str:
            ...

        def _options_dict(self) -> dict[Any, Any]:
            ...

        # NamedTuple API
        @classmethod
        def _make(cls, obj: Iterable[Any]) -> CodecOptions[_DocumentType]:
            ...

        def _asdict(self) -> dict[str, Any]:
            ...

        def _replace(self, **kwargs: Any) -> CodecOptions[_DocumentType]:
            ...

        _source: str
        _fields: Tuple[str]

else:

    class CodecOptions(_BaseCodecOptions):
        """Encapsulates options used encoding and / or decoding BSON."""

        def __init__(self, *args, **kwargs):
            """Encapsulates options used encoding and / or decoding BSON.

            The `document_class` option is used to define a custom type for use
            decoding BSON documents. Access to the underlying raw BSON bytes for
            a document is available using the :class:`~bson.raw_bson.RawBSONDocument`
            type::

              >>> from bson.raw_bson import RawBSONDocument
              >>> from bson.codec_options import CodecOptions
              >>> codec_options = CodecOptions(document_class=RawBSONDocument)
              >>> coll = db.get_collection('test', codec_options=codec_options)
              >>> doc = coll.find_one()
              >>> doc.raw
              '\\x16\\x00\\x00\\x00\\x07_id\\x00[0\\x165\\x91\\x10\\xea\\x14\\xe8\\xc5\\x8b\\x93\\x00'

            The document class can be any type that inherits from
            :class:`~collections.abc.MutableMapping`::

              >>> class AttributeDict(dict):
              ...     # A dict that supports attribute access.
              ...     def __getattr__(self, key):
              ...         return self[key]
              ...     def __setattr__(self, key, value):
              ...         self[key] = value
              ...
              >>> codec_options = CodecOptions(document_class=AttributeDict)
              >>> coll = db.get_collection('test', codec_options=codec_options)
              >>> doc = coll.find_one()
              >>> doc._id
              ObjectId('5b3016359110ea14e8c58b93')

            See :doc:`/examples/datetimes` for examples using the `tz_aware` and
            `tzinfo` options.

            See :doc:`/examples/uuid` for examples using the `uuid_representation`
            option.

            :param document_class: BSON documents returned in queries will be decoded
                to an instance of this class. Must be a subclass of
                :class:`~collections.abc.MutableMapping`. Defaults to :class:`dict`.
            :param tz_aware: If ``True``, BSON datetimes will be decoded to timezone
                aware instances of :class:`~datetime.datetime`. Otherwise they will be
                naive. Defaults to ``False``.
            :param uuid_representation: The BSON representation to use when encoding
                and decoding instances of :class:`~uuid.UUID`. Defaults to
                :data:`~bson.binary.UuidRepresentation.UNSPECIFIED`. New
                applications should consider setting this to
                :data:`~bson.binary.UuidRepresentation.STANDARD` for cross language
                compatibility. See :ref:`handling-uuid-data-example` for details.
            :param unicode_decode_error_handler: The error handler to apply when
                a Unicode-related error occurs during BSON decoding that would
                otherwise raise :exc:`UnicodeDecodeError`. Valid options include
                'strict', 'replace', 'backslashreplace', 'surrogateescape', and
                'ignore'. Defaults to 'strict'.
            :param tzinfo: A :class:`~datetime.tzinfo` subclass that specifies the
                timezone to/from which :class:`~datetime.datetime` objects should be
                encoded/decoded.
            :param type_registry: Instance of :class:`TypeRegistry` used to customize
                encoding and decoding behavior.
            :param datetime_conversion: Specifies how UTC datetimes should be decoded
                within BSON. Valid options include 'datetime_ms' to return as a
                DatetimeMS, 'datetime' to return as a datetime.datetime and
                raising a ValueError for out-of-range values, 'datetime_auto' to
                return DatetimeMS objects when the underlying datetime is
                out-of-range and 'datetime_clamp' to clamp to the minimum and
                maximum possible datetimes. Defaults to 'datetime'.

            .. versionchanged:: 4.0
               The default for `uuid_representation` was changed from
               :const:`~bson.binary.UuidRepresentation.PYTHON_LEGACY` to
               :const:`~bson.binary.UuidRepresentation.UNSPECIFIED`.

            .. versionadded:: 3.8
               `type_registry` attribute.

            .. warning:: Care must be taken when changing
               `unicode_decode_error_handler` from its default value ('strict').
               The 'replace' and 'ignore' modes should not be used when documents
               retrieved from the server will be modified in the client application
               and stored back to the server.
            """
            super().__init__()

        def __new__(
            cls: Type[CodecOptions],
            document_class: Optional[Type[Mapping[str, Any]]] = None,
            tz_aware: bool = False,
            uuid_representation: Optional[int] = UuidRepresentation.UNSPECIFIED,
            unicode_decode_error_handler: str = "strict",
            tzinfo: Optional[datetime.tzinfo] = None,
            type_registry: Optional[TypeRegistry] = None,
            datetime_conversion: Optional[DatetimeConversion] = DatetimeConversion.DATETIME,
        ) -> CodecOptions:
            doc_class = document_class or dict
            # issubclass can raise TypeError for generic aliases like SON[str, Any].
            # In that case we can use the base class for the comparison.
            is_mapping = False
            try:
                is_mapping = issubclass(doc_class, _MutableMapping)
            except TypeError:
                if hasattr(doc_class, "__origin__"):
                    is_mapping = issubclass(doc_class.__origin__, _MutableMapping)
            if not (is_mapping or _raw_document_class(doc_class)):
                raise TypeError(
                    "document_class must be dict, bson.son.SON, "
                    "bson.raw_bson.RawBSONDocument, or a "
                    "subclass of collections.abc.MutableMapping"
                )
            if not isinstance(tz_aware, bool):
                raise TypeError(f"tz_aware must be True or False, was: tz_aware={tz_aware}")
            if uuid_representation not in ALL_UUID_REPRESENTATIONS:
                raise ValueError(
                    "uuid_representation must be a value from bson.binary.UuidRepresentation"
                )
            if not isinstance(unicode_decode_error_handler, str):
                raise ValueError("unicode_decode_error_handler must be a string")
            if tzinfo is not None:
                if not isinstance(tzinfo, datetime.tzinfo):
                    raise TypeError("tzinfo must be an instance of datetime.tzinfo")
                if not tz_aware:
                    raise ValueError("cannot specify tzinfo without also setting tz_aware=True")

            type_registry = type_registry or TypeRegistry()

            if not isinstance(type_registry, TypeRegistry):
                raise TypeError("type_registry must be an instance of TypeRegistry")

            return tuple.__new__(
                cls,
                (
                    doc_class,
                    tz_aware,
                    uuid_representation,
                    unicode_decode_error_handler,
                    tzinfo,
                    type_registry,
                    datetime_conversion,
                ),
            )

        def _arguments_repr(self) -> str:
            """Representation of the arguments used to create this object."""
            document_class_repr = (
                "dict" if self.document_class is dict else repr(self.document_class)
            )

            uuid_rep_repr = UUID_REPRESENTATION_NAMES.get(
                self.uuid_representation, self.uuid_representation
            )

            return (
                "document_class={}, tz_aware={!r}, uuid_representation={}, "
                "unicode_decode_error_handler={!r}, tzinfo={!r}, "
                "type_registry={!r}, datetime_conversion={!s}".format(
                    document_class_repr,
                    self.tz_aware,
                    uuid_rep_repr,
                    self.unicode_decode_error_handler,
                    self.tzinfo,
                    self.type_registry,
                    self.datetime_conversion,
                )
            )

        def _options_dict(self) -> dict[str, Any]:
            """Dictionary of the arguments used to create this object."""
            # TODO: PYTHON-2442 use _asdict() instead
            return {
                "document_class": self.document_class,
                "tz_aware": self.tz_aware,
                "uuid_representation": self.uuid_representation,
                "unicode_decode_error_handler": self.unicode_decode_error_handler,
                "tzinfo": self.tzinfo,
                "type_registry": self.type_registry,
                "datetime_conversion": self.datetime_conversion,
            }

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self._arguments_repr()})"

        def with_options(self, **kwargs: Any) -> CodecOptions:
            """Make a copy of this CodecOptions, overriding some options::

                >>> from bson.codec_options import DEFAULT_CODEC_OPTIONS
                >>> DEFAULT_CODEC_OPTIONS.tz_aware
                False
                >>> options = DEFAULT_CODEC_OPTIONS.with_options(tz_aware=True)
                >>> options.tz_aware
                True

            .. versionadded:: 3.5
            """
            opts = self._options_dict()
            opts.update(kwargs)
            return CodecOptions(**opts)


DEFAULT_CODEC_OPTIONS: CodecOptions[dict[str, Any]] = CodecOptions()


def _parse_codec_options(options: Any) -> CodecOptions[Any]:
    """Parse BSON codec options."""
    kwargs = {}
    for k in set(options) & {
        "document_class",
        "tz_aware",
        "uuidrepresentation",
        "unicode_decode_error_handler",
        "tzinfo",
        "type_registry",
        "datetime_conversion",
    }:
        if k == "uuidrepresentation":
            kwargs["uuid_representation"] = options[k]
        else:
            kwargs[k] = options[k]
    return CodecOptions(**kwargs)
