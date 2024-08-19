# Copyright 2015-present MongoDB, Inc.
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

"""Tools for representing raw BSON documents.

Inserting and Retrieving RawBSONDocuments
=========================================

Example: Moving a document between different databases/collections

.. doctest::

  >>> import bson
  >>> from pymongo import MongoClient
  >>> from bson.raw_bson import RawBSONDocument
  >>> client = MongoClient(document_class=RawBSONDocument)
  >>> client.drop_database("db")
  >>> client.drop_database("replica_db")
  >>> db = client.db
  >>> result = db.test.insert_many(
  ...     [{"_id": 1, "a": 1}, {"_id": 2, "b": 1}, {"_id": 3, "c": 1}, {"_id": 4, "d": 1}]
  ... )
  >>> replica_db = client.replica_db
  >>> for doc in db.test.find():
  ...     print(f"raw document: {doc.raw}")
  ...     print(f"decoded document: {bson.decode(doc.raw)}")
  ...     result = replica_db.test.insert_one(doc)
  ...
  raw document: b'...'
  decoded document: {'_id': 1, 'a': 1}
  raw document: b'...'
  decoded document: {'_id': 2, 'b': 1}
  raw document: b'...'
  decoded document: {'_id': 3, 'c': 1}
  raw document: b'...'
  decoded document: {'_id': 4, 'd': 1}

For use cases like moving documents across different databases or writing binary
blobs to disk, using raw BSON documents provides better speed and avoids the
overhead of decoding or encoding BSON.
"""
from __future__ import annotations

from typing import Any, ItemsView, Iterator, Mapping, Optional

from bson import _get_object_size, _raw_to_dict
from bson.codec_options import _RAW_BSON_DOCUMENT_MARKER, CodecOptions
from bson.codec_options import DEFAULT_CODEC_OPTIONS as DEFAULT


def _inflate_bson(
    bson_bytes: bytes, codec_options: CodecOptions[RawBSONDocument], raw_array: bool = False
) -> dict[str, Any]:
    """Inflates the top level fields of a BSON document.

    :param bson_bytes: the BSON bytes that compose this document
    :param codec_options: An instance of
        :class:`~bson.codec_options.CodecOptions` whose ``document_class``
        must be :class:`RawBSONDocument`.
    """
    return _raw_to_dict(bson_bytes, 4, len(bson_bytes) - 1, codec_options, {}, raw_array=raw_array)


class RawBSONDocument(Mapping[str, Any]):
    """Representation for a MongoDB document that provides access to the raw
    BSON bytes that compose it.

    Only when a field is accessed or modified within the document does
    RawBSONDocument decode its bytes.
    """

    __slots__ = ("__raw", "__inflated_doc", "__codec_options")
    _type_marker = _RAW_BSON_DOCUMENT_MARKER
    __codec_options: CodecOptions[RawBSONDocument]

    def __init__(
        self, bson_bytes: bytes, codec_options: Optional[CodecOptions[RawBSONDocument]] = None
    ) -> None:
        """Create a new :class:`RawBSONDocument`

        :class:`RawBSONDocument` is a representation of a BSON document that
        provides access to the underlying raw BSON bytes. Only when a field is
        accessed or modified within the document does RawBSONDocument decode
        its bytes.

        :class:`RawBSONDocument` implements the ``Mapping`` abstract base
        class from the standard library so it can be used like a read-only
        ``dict``::

            >>> from bson import encode
            >>> raw_doc = RawBSONDocument(encode({'_id': 'my_doc'}))
            >>> raw_doc.raw
            b'...'
            >>> raw_doc['_id']
            'my_doc'

        :param bson_bytes: the BSON bytes that compose this document
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions` whose ``document_class``
            must be :class:`RawBSONDocument`. The default is
            :attr:`DEFAULT_RAW_BSON_OPTIONS`.

        .. versionchanged:: 3.8
          :class:`RawBSONDocument` now validates that the ``bson_bytes``
          passed in represent a single bson document.

        .. versionchanged:: 3.5
          If a :class:`~bson.codec_options.CodecOptions` is passed in, its
          `document_class` must be :class:`RawBSONDocument`.
        """
        self.__raw = bson_bytes
        self.__inflated_doc: Optional[Mapping[str, Any]] = None
        # Can't default codec_options to DEFAULT_RAW_BSON_OPTIONS in signature,
        # it refers to this class RawBSONDocument.
        if codec_options is None:
            codec_options = DEFAULT_RAW_BSON_OPTIONS
        elif not issubclass(codec_options.document_class, RawBSONDocument):
            raise TypeError(
                "RawBSONDocument cannot use CodecOptions with document "
                f"class {codec_options.document_class}"
            )
        self.__codec_options = codec_options
        # Validate the bson object size.
        _get_object_size(bson_bytes, 0, len(bson_bytes))

    @property
    def raw(self) -> bytes:
        """The raw BSON bytes composing this document."""
        return self.__raw

    def items(self) -> ItemsView[str, Any]:
        """Lazily decode and iterate elements in this document."""
        return self.__inflated.items()

    @property
    def __inflated(self) -> Mapping[str, Any]:
        if self.__inflated_doc is None:
            # We already validated the object's size when this document was
            # created, so no need to do that again.
            self.__inflated_doc = self._inflate_bson(self.__raw, self.__codec_options)
        return self.__inflated_doc

    @staticmethod
    def _inflate_bson(
        bson_bytes: bytes, codec_options: CodecOptions[RawBSONDocument]
    ) -> Mapping[str, Any]:
        return _inflate_bson(bson_bytes, codec_options)

    def __getitem__(self, item: str) -> Any:
        return self.__inflated[item]

    def __iter__(self) -> Iterator[str]:
        return iter(self.__inflated)

    def __len__(self) -> int:
        return len(self.__inflated)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RawBSONDocument):
            return self.__raw == other.raw
        return NotImplemented

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.raw!r}, codec_options={self.__codec_options!r})"


class _RawArrayBSONDocument(RawBSONDocument):
    """A RawBSONDocument that only expands sub-documents and arrays when accessed."""

    @staticmethod
    def _inflate_bson(
        bson_bytes: bytes, codec_options: CodecOptions[RawBSONDocument]
    ) -> Mapping[str, Any]:
        return _inflate_bson(bson_bytes, codec_options, raw_array=True)


DEFAULT_RAW_BSON_OPTIONS: CodecOptions[RawBSONDocument] = DEFAULT.with_options(
    document_class=RawBSONDocument
)
_RAW_ARRAY_BSON_OPTIONS: CodecOptions[_RawArrayBSONDocument] = DEFAULT.with_options(
    document_class=_RawArrayBSONDocument
)
"""The default :class:`~bson.codec_options.CodecOptions` for
:class:`RawBSONDocument`.
"""
