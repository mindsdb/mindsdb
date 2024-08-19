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

"""Collection level utilities for Mongo."""
from __future__ import annotations

from collections import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

from bson.codec_options import DEFAULT_CODEC_OPTIONS, CodecOptions
from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from bson.timestamp import Timestamp
from pymongo import ASCENDING, _csot, common, helpers, message
from pymongo.aggregation import (
    _CollectionAggregationCommand,
    _CollectionRawAggregationCommand,
)
from pymongo.bulk import _Bulk
from pymongo.change_stream import CollectionChangeStream
from pymongo.collation import validate_collation_or_none
from pymongo.command_cursor import CommandCursor, RawBatchCommandCursor
from pymongo.common import _ecoc_coll_name, _esc_coll_name
from pymongo.cursor import Cursor, RawBatchCursor
from pymongo.errors import (
    ConfigurationError,
    InvalidName,
    InvalidOperation,
    OperationFailure,
)
from pymongo.helpers import _check_write_command_response
from pymongo.message import _UNICODE_REPLACE_CODEC_OPTIONS
from pymongo.operations import (
    DeleteMany,
    DeleteOne,
    IndexModel,
    InsertOne,
    ReplaceOne,
    SearchIndexModel,
    UpdateMany,
    UpdateOne,
    _IndexKeyHint,
    _IndexList,
    _Op,
)
from pymongo.read_concern import DEFAULT_READ_CONCERN, ReadConcern
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)
from pymongo.typings import _CollationIn, _DocumentType, _DocumentTypeArg, _Pipeline
from pymongo.write_concern import DEFAULT_WRITE_CONCERN, WriteConcern, validate_boolean

T = TypeVar("T")

_FIND_AND_MODIFY_DOC_FIELDS = {"value": 1}


_WriteOp = Union[
    InsertOne[_DocumentType],
    DeleteOne,
    DeleteMany,
    ReplaceOne[_DocumentType],
    UpdateOne,
    UpdateMany,
]


class ReturnDocument:
    """An enum used with
    :meth:`~pymongo.collection.Collection.find_one_and_replace` and
    :meth:`~pymongo.collection.Collection.find_one_and_update`.
    """

    BEFORE = False
    """Return the original document before it was updated/replaced, or
    ``None`` if no document matches the query.
    """
    AFTER = True
    """Return the updated/replaced or inserted document."""


if TYPE_CHECKING:
    from pymongo.aggregation import _AggregationCommand
    from pymongo.client_session import ClientSession
    from pymongo.collation import Collation
    from pymongo.database import Database
    from pymongo.pool import Connection
    from pymongo.server import Server


class Collection(common.BaseObject, Generic[_DocumentType]):
    """A Mongo collection."""

    def __init__(
        self,
        database: Database[_DocumentType],
        name: str,
        create: Optional[bool] = False,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> None:
        """Get / create a Mongo collection.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`str`. Raises :class:`~pymongo.errors.InvalidName` if `name` is
        not a valid collection name. Any additional keyword arguments will be used
        as options passed to the create command. See
        :meth:`~pymongo.database.Database.create_collection` for valid
        options.

        If `create` is ``True``, `collation` is specified, or any additional
        keyword arguments are present, a ``create`` command will be
        sent, using ``session`` if specified. Otherwise, a ``create`` command
        will not be sent and the collection will be created implicitly on first
        use. The optional ``session`` argument is *only* used for the ``create``
        command, it is not associated with the collection afterward.

        :param database: the database to get a collection from
        :param name: the name of the collection to get
        :param create: if ``True``, force collection
            creation even without options being set
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) database.codec_options is used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) database.read_preference is used.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) database.write_concern is used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) database.read_concern is used.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`. If a collation is provided,
            it will be passed to the create collection command.
        :param session: a
            :class:`~pymongo.client_session.ClientSession` that is used with
            the create collection command
        :param kwargs: additional keyword arguments will
            be passed as options for the create collection command

        .. versionchanged:: 4.2
           Added the ``clusteredIndex`` and ``encryptedFields`` parameters.

        .. versionchanged:: 4.0
           Removed the reindex, map_reduce, inline_map_reduce,
           parallel_scan, initialize_unordered_bulk_op,
           initialize_ordered_bulk_op, group, count, insert, save,
           update, remove, find_and_modify, and ensure_index methods. See the
           :ref:`pymongo4-migration-guide`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        .. versionchanged:: 3.2
           Added the read_concern option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.
           Removed the uuid_subtype attribute.
           :class:`~pymongo.collection.Collection` no longer returns an
           instance of :class:`~pymongo.collection.Collection` for attribute
           names with leading underscores. You must use dict-style lookups
           instead::

               collection['__my_collection__']

           Not:

               collection.__my_collection__

        .. seealso:: The MongoDB documentation on `collections <https://dochub.mongodb.org/core/collections>`_.
        """
        super().__init__(
            codec_options or database.codec_options,
            read_preference or database.read_preference,
            write_concern or database.write_concern,
            read_concern or database.read_concern,
        )
        if not isinstance(name, str):
            raise TypeError("name must be an instance of str")

        if not name or ".." in name:
            raise InvalidName("collection names cannot be empty")
        if "$" in name and not (name.startswith(("oplog.$main", "$cmd"))):
            raise InvalidName("collection names must not contain '$': %r" % name)
        if name[0] == "." or name[-1] == ".":
            raise InvalidName("collection names must not start or end with '.': %r" % name)
        if "\x00" in name:
            raise InvalidName("collection names must not contain the null character")
        collation = validate_collation_or_none(kwargs.pop("collation", None))

        self.__database: Database[_DocumentType] = database
        self.__name = name
        self.__full_name = f"{self.__database.name}.{self.__name}"
        self.__write_response_codec_options = self.codec_options._replace(
            unicode_decode_error_handler="replace", document_class=dict
        )
        self._timeout = database.client.options.timeout
        encrypted_fields = kwargs.pop("encryptedFields", None)
        if create or kwargs or collation:
            if encrypted_fields:
                common.validate_is_mapping("encrypted_fields", encrypted_fields)
                opts = {"clusteredIndex": {"key": {"_id": 1}, "unique": True}}
                self.__create(
                    _esc_coll_name(encrypted_fields, name), opts, None, session, qev2_required=True
                )
                self.__create(_ecoc_coll_name(encrypted_fields, name), opts, None, session)
                self.__create(name, kwargs, collation, session, encrypted_fields=encrypted_fields)
                self.create_index([("__safeContent__", ASCENDING)], session)
            else:
                self.__create(name, kwargs, collation, session)

    def _conn_for_writes(
        self, session: Optional[ClientSession], operation: str
    ) -> ContextManager[Connection]:
        return self.__database.client._conn_for_writes(session, operation)

    def _command(
        self,
        conn: Connection,
        command: MutableMapping[str, Any],
        read_preference: Optional[_ServerMode] = None,
        codec_options: Optional[CodecOptions] = None,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_concern: Optional[ReadConcern] = None,
        write_concern: Optional[WriteConcern] = None,
        collation: Optional[_CollationIn] = None,
        session: Optional[ClientSession] = None,
        retryable_write: bool = False,
        user_fields: Optional[Any] = None,
    ) -> Mapping[str, Any]:
        """Internal command helper.

        :param conn` - A Connection instance.
        :param command` - The command itself, as a :class:`~bson.son.SON` instance.
        :param read_preference` (optional) - The read preference to use.
        :param codec_options` (optional) - An instance of
            :class:`~bson.codec_options.CodecOptions`.
        :param check: raise OperationFailure if there are errors
        :param allowable_errors: errors to ignore if `check` is True
        :param read_concern` (optional) - An instance of
            :class:`~pymongo.read_concern.ReadConcern`.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`.
        :param collation` (optional) - An instance of
            :class:`~pymongo.collation.Collation`.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param retryable_write: True if this command is a retryable
            write.
        :param user_fields: Response fields that should be decoded
            using the TypeDecoders from codec_options, passed to
            bson._decode_all_selective.

        :return: The result document.
        """
        with self.__database.client._tmp_session(session) as s:
            return conn.command(
                self.__database.name,
                command,
                read_preference or self._read_preference_for(session),
                codec_options or self.codec_options,
                check,
                allowable_errors,
                read_concern=read_concern,
                write_concern=write_concern,
                parse_write_concern_error=True,
                collation=collation,
                session=s,
                client=self.__database.client,
                retryable_write=retryable_write,
                user_fields=user_fields,
            )

    def __create(
        self,
        name: str,
        options: MutableMapping[str, Any],
        collation: Optional[_CollationIn],
        session: Optional[ClientSession],
        encrypted_fields: Optional[Mapping[str, Any]] = None,
        qev2_required: bool = False,
    ) -> None:
        """Sends a create command with the given options."""
        cmd: dict[str, Any] = {"create": name}
        if encrypted_fields:
            cmd["encryptedFields"] = encrypted_fields

        if options:
            if "size" in options:
                options["size"] = float(options["size"])
            cmd.update(options)
        with self._conn_for_writes(session, operation=_Op.CREATE) as conn:
            if qev2_required and conn.max_wire_version < 21:
                raise ConfigurationError(
                    "Driver support of Queryable Encryption is incompatible with server. "
                    "Upgrade server to use Queryable Encryption. "
                    f"Got maxWireVersion {conn.max_wire_version} but need maxWireVersion >= 21 (MongoDB >=7.0)"
                )

            self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                write_concern=self._write_concern_for(session),
                collation=collation,
                session=session,
            )

    def __getattr__(self, name: str) -> Collection[_DocumentType]:
        """Get a sub-collection of this collection by name.

        Raises InvalidName if an invalid collection name is used.

        :param name: the name of the collection to get
        """
        if name.startswith("_"):
            full_name = f"{self.__name}.{name}"
            raise AttributeError(
                f"Collection has no attribute {name!r}. To access the {full_name}"
                f" collection, use database['{full_name}']."
            )
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> Collection[_DocumentType]:
        return Collection(
            self.__database,
            f"{self.__name}.{name}",
            False,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )

    def __repr__(self) -> str:
        return f"Collection({self.__database!r}, {self.__name!r})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Collection):
            return self.__database == other.database and self.__name == other.name
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash((self.__database, self.__name))

    def __bool__(self) -> NoReturn:
        raise NotImplementedError(
            "Collection objects do not implement truth "
            "value testing or bool(). Please compare "
            "with None instead: collection is not None"
        )

    @property
    def full_name(self) -> str:
        """The full name of this :class:`Collection`.

        The full name is of the form `database_name.collection_name`.
        """
        return self.__full_name

    @property
    def name(self) -> str:
        """The name of this :class:`Collection`."""
        return self.__name

    @property
    def database(self) -> Database[_DocumentType]:
        """The :class:`~pymongo.database.Database` that this
        :class:`Collection` is a part of.
        """
        return self.__database

    def with_options(
        self,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> Collection[_DocumentType]:
        """Get a clone of this collection changing the specified settings.

          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = coll1.with_options(read_preference=ReadPreference.SECONDARY)
          >>> coll1.read_preference
          Primary()
          >>> coll2.read_preference
          Secondary(tag_sets=None)

        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Collection`
            is used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Collection` is used. See :mod:`~pymongo.read_preferences`
            for options.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Collection`
            is used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Collection`
            is used.
        """
        return Collection(
            self.__database,
            self.__name,
            False,
            codec_options or self.codec_options,
            read_preference or self.read_preference,
            write_concern or self.write_concern,
            read_concern or self.read_concern,
        )

    @_csot.apply
    def bulk_write(
        self,
        requests: Sequence[_WriteOp[_DocumentType]],
        ordered: bool = True,
        bypass_document_validation: bool = False,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        let: Optional[Mapping] = None,
    ) -> BulkWriteResult:
        """Send a batch of write operations to the server.

        Requests are passed as a list of write operation instances (
        :class:`~pymongo.operations.InsertOne`,
        :class:`~pymongo.operations.UpdateOne`,
        :class:`~pymongo.operations.UpdateMany`,
        :class:`~pymongo.operations.ReplaceOne`,
        :class:`~pymongo.operations.DeleteOne`, or
        :class:`~pymongo.operations.DeleteMany`).

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634ef')}
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634f0')}
          >>> # DeleteMany, UpdateOne, and UpdateMany are also available.
          ...
          >>> from pymongo import InsertOne, DeleteOne, ReplaceOne
          >>> requests = [InsertOne({'y': 1}), DeleteOne({'x': 1}),
          ...             ReplaceOne({'w': 1}, {'z': 1}, upsert=True)]
          >>> result = db.test.bulk_write(requests)
          >>> result.inserted_count
          1
          >>> result.deleted_count
          1
          >>> result.modified_count
          0
          >>> result.upserted_ids
          {2: ObjectId('54f62ee28891e756a6e1abd5')}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f62e60fba5226811f634f0')}
          {'y': 1, '_id': ObjectId('54f62ee2fba5226811f634f1')}
          {'z': 1, '_id': ObjectId('54f62ee28891e756a6e1abd5')}

        :param requests: A list of write operations (see examples above).
        :param ordered: If ``True`` (the default) requests will be
            performed on the server serially, in the order provided. If an error
            occurs all remaining operations are aborted. If ``False`` requests
            will be performed on the server in arbitrary order, possibly in
            parallel, and all operations will be attempted.
        :param bypass_document_validation: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").

        :return: An instance of :class:`~pymongo.results.BulkWriteResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 4.1
           Added ``comment`` parameter.
           Added ``let`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_list("requests", requests)

        blk = _Bulk(self, ordered, bypass_document_validation, comment=comment, let=let)
        for request in requests:
            try:
                request._add_to_bulk(blk)
            except AttributeError:
                raise TypeError(f"{request!r} is not a valid request") from None

        write_concern = self._write_concern_for(session)
        bulk_api_result = blk.execute(write_concern, session, _Op.INSERT)
        if bulk_api_result is not None:
            return BulkWriteResult(bulk_api_result, True)
        return BulkWriteResult({}, False)

    def _insert_one(
        self,
        doc: Mapping[str, Any],
        ordered: bool,
        write_concern: WriteConcern,
        op_id: Optional[int],
        bypass_doc_val: bool,
        session: Optional[ClientSession],
        comment: Optional[Any] = None,
    ) -> Any:
        """Internal helper for inserting a single document."""
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        command = {"insert": self.name, "ordered": ordered, "documents": [doc]}
        if comment is not None:
            command["comment"] = comment

        def _insert_command(
            session: Optional[ClientSession], conn: Connection, retryable_write: bool
        ) -> None:
            if bypass_doc_val:
                command["bypassDocumentValidation"] = True

            result = conn.command(
                self.__database.name,
                command,
                write_concern=write_concern,
                codec_options=self.__write_response_codec_options,
                session=session,
                client=self.__database.client,
                retryable_write=retryable_write,
            )

            _check_write_command_response(result)

        self.__database.client._retryable_write(
            acknowledged, _insert_command, session, operation=_Op.INSERT
        )

        if not isinstance(doc, RawBSONDocument):
            return doc.get("_id")
        return None

    def insert_one(
        self,
        document: Union[_DocumentType, RawBSONDocument],
        bypass_document_validation: bool = False,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> InsertOneResult:
        """Insert a single document.

          >>> db.test.count_documents({'x': 1})
          0
          >>> result = db.test.insert_one({'x': 1})
          >>> result.inserted_id
          ObjectId('54f112defba522406c9cc208')
          >>> db.test.find_one({'x': 1})
          {'x': 1, '_id': ObjectId('54f112defba522406c9cc208')}

        :param document: The document to insert. Must be a mutable mapping
            type. If the document does not have an _id field one will be
            added automatically.
        :param bypass_document_validation: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        :return: - An instance of :class:`~pymongo.results.InsertOneResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        common.validate_is_document_type("document", document)
        if not (isinstance(document, RawBSONDocument) or "_id" in document):
            document["_id"] = ObjectId()  # type: ignore[index]

        write_concern = self._write_concern_for(session)
        return InsertOneResult(
            self._insert_one(
                document,
                ordered=True,
                write_concern=write_concern,
                op_id=None,
                bypass_doc_val=bypass_document_validation,
                session=session,
                comment=comment,
            ),
            write_concern.acknowledged,
        )

    @_csot.apply
    def insert_many(
        self,
        documents: Iterable[Union[_DocumentType, RawBSONDocument]],
        ordered: bool = True,
        bypass_document_validation: bool = False,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> InsertManyResult:
        """Insert an iterable of documents.

          >>> db.test.count_documents({})
          0
          >>> result = db.test.insert_many([{'x': i} for i in range(2)])
          >>> result.inserted_ids
          [ObjectId('54f113fffba522406c9cc20e'), ObjectId('54f113fffba522406c9cc20f')]
          >>> db.test.count_documents({})
          2

        :param documents: A iterable of documents to insert.
        :param ordered: If ``True`` (the default) documents will be
            inserted on the server serially, in the order provided. If an error
            occurs all remaining inserts are aborted. If ``False``, documents
            will be inserted on the server in arbitrary order, possibly in
            parallel, and all document inserts will be attempted.
        :param bypass_document_validation: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        :return: An instance of :class:`~pymongo.results.InsertManyResult`.

        .. seealso:: :ref:`writes-and-ids`

        .. note:: `bypass_document_validation` requires server version
          **>= 3.2**

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.2
          Added bypass_document_validation support

        .. versionadded:: 3.0
        """
        if (
            not isinstance(documents, abc.Iterable)
            or isinstance(documents, abc.Mapping)
            or not documents
        ):
            raise TypeError("documents must be a non-empty list")
        inserted_ids: list[ObjectId] = []

        def gen() -> Iterator[tuple[int, Mapping[str, Any]]]:
            """A generator that validates documents and handles _ids."""
            for document in documents:
                common.validate_is_document_type("document", document)
                if not isinstance(document, RawBSONDocument):
                    if "_id" not in document:
                        document["_id"] = ObjectId()  # type: ignore[index]
                    inserted_ids.append(document["_id"])
                yield (message._INSERT, document)

        write_concern = self._write_concern_for(session)
        blk = _Bulk(self, ordered, bypass_document_validation, comment=comment)
        blk.ops = list(gen())
        blk.execute(write_concern, session, _Op.INSERT)
        return InsertManyResult(inserted_ids, write_concern.acknowledged)

    def _update(
        self,
        conn: Connection,
        criteria: Mapping[str, Any],
        document: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        multi: bool = False,
        write_concern: Optional[WriteConcern] = None,
        op_id: Optional[int] = None,
        ordered: bool = True,
        bypass_doc_val: Optional[bool] = False,
        collation: Optional[_CollationIn] = None,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        retryable_write: bool = False,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> Optional[Mapping[str, Any]]:
        """Internal update / replace helper."""
        validate_boolean("upsert", upsert)
        collation = validate_collation_or_none(collation)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        update_doc: dict[str, Any] = {
            "q": criteria,
            "u": document,
            "multi": multi,
            "upsert": upsert,
        }
        if collation is not None:
            if not acknowledged:
                raise ConfigurationError("Collation is unsupported for unacknowledged writes.")
            else:
                update_doc["collation"] = collation
        if array_filters is not None:
            if not acknowledged:
                raise ConfigurationError("arrayFilters is unsupported for unacknowledged writes.")
            else:
                update_doc["arrayFilters"] = array_filters
        if hint is not None:
            if not acknowledged and conn.max_wire_version < 8:
                raise ConfigurationError(
                    "Must be connected to MongoDB 4.2+ to use hint on unacknowledged update commands."
                )
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)
            update_doc["hint"] = hint
        command = {"update": self.name, "ordered": ordered, "updates": [update_doc]}
        if let is not None:
            common.validate_is_mapping("let", let)
            command["let"] = let

        if comment is not None:
            command["comment"] = comment
        # Update command.
        if bypass_doc_val:
            command["bypassDocumentValidation"] = True

        # The command result has to be published for APM unmodified
        # so we make a shallow copy here before adding updatedExisting.
        result = conn.command(
            self.__database.name,
            command,
            write_concern=write_concern,
            codec_options=self.__write_response_codec_options,
            session=session,
            client=self.__database.client,
            retryable_write=retryable_write,
        ).copy()
        _check_write_command_response(result)
        # Add the updatedExisting field for compatibility.
        if result.get("n") and "upserted" not in result:
            result["updatedExisting"] = True
        else:
            result["updatedExisting"] = False
            # MongoDB >= 2.6.0 returns the upsert _id in an array
            # element. Break it out for backward compatibility.
            if "upserted" in result:
                result["upserted"] = result["upserted"][0]["_id"]

        if not acknowledged:
            return None
        return result

    def _update_retryable(
        self,
        criteria: Mapping[str, Any],
        document: Union[Mapping[str, Any], _Pipeline],
        operation: str,
        upsert: bool = False,
        multi: bool = False,
        write_concern: Optional[WriteConcern] = None,
        op_id: Optional[int] = None,
        ordered: bool = True,
        bypass_doc_val: Optional[bool] = False,
        collation: Optional[_CollationIn] = None,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> Optional[Mapping[str, Any]]:
        """Internal update / replace helper."""

        def _update(
            session: Optional[ClientSession], conn: Connection, retryable_write: bool
        ) -> Optional[Mapping[str, Any]]:
            return self._update(
                conn,
                criteria,
                document,
                upsert=upsert,
                multi=multi,
                write_concern=write_concern,
                op_id=op_id,
                ordered=ordered,
                bypass_doc_val=bypass_doc_val,
                collation=collation,
                array_filters=array_filters,
                hint=hint,
                session=session,
                retryable_write=retryable_write,
                let=let,
                comment=comment,
            )

        return self.__database.client._retryable_write(
            (write_concern or self.write_concern).acknowledged and not multi,
            _update,
            session,
            operation,
        )

    def replace_one(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> UpdateResult:
        """Replace a single document matching the filter.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}
          >>> result = db.test.replace_one({'x': 1}, {'y': 1})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': ObjectId('54f4c5befba5220aa4d6dee7')}

        The *upsert* option can be used to insert a new document if a matching
        document does not exist.

          >>> result = db.test.replace_one({'x': 1}, {'x': 1}, True)
          >>> result.matched_count
          0
          >>> result.modified_count
          0
          >>> result.upserted_id
          ObjectId('54f11e5c8891e756a6e1abd4')
          >>> db.test.find_one({'x': 1})
          {'x': 1, '_id': ObjectId('54f11e5c8891e756a6e1abd4')}

        :param filter: A query that matches the document to replace.
        :param replacement: The new document.
        :param upsert: If ``True``, perform an insert if no documents
            match the filter.
        :param bypass_document_validation: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.
        :return: - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionchanged:: 3.2
          Added bypass_document_validation support.

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_replace(replacement)
        if let is not None:
            common.validate_is_mapping("let", let)
        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter,
                replacement,
                _Op.UPDATE,
                upsert,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation,
                hint=hint,
                session=session,
                let=let,
                comment=comment,
            ),
            write_concern.acknowledged,
        )

    def update_one(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        bypass_document_validation: bool = False,
        collation: Optional[_CollationIn] = None,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> UpdateResult:
        """Update a single document matching the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = db.test.update_one({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          1
          >>> result.modified_count
          1
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        If ``upsert=True`` and no documents match the filter, create a
        new document based on the filter criteria and update modifications.

          >>> result = db.test.update_one({'x': -10}, {'$inc': {'x': 3}}, upsert=True)
          >>> result.matched_count
          0
          >>> result.modified_count
          0
          >>> result.upserted_id
          ObjectId('626a678eeaa80587d4bb3fb7')
          >>> db.test.find_one(result.upserted_id)
          {'_id': ObjectId('626a678eeaa80587d4bb3fb7'), 'x': -7}

        :param filter: A query that matches the document to update.
        :param update: The modifications to apply.
        :param upsert: If ``True``, perform an insert if no documents
            match the filter.
        :param bypass_document_validation: (optional) If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param array_filters: A list of filters specifying which
            array elements an update should apply.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.

        :return: - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the ``update``.
        .. versionchanged:: 3.6
           Added the ``array_filters`` and ``session`` parameters.
        .. versionchanged:: 3.4
          Added the ``collation`` option.
        .. versionchanged:: 3.2
          Added ``bypass_document_validation`` support.

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_update(update)
        common.validate_list_or_none("array_filters", array_filters)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter,
                update,
                _Op.UPDATE,
                upsert,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation,
                array_filters=array_filters,
                hint=hint,
                session=session,
                let=let,
                comment=comment,
            ),
            write_concern.acknowledged,
        )

    def update_many(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        bypass_document_validation: Optional[bool] = None,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> UpdateResult:
        """Update one or more documents that match the filter.

          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> result = db.test.update_many({'x': 1}, {'$inc': {'x': 3}})
          >>> result.matched_count
          3
          >>> result.modified_count
          3
          >>> for doc in db.test.find():
          ...     print(doc)
          ...
          {'x': 4, '_id': 0}
          {'x': 4, '_id': 1}
          {'x': 4, '_id': 2}

        :param filter: A query that matches the documents to update.
        :param update: The modifications to apply.
        :param upsert: If ``True``, perform an insert if no documents
            match the filter.
        :param bypass_document_validation: If ``True``, allows the
            write to opt-out of document level validation. Default is
            ``False``.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param array_filters: A list of filters specifying which
            array elements an update should apply.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.

        :return: - An instance of :class:`~pymongo.results.UpdateResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added ``array_filters`` and ``session`` parameters.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionchanged:: 3.2
          Added bypass_document_validation support.

        .. versionadded:: 3.0
        """
        common.validate_is_mapping("filter", filter)
        common.validate_ok_for_update(update)
        common.validate_list_or_none("array_filters", array_filters)

        write_concern = self._write_concern_for(session)
        return UpdateResult(
            self._update_retryable(
                filter,
                update,
                _Op.UPDATE,
                upsert,
                multi=True,
                write_concern=write_concern,
                bypass_doc_val=bypass_document_validation,
                collation=collation,
                array_filters=array_filters,
                hint=hint,
                session=session,
                let=let,
                comment=comment,
            ),
            write_concern.acknowledged,
        )

    def drop(
        self,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        encrypted_fields: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Alias for :meth:`~pymongo.database.Database.drop_collection`.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param encrypted_fields: **(BETA)** Document that describes the encrypted fields for
            Queryable Encryption.

        The following two calls are equivalent:

          >>> db.foo.drop()
          >>> db.drop_collection("foo")

        .. versionchanged:: 4.2
           Added ``encrypted_fields`` parameter.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.7
           :meth:`drop` now respects this :class:`Collection`'s :attr:`write_concern`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        dbo = self.__database.client.get_database(
            self.__database.name,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )
        dbo.drop_collection(
            self.__name, session=session, comment=comment, encrypted_fields=encrypted_fields
        )

    def _delete(
        self,
        conn: Connection,
        criteria: Mapping[str, Any],
        multi: bool,
        write_concern: Optional[WriteConcern] = None,
        op_id: Optional[int] = None,
        ordered: bool = True,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        retryable_write: bool = False,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> Mapping[str, Any]:
        """Internal delete helper."""
        common.validate_is_mapping("filter", criteria)
        write_concern = write_concern or self.write_concern
        acknowledged = write_concern.acknowledged
        delete_doc = {"q": criteria, "limit": int(not multi)}
        collation = validate_collation_or_none(collation)
        if collation is not None:
            if not acknowledged:
                raise ConfigurationError("Collation is unsupported for unacknowledged writes.")
            else:
                delete_doc["collation"] = collation
        if hint is not None:
            if not acknowledged and conn.max_wire_version < 9:
                raise ConfigurationError(
                    "Must be connected to MongoDB 4.4+ to use hint on unacknowledged delete commands."
                )
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)
            delete_doc["hint"] = hint
        command = {"delete": self.name, "ordered": ordered, "deletes": [delete_doc]}

        if let is not None:
            common.validate_is_document_type("let", let)
            command["let"] = let

        if comment is not None:
            command["comment"] = comment

        # Delete command.
        result = conn.command(
            self.__database.name,
            command,
            write_concern=write_concern,
            codec_options=self.__write_response_codec_options,
            session=session,
            client=self.__database.client,
            retryable_write=retryable_write,
        )
        _check_write_command_response(result)
        return result

    def _delete_retryable(
        self,
        criteria: Mapping[str, Any],
        multi: bool,
        write_concern: Optional[WriteConcern] = None,
        op_id: Optional[int] = None,
        ordered: bool = True,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> Mapping[str, Any]:
        """Internal delete helper."""

        def _delete(
            session: Optional[ClientSession], conn: Connection, retryable_write: bool
        ) -> Mapping[str, Any]:
            return self._delete(
                conn,
                criteria,
                multi,
                write_concern=write_concern,
                op_id=op_id,
                ordered=ordered,
                collation=collation,
                hint=hint,
                session=session,
                retryable_write=retryable_write,
                let=let,
                comment=comment,
            )

        return self.__database.client._retryable_write(
            (write_concern or self.write_concern).acknowledged and not multi,
            _delete,
            session,
            operation=_Op.DELETE,
        )

    def delete_one(
        self,
        filter: Mapping[str, Any],
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> DeleteResult:
        """Delete a single document matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_one({'x': 1})
          >>> result.deleted_count
          1
          >>> db.test.count_documents({'x': 1})
          2

        :param filter: A query that matches the document to delete.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.

        :return: - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionadded:: 3.0
        """
        write_concern = self._write_concern_for(session)
        return DeleteResult(
            self._delete_retryable(
                filter,
                False,
                write_concern=write_concern,
                collation=collation,
                hint=hint,
                session=session,
                let=let,
                comment=comment,
            ),
            write_concern.acknowledged,
        )

    def delete_many(
        self,
        filter: Mapping[str, Any],
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
    ) -> DeleteResult:
        """Delete one or more documents matching the filter.

          >>> db.test.count_documents({'x': 1})
          3
          >>> result = db.test.delete_many({'x': 1})
          >>> result.deleted_count
          3
          >>> db.test.count_documents({'x': 1})
          0

        :param filter: A query that matches the documents to delete.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.

        :return: - An instance of :class:`~pymongo.results.DeleteResult`.

        .. versionchanged:: 4.1
           Added ``let`` parameter.
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
          Added the `collation` option.
        .. versionadded:: 3.0
        """
        write_concern = self._write_concern_for(session)
        return DeleteResult(
            self._delete_retryable(
                filter,
                True,
                write_concern=write_concern,
                collation=collation,
                hint=hint,
                session=session,
                let=let,
                comment=comment,
            ),
            write_concern.acknowledged,
        )

    def find_one(
        self, filter: Optional[Any] = None, *args: Any, **kwargs: Any
    ) -> Optional[_DocumentType]:
        """Get a single document from the database.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single document, or ``None`` if no matching
        document is found.

        The :meth:`find_one` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :param filter: a dictionary specifying
            the query to be performed OR any other type to be used as
            the value for a query for ``"_id"``.

        :param args: any additional positional arguments
            are the same as the arguments to :meth:`find`.

        :param kwargs: any additional keyword arguments
            are the same as the arguments to :meth:`find`.

            :: code-block: python

              >>> collection.find_one(max_time_ms=100)

        """
        if filter is not None and not isinstance(filter, abc.Mapping):
            filter = {"_id": filter}
        cursor = self.find(filter, *args, **kwargs)
        for result in cursor.limit(-1):
            return result
        return None

    def find(self, *args: Any, **kwargs: Any) -> Cursor[_DocumentType]:
        """Query the database.

        The `filter` argument is a query document that all results
        must match. For example:

        >>> db.test.find({"hello": "world"})

        only matches documents that have a key "hello" with value
        "world".  Matches can have other keys *in addition* to
        "hello". The `projection` argument is used to specify a subset
        of fields that should be included in the result documents. By
        limiting results to a certain subset of fields you can cut
        down on network traffic and decoding time.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~pymongo.cursor.Cursor` corresponding to this query.

        The :meth:`find` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :param filter: A query document that selects which documents
            to include in the result set. Can be an empty document to include
            all documents.
        :param projection: a list of field names that should be
            returned in the result set or a dict specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param skip: the number of documents to omit (from
            the start of the result set) when returning the results
        :param limit: the maximum number of results to
            return. A limit of 0 (the default) is equivalent to setting no
            limit.
        :param no_cursor_timeout: if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
        :param cursor_type: the type of cursor to return. The valid
            options are defined by :class:`~pymongo.cursor.CursorType`:

            - :attr:`~pymongo.cursor.CursorType.NON_TAILABLE` - the result of
              this find call will return a standard cursor over the result set.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE` - the result of this
              find call will be a tailable cursor - tailable cursors are only
              for use with capped collections. They are not closed when the
              last data is retrieved but are kept open and the cursor location
              marks the final document position. If more data is received
              iteration of the cursor will continue from the last document
              received. For details, see the `tailable cursor documentation
              <https://www.mongodb.com/docs/manual/core/tailable-cursors/>`_.
            - :attr:`~pymongo.cursor.CursorType.TAILABLE_AWAIT` - the result
              of this find call will be a tailable cursor with the await flag
              set. The server will wait for a few seconds after returning the
              full result set so that it can capture and return additional data
              added during the query.
            - :attr:`~pymongo.cursor.CursorType.EXHAUST` - the result of this
              find call will be an exhaust cursor. MongoDB will stream batched
              results to the client without waiting for the client to request
              each batch, reducing latency. See notes on compatibility below.

        :param sort: a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.
        :param allow_partial_results: if True, mongos will return
            partial results if some shards are down instead of returning an
            error.
        :param oplog_replay: **DEPRECATED** - if True, set the
            oplogReplay query flag. Default: False.
        :param batch_size: Limits the number of documents returned in
            a single batch.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param return_key: If True, return only the index keys in
            each document.
        :param show_record_id: If True, adds a field ``$recordId`` in
            each document with the storage engine's internal record identifier.
        :param snapshot: **DEPRECATED** - If True, prevents the
            cursor from returning a document more than once because of an
            intervening write operation.
        :param hint: An index, in the same format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.hint` on the cursor to tell Mongo the
            proper index to use for the query.
        :param max_time_ms: Specifies a time limit for a query
            operation. If the specified time is exceeded, the operation will be
            aborted and :exc:`~pymongo.errors.ExecutionTimeout` is raised. Pass
            this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_time_ms` on the cursor.
        :param max_scan: **DEPRECATED** - The maximum number of
            documents to scan. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max_scan` on the cursor.
        :param min: A list of field, limit pairs specifying the
            inclusive lower bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.min` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
        :param max: A list of field, limit pairs specifying the
            exclusive upper bound for all keys of a specific index in order.
            Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.max` on the cursor. ``hint`` must
            also be passed to ensure the query utilizes the correct index.
        :param comment: A string to attach to the query to help
            interpret and trace the operation in the server logs and in profile
            data. Pass this as an alternative to calling
            :meth:`~pymongo.cursor.Cursor.comment` on the cursor.
        :param allow_disk_use: if True, MongoDB may use temporary
            disk files to store data exceeding the system memory limit while
            processing a blocking sort operation. The option has no effect if
            MongoDB can satisfy the specified sort using an index, or if the
            blocking sort requires less memory than the 100 MiB limit. This
            option is only supported on MongoDB 4.4 and above.

        .. note:: There are a number of caveats to using
          :attr:`~pymongo.cursor.CursorType.EXHAUST` as cursor_type:

          - The `limit` option can not be used with an exhaust cursor.

          - Exhaust cursors are not supported by mongos and can not be
            used with a sharded cluster.

          - A :class:`~pymongo.cursor.Cursor` instance created with the
            :attr:`~pymongo.cursor.CursorType.EXHAUST` cursor_type requires an
            exclusive :class:`~socket.socket` connection to MongoDB. If the
            :class:`~pymongo.cursor.Cursor` is discarded without being
            completely iterated the underlying :class:`~socket.socket`
            connection will be closed and discarded without being returned to
            the connection pool.

        .. versionchanged:: 4.0
           Removed the ``modifiers`` option.
           Empty projections (eg {} or []) are passed to the server as-is,
           rather than the previous behavior which substituted in a
           projection of ``{"_id": 1}``. This means that an empty projection
           will now return the entire document, not just the ``"_id"`` field.

        .. versionchanged:: 3.11
           Added the ``allow_disk_use`` option.
           Deprecated the ``oplog_replay`` option. Support for this option is
           deprecated in MongoDB 4.4. The query engine now automatically
           optimizes queries against the oplog without requiring this
           option to be set.

        .. versionchanged:: 3.7
           Deprecated the ``snapshot`` option, which is deprecated in MongoDB
           3.6 and removed in MongoDB 4.0.
           Deprecated the ``max_scan`` option. Support for this option is
           deprecated in MongoDB 4.0. Use ``max_time_ms`` instead to limit
           server-side execution time.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.5
           Added the options ``return_key``, ``show_record_id``, ``snapshot``,
           ``hint``, ``max_time_ms``, ``max_scan``, ``min``, ``max``, and
           ``comment``.
           Deprecated the ``modifiers`` option.

        .. versionchanged:: 3.4
           Added support for the ``collation`` option.

        .. versionchanged:: 3.0
           Changed the parameter names ``spec``, ``fields``, ``timeout``, and
           ``partial`` to ``filter``, ``projection``, ``no_cursor_timeout``,
           and ``allow_partial_results`` respectively.
           Added the ``cursor_type``, ``oplog_replay``, and ``modifiers``
           options.
           Removed the ``network_timeout``, ``read_preference``, ``tag_sets``,
           ``secondary_acceptable_latency_ms``, ``max_scan``, ``snapshot``,
           ``tailable``, ``await_data``, ``exhaust``, ``as_class``, and
           slave_okay parameters.
           Removed ``compile_re`` option: PyMongo now always
           represents BSON regular expressions as :class:`~bson.regex.Regex`
           objects. Use :meth:`~bson.regex.Regex.try_compile` to attempt to
           convert from a BSON regular expression to a Python regular
           expression object.
           Soft deprecated the ``manipulate`` option.

        .. seealso:: The MongoDB documentation on `find <https://dochub.mongodb.org/core/find>`_.
        """
        return Cursor(self, *args, **kwargs)

    def find_raw_batches(self, *args: Any, **kwargs: Any) -> RawBatchCursor[_DocumentType]:
        """Query the database and retrieve batches of raw BSON.

        Similar to the :meth:`find` method but returns a
        :class:`~pymongo.cursor.RawBatchCursor`.

        This example demonstrates how to work with raw batches, but in practice
        raw batches should be passed to an external library that can decode
        BSON into another data type, rather than used with PyMongo's
        :mod:`bson` module.

          >>> import bson
          >>> cursor = db.test.find_raw_batches()
          >>> for batch in cursor:
          ...     print(bson.decode_all(batch))

        .. note:: find_raw_batches does not support auto encryption.

        .. versionchanged:: 3.12
           Instead of ignoring the user-specified read concern, this method
           now sends it to the server when connected to MongoDB 3.6+.

           Added session support.

        .. versionadded:: 3.6
        """
        # OP_MSG is required to support encryption.
        if self.__database.client._encrypter:
            raise InvalidOperation("find_raw_batches does not support auto encryption")
        return RawBatchCursor(self, *args, **kwargs)

    def _count_cmd(
        self,
        session: Optional[ClientSession],
        conn: Connection,
        read_preference: Optional[_ServerMode],
        cmd: dict[str, Any],
        collation: Optional[Collation],
    ) -> int:
        """Internal count command helper."""
        # XXX: "ns missing" checks can be removed when we drop support for
        # MongoDB 3.0, see SERVER-17051.
        res = self._command(
            conn,
            cmd,
            read_preference=read_preference,
            allowable_errors=["ns missing"],
            codec_options=self.__write_response_codec_options,
            read_concern=self.read_concern,
            collation=collation,
            session=session,
        )
        if res.get("errmsg", "") == "ns missing":
            return 0
        return int(res["n"])

    def _aggregate_one_result(
        self,
        conn: Connection,
        read_preference: Optional[_ServerMode],
        cmd: dict[str, Any],
        collation: Optional[_CollationIn],
        session: Optional[ClientSession],
    ) -> Optional[Mapping[str, Any]]:
        """Internal helper to run an aggregate that returns a single result."""
        result = self._command(
            conn,
            cmd,
            read_preference,
            allowable_errors=[26],  # Ignore NamespaceNotFound.
            codec_options=self.__write_response_codec_options,
            read_concern=self.read_concern,
            collation=collation,
            session=session,
        )
        # cursor will not be present for NamespaceNotFound errors.
        if "cursor" not in result:
            return None
        batch = result["cursor"]["firstBatch"]
        return batch[0] if batch else None

    def estimated_document_count(self, comment: Optional[Any] = None, **kwargs: Any) -> int:
        """Get an estimate of the number of documents in this collection using
        collection metadata.

        The :meth:`estimated_document_count` method is **not** supported in a
        transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.

        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: See list of options above.

        .. versionchanged:: 4.2
           This method now always uses the `count`_ command. Due to an oversight in versions
           5.0.0-5.0.8 of MongoDB, the count command was not included in V1 of the
           :ref:`versioned-api-ref`. Users of the Stable API with estimated_document_count are
           recommended to upgrade their server version to 5.0.9+ or set
           :attr:`pymongo.server_api.ServerApi.strict` to ``False`` to avoid encountering errors.

        .. versionadded:: 3.7
        .. _count: https://mongodb.com/docs/manual/reference/command/count/
        """
        if "session" in kwargs:
            raise ConfigurationError("estimated_document_count does not support sessions")
        if comment is not None:
            kwargs["comment"] = comment

        def _cmd(
            session: Optional[ClientSession],
            _server: Server,
            conn: Connection,
            read_preference: Optional[_ServerMode],
        ) -> int:
            cmd: dict[str, Any] = {"count": self.__name}
            cmd.update(kwargs)
            return self._count_cmd(session, conn, read_preference, cmd, collation=None)

        return self._retryable_non_cursor_read(_cmd, None, operation=_Op.COUNT)

    def count_documents(
        self,
        filter: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> int:
        """Count the number of documents in this collection.

        .. note:: For a fast count of the total documents in a collection see
           :meth:`estimated_document_count`.

        The :meth:`count_documents` method is supported in a transaction.

        All optional parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `skip` (int): The number of matching documents to skip before
            returning results.
          - `limit` (int): The maximum number of documents to count. Must be
            a positive integer. If not provided, no limit is imposed.
          - `maxTimeMS` (int): The maximum amount of time to allow this
            operation to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (string or list of tuples): The index to use. Specify either
            the index name as a string or the index specification as a list of
            tuples (e.g. [('a', pymongo.ASCENDING), ('b', pymongo.ASCENDING)]).

        The :meth:`count_documents` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        .. note:: When migrating from :meth:`count` to :meth:`count_documents`
           the following query operators must be replaced:

           +-------------+-------------------------------------+
           | Operator    | Replacement                         |
           +=============+=====================================+
           | $where      | `$expr`_                            |
           +-------------+-------------------------------------+
           | $near       | `$geoWithin`_ with `$center`_       |
           +-------------+-------------------------------------+
           | $nearSphere | `$geoWithin`_ with `$centerSphere`_ |
           +-------------+-------------------------------------+

        :param filter: A query document that selects which documents
            to count in the collection. Can be an empty document to count all
            documents.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: See list of options above.


        .. versionadded:: 3.7

        .. _$expr: https://mongodb.com/docs/manual/reference/operator/query/expr/
        .. _$geoWithin: https://mongodb.com/docs/manual/reference/operator/query/geoWithin/
        .. _$center: https://mongodb.com/docs/manual/reference/operator/query/center/
        .. _$centerSphere: https://mongodb.com/docs/manual/reference/operator/query/centerSphere/
        """
        pipeline = [{"$match": filter}]
        if "skip" in kwargs:
            pipeline.append({"$skip": kwargs.pop("skip")})
        if "limit" in kwargs:
            pipeline.append({"$limit": kwargs.pop("limit")})
        if comment is not None:
            kwargs["comment"] = comment
        pipeline.append({"$group": {"_id": 1, "n": {"$sum": 1}}})
        cmd = {"aggregate": self.__name, "pipeline": pipeline, "cursor": {}}
        if "hint" in kwargs and not isinstance(kwargs["hint"], str):
            kwargs["hint"] = helpers._index_document(kwargs["hint"])
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        cmd.update(kwargs)

        def _cmd(
            session: Optional[ClientSession],
            _server: Server,
            conn: Connection,
            read_preference: Optional[_ServerMode],
        ) -> int:
            result = self._aggregate_one_result(conn, read_preference, cmd, collation, session)
            if not result:
                return 0
            return result["n"]

        return self._retryable_non_cursor_read(_cmd, session, _Op.COUNT)

    def _retryable_non_cursor_read(
        self,
        func: Callable[[Optional[ClientSession], Server, Connection, Optional[_ServerMode]], T],
        session: Optional[ClientSession],
        operation: str,
    ) -> T:
        """Non-cursor read helper to handle implicit session creation."""
        client = self.__database.client
        with client._tmp_session(session) as s:
            return client._retryable_read(func, self._read_preference_for(s), s, operation)

    def create_indexes(
        self,
        indexes: Sequence[IndexModel],
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> list[str]:
        """Create one or more indexes on this collection.

          >>> from pymongo import IndexModel, ASCENDING, DESCENDING
          >>> index1 = IndexModel([("hello", DESCENDING),
          ...                      ("world", ASCENDING)], name="hello_world")
          >>> index2 = IndexModel([("goodbye", DESCENDING)])
          >>> db.test.create_indexes([index1, index2])
          ["hello_world", "goodbye_-1"]

        :param indexes: A list of :class:`~pymongo.operations.IndexModel`
            instances.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.




        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.
        .. versionadded:: 3.0

        .. _createIndexes: https://mongodb.com/docs/manual/reference/command/createIndexes/
        """
        common.validate_list("indexes", indexes)
        if comment is not None:
            kwargs["comment"] = comment
        return self.__create_indexes(indexes, session, **kwargs)

    @_csot.apply
    def __create_indexes(
        self, indexes: Sequence[IndexModel], session: Optional[ClientSession], **kwargs: Any
    ) -> list[str]:
        """Internal createIndexes helper.

        :param indexes: A list of :class:`~pymongo.operations.IndexModel`
            instances.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param kwargs: optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.
        """
        names = []
        with self._conn_for_writes(session, operation=_Op.CREATE_INDEXES) as conn:
            supports_quorum = conn.max_wire_version >= 9

            def gen_indexes() -> Iterator[Mapping[str, Any]]:
                for index in indexes:
                    if not isinstance(index, IndexModel):
                        raise TypeError(
                            f"{index!r} is not an instance of pymongo.operations.IndexModel"
                        )
                    document = index.document
                    names.append(document["name"])
                    yield document

            cmd = {"createIndexes": self.name, "indexes": list(gen_indexes())}
            cmd.update(kwargs)
            if "commitQuorum" in kwargs and not supports_quorum:
                raise ConfigurationError(
                    "Must be connected to MongoDB 4.4+ to use the "
                    "commitQuorum option for createIndexes"
                )

            self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
                write_concern=self._write_concern_for(session),
                session=session,
            )
        return names

    def create_index(
        self,
        keys: _IndexKeyHint,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> str:
        """Creates an index on this collection.

        Takes either a single key or a list containing (key, direction) pairs
        or keys.  If no direction is given, :data:`~pymongo.ASCENDING` will
        be assumed.
        The key(s) must be an instance of :class:`str` and the direction(s) must
        be one of (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOSPHERE`,
        :data:`~pymongo.HASHED`, :data:`~pymongo.TEXT`).

        To create a single key ascending index on the key ``'mike'`` we just
        use a string argument::

          >>> my_collection.create_index("mike")

        For a compound index on ``'mike'`` descending and ``'eliot'``
        ascending we need to use a list of tuples::

          >>> my_collection.create_index([("mike", pymongo.DESCENDING),
          ...                             "eliot"])

        All optional index creation parameters should be passed as
        keyword arguments to this method. For example::

          >>> my_collection.create_index([("mike", pymongo.DESCENDING)],
          ...                            background=True)

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated.
          - `unique`: if ``True``, creates a uniqueness constraint on the
            index.
          - `background`: if ``True``, this index should be created in the
            background.
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field.
          - `bucketSize`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `partialFilterExpression`: A document that specifies a filter for
            a partial index.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `wildcardProjection`: Allows users to include or exclude specific
            field paths from a `wildcard index`_ using the {"$**" : 1} key
            pattern. Requires MongoDB >= 4.2.
          - `hidden`: if ``True``, this index will be hidden from the query
            planner and will not be evaluated as part of query plan
            selection. Requires MongoDB >= 4.4.

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. warning:: `dropDups` is not supported by MongoDB 3.0 or newer. The
          option is silently ignored by the server and unique index builds
          using the option will fail if a duplicate value is detected.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.

        :param keys: a single key or a list of (key, direction)
            pairs specifying the index to create
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: any additional index creation
            options (see the above list) should be passed as keyword
            arguments.

        .. versionchanged:: 4.4
           Allow passing a list containing (key, direction) pairs
           or keys for the ``keys`` parameter.
        .. versionchanged:: 4.1
           Added ``comment`` parameter.
        .. versionchanged:: 3.11
           Added the ``hidden`` option.
        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for passing maxTimeMS
           in kwargs.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.2
           Added partialFilterExpression to support partial indexes.
        .. versionchanged:: 3.0
           Renamed `key_or_list` to `keys`. Removed the `cache_for` option.
           :meth:`create_index` no longer caches index names. Removed support
           for the drop_dups and bucket_size aliases.

        .. seealso:: The MongoDB documentation on `indexes <https://dochub.mongodb.org/core/indexes>`_.

        .. _wildcard index: https://dochub.mongodb.org/core/index-wildcard/
        """
        cmd_options = {}
        if "maxTimeMS" in kwargs:
            cmd_options["maxTimeMS"] = kwargs.pop("maxTimeMS")
        if comment is not None:
            cmd_options["comment"] = comment
        index = IndexModel(keys, **kwargs)
        return self.__create_indexes([index], session, **cmd_options)[0]

    def drop_indexes(
        self,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """Drops all indexes on this collection.

        Can be used on non-existent collections or collections with no indexes.
        Raises OperationFailure on an error.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.

        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.
        """
        if comment is not None:
            kwargs["comment"] = comment
        self.drop_index("*", session=session, **kwargs)

    @_csot.apply
    def drop_index(
        self,
        index_or_name: _IndexKeyHint,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """Drops the specified index on this collection.

        Can be used on non-existent collections or collections with no
        indexes.  Raises OperationFailure on an error (e.g. trying to
        drop an index that does not exist). `index_or_name`
        can be either an index name (as returned by `create_index`),
        or an index specifier (as passed to `create_index`). An index
        specifier should be a list of (key, direction) pairs. Raises
        TypeError if index is not an instance of (str, unicode, list).

        .. warning::

          if a custom name was used on index creation (by
          passing the `name` parameter to :meth:`create_index`) the index
          **must** be dropped by name.

        :param index_or_name: index (or name of index) to drop
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the createIndexes
            command (like maxTimeMS) can be passed as keyword arguments.



        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.


        .. versionchanged:: 3.6
           Added ``session`` parameter. Added support for arbitrary keyword
           arguments.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = index_or_name
        if isinstance(index_or_name, list):
            name = helpers._gen_index_name(index_or_name)

        if not isinstance(name, str):
            raise TypeError("index_or_name must be an instance of str or list")

        cmd = {"dropIndexes": self.__name, "index": name}
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment
        with self._conn_for_writes(session, operation=_Op.DROP_INDEXES) as conn:
            self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                allowable_errors=["ns not found", 26],
                write_concern=self._write_concern_for(session),
                session=session,
            )

    def list_indexes(
        self,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> CommandCursor[MutableMapping[str, Any]]:
        """Get a cursor over the index documents for this collection.

          >>> for index in db.test.list_indexes():
          ...     print(index)
          ...
          SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')])

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        :return: An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionadded:: 3.0
        """
        codec_options: CodecOptions = CodecOptions(SON)
        coll = cast(
            Collection[MutableMapping[str, Any]],
            self.with_options(codec_options=codec_options, read_preference=ReadPreference.PRIMARY),
        )
        read_pref = (session and session._txn_read_preference()) or ReadPreference.PRIMARY
        explicit_session = session is not None

        def _cmd(
            session: Optional[ClientSession],
            _server: Server,
            conn: Connection,
            read_preference: _ServerMode,
        ) -> CommandCursor[MutableMapping[str, Any]]:
            cmd = {"listIndexes": self.__name, "cursor": {}}
            if comment is not None:
                cmd["comment"] = comment

            try:
                cursor = self._command(conn, cmd, read_preference, codec_options, session=session)[
                    "cursor"
                ]
            except OperationFailure as exc:
                # Ignore NamespaceNotFound errors to match the behavior
                # of reading from *.system.indexes.
                if exc.code != 26:
                    raise
                cursor = {"id": 0, "firstBatch": []}
            cmd_cursor = CommandCursor(
                coll,
                cursor,
                conn.address,
                session=session,
                explicit_session=explicit_session,
                comment=cmd.get("comment"),
            )
            cmd_cursor._maybe_pin_connection(conn)
            return cmd_cursor

        with self.__database.client._tmp_session(session, False) as s:
            return self.__database.client._retryable_read(
                _cmd, read_pref, s, operation=_Op.LIST_INDEXES
            )

    def index_information(
        self,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> MutableMapping[str, Any]:
        """Get information on this collection's indexes.

        Returns a dictionary where the keys are index names (as
        returned by create_index()) and the values are dictionaries
        containing information about each index. The dictionary is
        guaranteed to contain at least a single key, ``"key"`` which
        is a list of (key, direction) pairs specifying the index (as
        passed to create_index()). It will also contain any other
        metadata about the indexes, except for the ``"ns"`` and
        ``"name"`` keys, which are cleaned. Example output might look
        like this:

        >>> db.test.create_index("x", unique=True)
        'x_1'
        >>> db.test.index_information()
        {'_id_': {'key': [('_id', 1)]},
         'x_1': {'unique': True, 'key': [('x', 1)]}}

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        cursor = self.list_indexes(session=session, comment=comment)
        info = {}
        for index in cursor:
            index["key"] = list(index["key"].items())
            index = dict(index)  # noqa: PLW2901
            info[index.pop("name")] = index
        return info

    def list_search_indexes(
        self,
        name: Optional[str] = None,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[Mapping[str, Any]]:
        """Return a cursor over search indexes for the current collection.

        :param name: If given, the name of the index to search
            for.  Only indexes with matching index names will be returned.
            If not given, all search indexes for the current collection
            will be returned.
        :param session: a :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        :return: A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. note:: requires a MongoDB server version 7.0+ Atlas cluster.

        .. versionadded:: 4.5
        """
        if name is None:
            pipeline: _Pipeline = [{"$listSearchIndexes": {}}]
        else:
            pipeline = [{"$listSearchIndexes": {"name": name}}]

        coll = self.with_options(
            codec_options=DEFAULT_CODEC_OPTIONS,
            read_preference=ReadPreference.PRIMARY,
            write_concern=DEFAULT_WRITE_CONCERN,
            read_concern=DEFAULT_READ_CONCERN,
        )
        cmd = _CollectionAggregationCommand(
            coll,
            CommandCursor,
            pipeline,
            kwargs,
            explicit_session=session is not None,
            comment=comment,
            user_fields={"cursor": {"firstBatch": 1}},
        )

        return self.__database.client._retryable_read(
            cmd.get_cursor,
            cmd.get_read_preference(session),  # type: ignore[arg-type]
            session,
            retryable=not cmd._performs_write,
            operation=_Op.LIST_SEARCH_INDEX,
        )

    def create_search_index(
        self,
        model: Union[Mapping[str, Any], SearchIndexModel],
        session: Optional[ClientSession] = None,
        comment: Any = None,
        **kwargs: Any,
    ) -> str:
        """Create a single search index for the current collection.

        :param model: The model for the new search index.
            It can be given as a :class:`~pymongo.operations.SearchIndexModel`
            instance or a dictionary with a model "definition"  and optional
            "name".
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the createSearchIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        :return: The name of the new search index.

        .. note:: requires a MongoDB server version 7.0+ Atlas cluster.

        .. versionadded:: 4.5
        """
        if not isinstance(model, SearchIndexModel):
            model = SearchIndexModel(**model)
        return self.create_search_indexes([model], session, comment, **kwargs)[0]

    def create_search_indexes(
        self,
        models: list[SearchIndexModel],
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> list[str]:
        """Create multiple search indexes for the current collection.

        :param models: A list of :class:`~pymongo.operations.SearchIndexModel` instances.
        :param session: a :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the createSearchIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        :return: A list of the newly created search index names.

        .. note:: requires a MongoDB server version 7.0+ Atlas cluster.

        .. versionadded:: 4.5
        """
        if comment is not None:
            kwargs["comment"] = comment

        def gen_indexes() -> Iterator[Mapping[str, Any]]:
            for index in models:
                if not isinstance(index, SearchIndexModel):
                    raise TypeError(
                        f"{index!r} is not an instance of pymongo.operations.SearchIndexModel"
                    )
                yield index.document

        cmd = {"createSearchIndexes": self.name, "indexes": list(gen_indexes())}
        cmd.update(kwargs)

        with self._conn_for_writes(session, operation=_Op.CREATE_SEARCH_INDEXES) as conn:
            resp = self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
            )
            return [index["name"] for index in resp["indexesCreated"]]

    def drop_search_index(
        self,
        name: str,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """Delete a search index by index name.

        :param name: The name of the search index to be deleted.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the dropSearchIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: requires a MongoDB server version 7.0+ Atlas cluster.

        .. versionadded:: 4.5
        """
        cmd = {"dropSearchIndex": self.__name, "name": name}
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment
        with self._conn_for_writes(session, operation=_Op.DROP_SEARCH_INDEXES) as conn:
            self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                allowable_errors=["ns not found", 26],
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
            )

    def update_search_index(
        self,
        name: str,
        definition: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """Update a search index by replacing the existing index definition with the provided definition.

        :param name: The name of the search index to be updated.
        :param definition: The new search index definition.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: optional arguments to the updateSearchIndexes
            command (like maxTimeMS) can be passed as keyword arguments.

        .. note:: requires a MongoDB server version 7.0+ Atlas cluster.

        .. versionadded:: 4.5
        """
        cmd = {"updateSearchIndex": self.__name, "name": name, "definition": definition}
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment
        with self._conn_for_writes(session, operation=_Op.UPDATE_SEARCH_INDEX) as conn:
            self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                allowable_errors=["ns not found", 26],
                codec_options=_UNICODE_REPLACE_CODEC_OPTIONS,
            )

    def options(
        self,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
    ) -> MutableMapping[str, Any]:
        """Get the options set on this collection.

        Returns a dictionary of options and their values - see
        :meth:`~pymongo.database.Database.create_collection` for more
        information on the possible options. Returns an empty
        dictionary if the collection has not been created yet.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        dbo = self.__database.client.get_database(
            self.__database.name,
            self.codec_options,
            self.read_preference,
            self.write_concern,
            self.read_concern,
        )
        cursor = dbo.list_collections(
            session=session, filter={"name": self.__name}, comment=comment
        )

        result = None
        for doc in cursor:
            result = doc
            break

        if not result:
            return {}

        options = result.get("options", {})
        assert options is not None
        if "create" in options:
            del options["create"]

        return options

    @_csot.apply
    def _aggregate(
        self,
        aggregation_command: Type[_AggregationCommand],
        pipeline: _Pipeline,
        cursor_class: Type[CommandCursor],
        session: Optional[ClientSession],
        explicit_session: bool,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[_DocumentType]:
        if comment is not None:
            kwargs["comment"] = comment
        cmd = aggregation_command(
            self,
            cursor_class,
            pipeline,
            kwargs,
            explicit_session,
            let,
            user_fields={"cursor": {"firstBatch": 1}},
        )

        return self.__database.client._retryable_read(
            cmd.get_cursor,
            cmd.get_read_preference(session),  # type: ignore[arg-type]
            session,
            retryable=not cmd._performs_write,
            operation=_Op.AGGREGATE,
        )

    def aggregate(
        self,
        pipeline: _Pipeline,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[_DocumentType]:
        """Perform an aggregation using the aggregation framework on this
        collection.

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Collection`, except when ``$out`` or ``$merge`` are used on
        MongoDB <5.0, in which case
        :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY` is used.

        .. note:: This method does not support the 'explain' option. Please
           use `PyMongoExplain <https://pypi.org/project/pymongoexplain/>`_
           instead. An example is included in the :ref:`aggregate-examples`
           documentation.

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.

        :param pipeline: a list of aggregation pipeline stages
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: A dict of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. ``"$$var"``). This option is
            only supported on MongoDB >= 5.0.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: extra `aggregate command`_ parameters.

        All optional `aggregate command`_ parameters should be passed as
        keyword arguments to this method. Valid options include, but are not
        limited to:

          - `allowDiskUse` (bool): Enables writing to temporary files. When set
            to True, aggregation stages can write data to the _tmp subdirectory
            of the --dbpath directory. The default is False.
          - `maxTimeMS` (int): The maximum amount of time to allow the operation
            to run in milliseconds.
          - `batchSize` (int): The maximum number of documents to return per
            batch. Ignored if the connected mongod or mongos does not support
            returning aggregate results using a cursor.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.


        :return: A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.
           Added ``let`` parameter.
           Support $merge and $out executing on secondaries according to the
           collection's :attr:`read_preference`.
        .. versionchanged:: 4.0
           Removed the ``useCursor`` option.
        .. versionchanged:: 3.9
           Apply this collection's read concern to pipelines containing the
           `$out` stage when connected to MongoDB >= 4.2.
           Added support for the ``$merge`` pipeline stage.
           Aggregations that write always use read preference
           :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
        .. versionchanged:: 3.6
           Added the `session` parameter. Added the `maxAwaitTimeMS` option.
           Deprecated the `useCursor` option.
        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4. Support the `collation` option.
        .. versionchanged:: 3.0
           The :meth:`aggregate` method always returns a CommandCursor. The
           pipeline argument must be a list.

        .. seealso:: :doc:`/examples/aggregation`

        .. _aggregate command:
            https://mongodb.com/docs/manual/reference/command/aggregate
        """
        with self.__database.client._tmp_session(session, close=False) as s:
            return self._aggregate(
                _CollectionAggregationCommand,
                pipeline,
                CommandCursor,
                session=s,
                explicit_session=session is not None,
                let=let,
                comment=comment,
                **kwargs,
            )

    def aggregate_raw_batches(
        self,
        pipeline: _Pipeline,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> RawBatchCursor[_DocumentType]:
        """Perform an aggregation and retrieve batches of raw BSON.

        Similar to the :meth:`aggregate` method but returns a
        :class:`~pymongo.cursor.RawBatchCursor`.

        This example demonstrates how to work with raw batches, but in practice
        raw batches should be passed to an external library that can decode
        BSON into another data type, rather than used with PyMongo's
        :mod:`bson` module.

          >>> import bson
          >>> cursor = db.test.aggregate_raw_batches([
          ...     {'$project': {'x': {'$multiply': [2, '$x']}}}])
          >>> for batch in cursor:
          ...     print(bson.decode_all(batch))

        .. note:: aggregate_raw_batches does not support auto encryption.

        .. versionchanged:: 3.12
           Added session support.

        .. versionadded:: 3.6
        """
        # OP_MSG is required to support encryption.
        if self.__database.client._encrypter:
            raise InvalidOperation("aggregate_raw_batches does not support auto encryption")
        if comment is not None:
            kwargs["comment"] = comment
        with self.__database.client._tmp_session(session, close=False) as s:
            return cast(
                RawBatchCursor[_DocumentType],
                self._aggregate(
                    _CollectionRawAggregationCommand,
                    pipeline,
                    RawBatchCommandCursor,
                    session=s,
                    explicit_session=session is not None,
                    **kwargs,
                ),
            )

    def watch(
        self,
        pipeline: Optional[_Pipeline] = None,
        full_document: Optional[str] = None,
        resume_after: Optional[Mapping[str, Any]] = None,
        max_await_time_ms: Optional[int] = None,
        batch_size: Optional[int] = None,
        collation: Optional[_CollationIn] = None,
        start_at_operation_time: Optional[Timestamp] = None,
        session: Optional[ClientSession] = None,
        start_after: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        full_document_before_change: Optional[str] = None,
        show_expanded_events: Optional[bool] = None,
    ) -> CollectionChangeStream[_DocumentType]:
        """Watch changes on this collection.

        Performs an aggregation with an implicit initial ``$changeStream``
        stage and returns a
        :class:`~pymongo.change_stream.CollectionChangeStream` cursor which
        iterates over changes on this collection.

        .. code-block:: python

           with db.collection.watch() as stream:
               for change in stream:
                   print(change)

        The :class:`~pymongo.change_stream.CollectionChangeStream` iterable
        blocks until the next change document is returned or an error is
        raised. If the
        :meth:`~pymongo.change_stream.CollectionChangeStream.next` method
        encounters a network error when retrieving a batch from the server,
        it will automatically attempt to recreate the cursor such that no
        change events are missed. Any error encountered during the resume
        attempt indicates there may be an outage and will be raised.

        .. code-block:: python

            try:
                with db.collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error("...")

        For a precise description of the resume process see the
        `change streams specification`_.

        .. note:: Using this helper method is preferred to directly calling
            :meth:`~pymongo.collection.Collection.aggregate` with a
            ``$changeStream`` stage, for the purpose of supporting
            resumability.

        .. warning:: This Collection's :attr:`read_concern` must be
            ``ReadConcern("majority")`` in order to use the ``$changeStream``
            stage.

        :param pipeline: A list of aggregation pipeline stages to
            append to an initial ``$changeStream`` stage. Not all
            pipeline stages are valid after a ``$changeStream`` stage, see the
            MongoDB documentation on change streams for the supported stages.
        :param full_document: The fullDocument to pass as an option
            to the ``$changeStream`` stage. Allowed values: 'updateLookup',
            'whenAvailable', 'required'. When set to 'updateLookup', the
            change notification for partial updates will include both a delta
            describing the changes to the document, as well as a copy of the
            entire document that was changed from some time after the change
            occurred.
        :param full_document_before_change: Allowed values: 'whenAvailable'
            and 'required'. Change events may now result in a
            'fullDocumentBeforeChange' response field.
        :param resume_after: A resume token. If provided, the
            change stream will start returning changes that occur directly
            after the operation specified in the resume token. A resume token
            is the _id value of a change document.
        :param max_await_time_ms: The maximum time in milliseconds
            for the server to wait for changes before responding to a getMore
            operation.
        :param batch_size: The maximum number of documents to return
            per batch.
        :param collation: The :class:`~pymongo.collation.Collation`
            to use for the aggregation.
        :param start_at_operation_time: If provided, the resulting
            change stream will only return changes that occurred at or after
            the specified :class:`~bson.timestamp.Timestamp`. Requires
            MongoDB >= 4.0.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param start_after: The same as `resume_after` except that
            `start_after` can resume notifications after an invalidate event.
            This option and `resume_after` are mutually exclusive.
        :param comment: A user-provided comment to attach to this
            command.
        :param show_expanded_events: Include expanded events such as DDL events like `dropIndexes`.

        :return: A :class:`~pymongo.change_stream.CollectionChangeStream` cursor.

        .. versionchanged:: 4.3
           Added `show_expanded_events` parameter.

        .. versionchanged:: 4.2
           Added ``full_document_before_change`` parameter.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionchanged:: 3.7
           Added the ``start_at_operation_time`` parameter.

        .. versionadded:: 3.6

        .. seealso:: The MongoDB documentation on `changeStreams <https://mongodb.com/docs/manual/changeStreams/>`_.

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md
        """
        return CollectionChangeStream(
            self,
            pipeline,
            full_document,
            resume_after,
            max_await_time_ms,
            batch_size,
            collation,
            start_at_operation_time,
            session,
            start_after,
            comment,
            full_document_before_change,
            show_expanded_events,
        )

    @_csot.apply
    def rename(
        self,
        new_name: str,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> MutableMapping[str, Any]:
        """Rename this collection.

        If operating in auth mode, client must be authorized as an
        admin to perform this operation. Raises :class:`TypeError` if
        `new_name` is not an instance of :class:`str`.
        Raises :class:`~pymongo.errors.InvalidName`
        if `new_name` is not a valid collection name.

        :param new_name: new name for this collection
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: additional arguments to the rename command
            may be passed as keyword arguments to this helper method
            (i.e. ``dropTarget=True``)

        .. note:: The :attr:`~pymongo.collection.Collection.write_concern` of
           this collection is automatically applied to this operation.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this collection's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        if not isinstance(new_name, str):
            raise TypeError("new_name must be an instance of str")

        if not new_name or ".." in new_name:
            raise InvalidName("collection names cannot be empty")
        if new_name[0] == "." or new_name[-1] == ".":
            raise InvalidName("collection names must not start or end with '.'")
        if "$" in new_name and not new_name.startswith("oplog.$main"):
            raise InvalidName("collection names must not contain '$'")

        new_name = f"{self.__database.name}.{new_name}"
        cmd = {"renameCollection": self.__full_name, "to": new_name}
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment
        write_concern = self._write_concern_for_cmd(cmd, session)

        with self._conn_for_writes(session, operation=_Op.RENAME) as conn:
            with self.__database.client._tmp_session(session) as s:
                return conn.command(
                    "admin",
                    cmd,
                    write_concern=write_concern,
                    parse_write_concern_error=True,
                    session=s,
                    client=self.__database.client,
                )

    def distinct(
        self,
        key: str,
        filter: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> list:
        """Get a list of distinct values for `key` among all documents
        in this collection.

        Raises :class:`TypeError` if `key` is not an instance of
        :class:`str`.

        All optional distinct parameters should be passed as keyword arguments
        to this method. Valid options include:

          - `maxTimeMS` (int): The maximum amount of time to allow the count
            command to run, in milliseconds.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.

        The :meth:`distinct` method obeys the :attr:`read_preference` of
        this :class:`Collection`.

        :param key: name of the field for which we want to get the distinct
            values
        :param filter: A query document that specifies the documents
            from which to retrieve the distinct values.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: See list of options above.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Support the `collation` option.

        """
        if not isinstance(key, str):
            raise TypeError("key must be an instance of str")
        cmd = {"distinct": self.__name, "key": key}
        if filter is not None:
            if "query" in kwargs:
                raise ConfigurationError("can't pass both filter and query")
            kwargs["query"] = filter
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        cmd.update(kwargs)
        if comment is not None:
            cmd["comment"] = comment

        def _cmd(
            session: Optional[ClientSession],
            _server: Server,
            conn: Connection,
            read_preference: Optional[_ServerMode],
        ) -> list:
            return self._command(
                conn,
                cmd,
                read_preference=read_preference,
                read_concern=self.read_concern,
                collation=collation,
                session=session,
                user_fields={"values": 1},
            )["values"]

        return self._retryable_non_cursor_read(_cmd, session, operation=_Op.DISTINCT)

    def _write_concern_for_cmd(
        self, cmd: Mapping[str, Any], session: Optional[ClientSession]
    ) -> WriteConcern:
        raw_wc = cmd.get("writeConcern")
        if raw_wc is not None:
            return WriteConcern(**raw_wc)
        else:
            return self._write_concern_for(session)

    def __find_and_modify(
        self,
        filter: Mapping[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]],
        sort: Optional[_IndexList],
        upsert: Optional[bool] = None,
        return_document: bool = ReturnDocument.BEFORE,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping] = None,
        **kwargs: Any,
    ) -> Any:
        """Internal findAndModify helper."""
        common.validate_is_mapping("filter", filter)
        if not isinstance(return_document, bool):
            raise ValueError(
                "return_document must be ReturnDocument.BEFORE or ReturnDocument.AFTER"
            )
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        cmd = {"findAndModify": self.__name, "query": filter, "new": return_document}
        if let is not None:
            common.validate_is_mapping("let", let)
            cmd["let"] = let
        cmd.update(kwargs)
        if projection is not None:
            cmd["fields"] = helpers._fields_list_to_dict(projection, "projection")
        if sort is not None:
            cmd["sort"] = helpers._index_document(sort)
        if upsert is not None:
            validate_boolean("upsert", upsert)
            cmd["upsert"] = upsert
        if hint is not None:
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)

        write_concern = self._write_concern_for_cmd(cmd, session)

        def _find_and_modify(
            session: Optional[ClientSession], conn: Connection, retryable_write: bool
        ) -> Any:
            acknowledged = write_concern.acknowledged
            if array_filters is not None:
                if not acknowledged:
                    raise ConfigurationError(
                        "arrayFilters is unsupported for unacknowledged writes."
                    )
                cmd["arrayFilters"] = list(array_filters)
            if hint is not None:
                if conn.max_wire_version < 8:
                    raise ConfigurationError(
                        "Must be connected to MongoDB 4.2+ to use hint on find and modify commands."
                    )
                elif not acknowledged and conn.max_wire_version < 9:
                    raise ConfigurationError(
                        "Must be connected to MongoDB 4.4+ to use hint on unacknowledged find and modify commands."
                    )
                cmd["hint"] = hint
            out = self._command(
                conn,
                cmd,
                read_preference=ReadPreference.PRIMARY,
                write_concern=write_concern,
                collation=collation,
                session=session,
                retryable_write=retryable_write,
                user_fields=_FIND_AND_MODIFY_DOC_FIELDS,
            )
            _check_write_command_response(out)

            return out.get("value")

        return self.__database.client._retryable_write(
            write_concern.acknowledged,
            _find_and_modify,
            session,
            operation=_Op.FIND_AND_MODIFY,
        )

    def find_one_and_delete(
        self,
        filter: Mapping[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        sort: Optional[_IndexList] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _DocumentType:
        """Finds a single document and deletes it, returning the document.

          >>> db.test.count_documents({'x': 1})
          2
          >>> db.test.find_one_and_delete({'x': 1})
          {'x': 1, '_id': ObjectId('54f4e12bfba5220aa4d6dee8')}
          >>> db.test.count_documents({'x': 1})
          1

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'x': 1}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> db.test.find_one_and_delete(
          ...     {'x': 1}, sort=[('_id', pymongo.DESCENDING)])
          {'x': 1, '_id': 2}

        The *projection* option can be used to limit the fields returned.

          >>> db.test.find_one_and_delete({'x': 1}, projection={'_id': False})
          {'x': 1}

        :param filter: A query that matches the document to delete.
        :param projection: a list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
        :param sort: a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is deleted.
        :param hint: An index to use to support the query predicate
            specified either by its string name, or in the same format as
            passed to :meth:`~pymongo.collection.Collection.create_index`
            (e.g. ``[('field', ASCENDING)]``). This option is only supported
            on MongoDB 4.4 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 4.1
           Added ``let`` parameter.
        .. versionchanged:: 3.11
           Added ``hint`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionchanged:: 3.4
           Added the `collation` option.
        .. versionadded:: 3.0
        """
        kwargs["remove"] = True
        if comment is not None:
            kwargs["comment"] = comment
        return self.__find_and_modify(
            filter, projection, sort, let=let, hint=hint, session=session, **kwargs
        )

    def find_one_and_replace(
        self,
        filter: Mapping[str, Any],
        replacement: Mapping[str, Any],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        sort: Optional[_IndexList] = None,
        upsert: bool = False,
        return_document: bool = ReturnDocument.BEFORE,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _DocumentType:
        """Finds a single document and replaces it, returning either the
        original or the replaced document.

        The :meth:`find_one_and_replace` method differs from
        :meth:`find_one_and_update` by replacing the document matched by
        *filter*, rather than modifying the existing document.

          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'x': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}
          >>> db.test.find_one_and_replace({'x': 1}, {'y': 1})
          {'x': 1, '_id': 0}
          >>> for doc in db.test.find({}):
          ...     print(doc)
          ...
          {'y': 1, '_id': 0}
          {'x': 1, '_id': 1}
          {'x': 1, '_id': 2}

        :param filter: A query that matches the document to replace.
        :param replacement: The replacement document.
        :param projection: A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a mapping to exclude fields from
            the result (e.g. projection={'_id': False}).
        :param sort: a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is replaced.
        :param upsert: When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
        :param return_document: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was replaced, or ``None``
            if no document matches. If
            :attr:`ReturnDocument.AFTER`, returns the replaced
            or inserted document.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 4.1
           Added ``let`` parameter.
        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        .. versionchanged:: 3.4
           Added the ``collation`` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """
        common.validate_ok_for_replace(replacement)
        kwargs["update"] = replacement
        if comment is not None:
            kwargs["comment"] = comment
        return self.__find_and_modify(
            filter,
            projection,
            sort,
            upsert,
            return_document,
            let=let,
            hint=hint,
            session=session,
            **kwargs,
        )

    def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        projection: Optional[Union[Mapping[str, Any], Iterable[str]]] = None,
        sort: Optional[_IndexList] = None,
        upsert: bool = False,
        return_document: bool = ReturnDocument.BEFORE,
        array_filters: Optional[Sequence[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
        session: Optional[ClientSession] = None,
        let: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _DocumentType:
        """Finds a single document and updates it, returning either the
        original or the updated document.

          >>> db.test.find_one_and_update(
          ...    {'_id': 665}, {'$inc': {'count': 1}, '$set': {'done': True}})
          {'_id': 665, 'done': False, 'count': 25}}

        Returns ``None`` if no document matches the filter.

          >>> db.test.find_one_and_update(
          ...    {'_exists': False}, {'$inc': {'count': 1}})

        When the filter matches, by default :meth:`find_one_and_update`
        returns the original version of the document before the update was
        applied. To return the updated (or inserted in the case of
        *upsert*) version of the document instead, use the *return_document*
        option.

          >>> from pymongo import ReturnDocument
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     return_document=ReturnDocument.AFTER)
          {'_id': 'userid', 'seq': 1}

        You can limit the fields returned with the *projection* option.

          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     return_document=ReturnDocument.AFTER)
          {'seq': 2}

        The *upsert* option can be used to create the document if it doesn't
        already exist.

          >>> db.example.delete_many({}).deleted_count
          1
          >>> db.example.find_one_and_update(
          ...     {'_id': 'userid'},
          ...     {'$inc': {'seq': 1}},
          ...     projection={'seq': True, '_id': False},
          ...     upsert=True,
          ...     return_document=ReturnDocument.AFTER)
          {'seq': 1}

        If multiple documents match *filter*, a *sort* can be applied.

          >>> for doc in db.test.find({'done': True}):
          ...     print(doc)
          ...
          {'_id': 665, 'done': True, 'result': {'count': 26}}
          {'_id': 701, 'done': True, 'result': {'count': 17}}
          >>> db.test.find_one_and_update(
          ...     {'done': True},
          ...     {'$set': {'final': True}},
          ...     sort=[('_id', pymongo.DESCENDING)])
          {'_id': 701, 'done': True, 'result': {'count': 17}}

        :param filter: A query that matches the document to update.
        :param update: The update operations to apply.
        :param projection: A list of field names that should be
            returned in the result document or a mapping specifying the fields
            to include or exclude. If `projection` is a list "_id" will
            always be returned. Use a dict to exclude fields from
            the result (e.g. projection={'_id': False}).
        :param sort: a list of (key, direction) pairs
            specifying the sort order for the query. If multiple documents
            match the query, they are sorted and the first is updated.
        :param upsert: When ``True``, inserts a new document if no
            document matches the query. Defaults to ``False``.
        :param return_document: If
            :attr:`ReturnDocument.BEFORE` (the default),
            returns the original document before it was updated. If
            :attr:`ReturnDocument.AFTER`, returns the updated
            or inserted document.
        :param array_filters: A list of filters specifying which
            array elements an update should apply.
        :param hint: An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param let: Map of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. "$$var").
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: additional command arguments can be passed
            as keyword arguments (for example maxTimeMS can be used with
            recent server versions).

        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the ``update``.
        .. versionchanged:: 3.6
           Added the ``array_filters`` and ``session`` options.
        .. versionchanged:: 3.4
           Added the ``collation`` option.
        .. versionchanged:: 3.2
           Respects write concern.

        .. warning:: Starting in PyMongo 3.2, this command uses the
           :class:`~pymongo.write_concern.WriteConcern` of this
           :class:`~pymongo.collection.Collection` when connected to MongoDB >=
           3.2. Note that using an elevated write concern with this command may
           be slower compared to using the default write concern.

        .. versionadded:: 3.0
        """
        common.validate_ok_for_update(update)
        common.validate_list_or_none("array_filters", array_filters)
        kwargs["update"] = update
        if comment is not None:
            kwargs["comment"] = comment
        return self.__find_and_modify(
            filter,
            projection,
            sort,
            upsert,
            return_document,
            array_filters,
            hint=hint,
            let=let,
            session=session,
            **kwargs,
        )

    # See PYTHON-3084.
    __iter__ = None

    def __next__(self) -> NoReturn:
        raise TypeError("'Collection' object is not iterable")

    next = __next__

    def __call__(self, *args: Any, **kwargs: Any) -> NoReturn:
        """This is only here so that some API misusages are easier to debug."""
        if "." not in self.__name:
            raise TypeError(
                "'Collection' object is not callable. If you "
                "meant to call the '%s' method on a 'Database' "
                "object it is failing because no such method "
                "exists." % self.__name
            )
        raise TypeError(
            "'Collection' object is not callable. If you meant to "
            "call the '%s' method on a 'Collection' object it is "
            "failing because no such method exists." % self.__name.split(".")[-1]
        )
