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

"""Database level operations."""
from __future__ import annotations

from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)

from bson.codec_options import DEFAULT_CODEC_OPTIONS, CodecOptions
from bson.dbref import DBRef
from bson.timestamp import Timestamp
from pymongo import _csot, common
from pymongo.aggregation import _DatabaseAggregationCommand
from pymongo.change_stream import DatabaseChangeStream
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.common import _ecoc_coll_name, _esc_coll_name
from pymongo.errors import CollectionInvalid, InvalidName, InvalidOperation
from pymongo.operations import _Op
from pymongo.read_preferences import ReadPreference, _ServerMode
from pymongo.typings import _CollationIn, _DocumentType, _DocumentTypeArg, _Pipeline

if TYPE_CHECKING:
    import bson
    import bson.codec_options
    from pymongo.client_session import ClientSession
    from pymongo.mongo_client import MongoClient
    from pymongo.pool import Connection
    from pymongo.read_concern import ReadConcern
    from pymongo.server import Server
    from pymongo.write_concern import WriteConcern


def _check_name(name: str) -> None:
    """Check if a database name is valid."""
    if not name:
        raise InvalidName("database name cannot be the empty string")

    for invalid_char in [" ", ".", "$", "/", "\\", "\x00", '"']:
        if invalid_char in name:
            raise InvalidName("database names cannot contain the character %r" % invalid_char)


_CodecDocumentType = TypeVar("_CodecDocumentType", bound=Mapping[str, Any])


class Database(common.BaseObject, Generic[_DocumentType]):
    """A Mongo database."""

    def __init__(
        self,
        client: MongoClient[_DocumentType],
        name: str,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> None:
        """Get a database by client and name.

        Raises :class:`TypeError` if `name` is not an instance of
        :class:`str`. Raises :class:`~pymongo.errors.InvalidName` if
        `name` is not a valid database name.

        :param client: A :class:`~pymongo.mongo_client.MongoClient` instance.
        :param name: The database name.
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) client.codec_options is used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) client.read_preference is used.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) client.write_concern is used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) client.read_concern is used.

        .. seealso:: The MongoDB documentation on `databases <https://dochub.mongodb.org/core/databases>`_.

        .. versionchanged:: 4.0
           Removed the eval, system_js, error, last_status, previous_error,
           reset_error_history, authenticate, logout, collection_names,
           current_op, add_user, remove_user, profiling_level,
           set_profiling_level, and profiling_info methods.
           See the :ref:`pymongo4-migration-guide`.

        .. versionchanged:: 3.2
           Added the read_concern option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.
           :class:`~pymongo.database.Database` no longer returns an instance
           of :class:`~pymongo.collection.Collection` for attribute names
           with leading underscores. You must use dict-style lookups instead::

               db['__my_collection__']

           Not:

               db.__my_collection__
        """
        super().__init__(
            codec_options or client.codec_options,
            read_preference or client.read_preference,
            write_concern or client.write_concern,
            read_concern or client.read_concern,
        )

        if not isinstance(name, str):
            raise TypeError("name must be an instance of str")

        if name != "$external":
            _check_name(name)

        self.__name = name
        self.__client: MongoClient[_DocumentType] = client
        self._timeout = client.options.timeout

    @property
    def client(self) -> MongoClient[_DocumentType]:
        """The client instance for this :class:`Database`."""
        return self.__client

    @property
    def name(self) -> str:
        """The name of this :class:`Database`."""
        return self.__name

    def with_options(
        self,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> Database[_DocumentType]:
        """Get a clone of this database changing the specified settings.

          >>> db1.read_preference
          Primary()
          >>> from pymongo.read_preferences import Secondary
          >>> db2 = db1.with_options(read_preference=Secondary([{'node': 'analytics'}]))
          >>> db1.read_preference
          Primary()
          >>> db2.read_preference
          Secondary(tag_sets=[{'node': 'analytics'}], max_staleness=-1, hedge=None)

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

        .. versionadded:: 3.8
        """
        return Database(
            self.client,
            self.__name,
            codec_options or self.codec_options,
            read_preference or self.read_preference,
            write_concern or self.write_concern,
            read_concern or self.read_concern,
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Database):
            return self.__client == other.client and self.__name == other.name
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash((self.__client, self.__name))

    def __repr__(self) -> str:
        return f"Database({self.__client!r}, {self.__name!r})"

    def __getattr__(self, name: str) -> Collection[_DocumentType]:
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :param name: the name of the collection to get
        """
        if name.startswith("_"):
            raise AttributeError(
                f"Database has no attribute {name!r}. To access the {name}"
                f" collection, use database[{name!r}]."
            )
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> Collection[_DocumentType]:
        """Get a collection of this database by name.

        Raises InvalidName if an invalid collection name is used.

        :param name: the name of the collection to get
        """
        return Collection(self, name)

    def get_collection(
        self,
        name: str,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
    ) -> Collection[_DocumentType]:
        """Get a :class:`~pymongo.collection.Collection` with the given name
        and options.

        Useful for creating a :class:`~pymongo.collection.Collection` with
        different codec options, read preference, and/or write concern from
        this :class:`Database`.

          >>> db.read_preference
          Primary()
          >>> coll1 = db.test
          >>> coll1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> coll2 = db.get_collection(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> coll2.read_preference
          Secondary(tag_sets=None)

        :param name: The name of the collection - a string.
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used. See :mod:`~pymongo.read_preferences`
            for options.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
        """
        return Collection(
            self,
            name,
            False,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
        )

    def _get_encrypted_fields(
        self, kwargs: Mapping[str, Any], coll_name: str, ask_db: bool
    ) -> Optional[Mapping[str, Any]]:
        encrypted_fields = kwargs.get("encryptedFields")
        if encrypted_fields:
            return cast(Mapping[str, Any], deepcopy(encrypted_fields))
        if (
            self.client.options.auto_encryption_opts
            and self.client.options.auto_encryption_opts._encrypted_fields_map
            and self.client.options.auto_encryption_opts._encrypted_fields_map.get(
                f"{self.name}.{coll_name}"
            )
        ):
            return cast(
                Mapping[str, Any],
                deepcopy(
                    self.client.options.auto_encryption_opts._encrypted_fields_map[
                        f"{self.name}.{coll_name}"
                    ]
                ),
            )
        if ask_db and self.client.options.auto_encryption_opts:
            options = self[coll_name].options()
            if options.get("encryptedFields"):
                return cast(Mapping[str, Any], deepcopy(options["encryptedFields"]))
        return None

    @_csot.apply
    def create_collection(
        self,
        name: str,
        codec_options: Optional[CodecOptions[_DocumentTypeArg]] = None,
        read_preference: Optional[_ServerMode] = None,
        write_concern: Optional[WriteConcern] = None,
        read_concern: Optional[ReadConcern] = None,
        session: Optional[ClientSession] = None,
        check_exists: Optional[bool] = True,
        **kwargs: Any,
    ) -> Collection[_DocumentType]:
        """Create a new :class:`~pymongo.collection.Collection` in this
        database.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.

        :param name: the name of the collection to create
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used.
        :param write_concern: An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
        :param read_concern: An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param `check_exists`: if True (the default), send a listCollections command to
            check if the collection already exists before creation.
        :param kwargs: additional keyword arguments will
            be passed as options for the `create collection command`_

        All optional `create collection command`_ parameters should be passed
        as keyword arguments to this method. Valid options include, but are not
        limited to:

          - ``size`` (int): desired initial size for the collection (in
            bytes). For capped collections this size is the max
            size of the collection.
          - ``capped`` (bool): if True, this is a capped collection
          - ``max`` (int): maximum number of objects if capped (optional)
          - ``timeseries`` (dict): a document specifying configuration options for
            timeseries collections
          - ``expireAfterSeconds`` (int): the number of seconds after which a
            document in a timeseries collection expires
          - ``validator`` (dict): a document specifying validation rules or expressions
            for the collection
          - ``validationLevel`` (str): how strictly to apply the
            validation rules to existing documents during an update.  The default level
            is "strict"
          - ``validationAction`` (str): whether to "error" on invalid documents
            (the default) or just "warn" about the violations but allow invalid
            documents to be inserted
          - ``indexOptionDefaults`` (dict): a document specifying a default configuration
            for indexes when creating a collection
          - ``viewOn`` (str): the name of the source collection or view from which
            to create the view
          - ``pipeline`` (list): a list of aggregation pipeline stages
          - ``comment`` (str): a user-provided comment to attach to this command.
            This option is only supported on MongoDB >= 4.4.
          - ``encryptedFields`` (dict): **(BETA)** Document that describes the encrypted fields for
            Queryable Encryption. For example::

                {
                  "escCollection": "enxcol_.encryptedCollection.esc",
                  "ecocCollection": "enxcol_.encryptedCollection.ecoc",
                  "fields": [
                      {
                          "path": "firstName",
                          "keyId": Binary.from_uuid(UUID('00000000-0000-0000-0000-000000000000')),
                          "bsonType": "string",
                          "queries": {"queryType": "equality"}
                      },
                      {
                          "path": "ssn",
                          "keyId": Binary.from_uuid(UUID('04104104-1041-0410-4104-104104104104')),
                          "bsonType": "string"
                      }
                    ]
                }
          - ``clusteredIndex`` (dict): Document that specifies the clustered index
            configuration. It must have the following form::

                {
                    // key pattern must be {_id: 1}
                    key: <key pattern>, // required
                    unique: <bool>, // required, must be `true`
                    name: <string>, // optional, otherwise automatically generated
                    v: <int>, // optional, must be `2` if provided
                }
          - ``changeStreamPreAndPostImages`` (dict): a document with a boolean field ``enabled`` for
            enabling pre- and post-images.

        .. versionchanged:: 4.2
           Added the ``check_exists``, ``clusteredIndex``, and  ``encryptedFields`` parameters.

        .. versionchanged:: 3.11
           This method is now supported inside multi-document transactions
           with MongoDB 4.4+.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Added the collation option.

        .. versionchanged:: 3.0
           Added the codec_options, read_preference, and write_concern options.

        .. _create collection command:
            https://mongodb.com/docs/manual/reference/command/create
        """
        encrypted_fields = self._get_encrypted_fields(kwargs, name, False)
        if encrypted_fields:
            common.validate_is_mapping("encryptedFields", encrypted_fields)
            kwargs["encryptedFields"] = encrypted_fields

        clustered_index = kwargs.get("clusteredIndex")
        if clustered_index:
            common.validate_is_mapping("clusteredIndex", clustered_index)

        with self.__client._tmp_session(session) as s:
            # Skip this check in a transaction where listCollections is not
            # supported.
            if (
                check_exists
                and (not s or not s.in_transaction)
                and name in self.list_collection_names(filter={"name": name}, session=s)
            ):
                raise CollectionInvalid("collection %s already exists" % name)
            return Collection(
                self,
                name,
                True,
                codec_options,
                read_preference,
                write_concern,
                read_concern,
                session=s,
                **kwargs,
            )

    def aggregate(
        self, pipeline: _Pipeline, session: Optional[ClientSession] = None, **kwargs: Any
    ) -> CommandCursor[_DocumentType]:
        """Perform a database-level aggregation.

        See the `aggregation pipeline`_ documentation for a list of stages
        that are supported.

        .. code-block:: python

           # Lists all operations currently running on the server.
           with client.admin.aggregate([{"$currentOp": {}}]) as cursor:
               for operation in cursor:
                   print(operation)

        The :meth:`aggregate` method obeys the :attr:`read_preference` of this
        :class:`Database`, except when ``$out`` or ``$merge`` are used, in
        which case  :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`
        is used.

        .. note:: This method does not support the 'explain' option. Please
           use :meth:`~pymongo.database.Database.command` instead.

        .. note:: The :attr:`~pymongo.database.Database.write_concern` of
           this collection is automatically applied to this operation.

        :param pipeline: a list of aggregation pipeline stages
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
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
          - `let` (dict): A dict of parameter names and values. Values must be
            constant or closed expressions that do not reference document
            fields. Parameters can then be accessed as variables in an
            aggregate expression context (e.g. ``"$$var"``). This option is
            only supported on MongoDB >= 5.0.

        :return: A :class:`~pymongo.command_cursor.CommandCursor` over the result
          set.

        .. versionadded:: 3.9

        .. _aggregation pipeline:
            https://mongodb.com/docs/manual/reference/operator/aggregation-pipeline

        .. _aggregate command:
            https://mongodb.com/docs/manual/reference/command/aggregate
        """
        with self.client._tmp_session(session, close=False) as s:
            cmd = _DatabaseAggregationCommand(
                self,
                CommandCursor,
                pipeline,
                kwargs,
                session is not None,
                user_fields={"cursor": {"firstBatch": 1}},
            )
            return self.client._retryable_read(
                cmd.get_cursor,
                cmd.get_read_preference(s),  # type: ignore[arg-type]
                s,
                retryable=not cmd._performs_write,
                operation=_Op.AGGREGATE,
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
    ) -> DatabaseChangeStream[_DocumentType]:
        """Watch changes on this database.

        Performs an aggregation with an implicit initial ``$changeStream``
        stage and returns a
        :class:`~pymongo.change_stream.DatabaseChangeStream` cursor which
        iterates over changes on all collections in this database.

        Introduced in MongoDB 4.0.

        .. code-block:: python

           with db.watch() as stream:
               for change in stream:
                   print(change)

        The :class:`~pymongo.change_stream.DatabaseChangeStream` iterable
        blocks until the next change document is returned or an error is
        raised. If the
        :meth:`~pymongo.change_stream.DatabaseChangeStream.next` method
        encounters a network error when retrieving a batch from the server,
        it will automatically attempt to recreate the cursor such that no
        change events are missed. Any error encountered during the resume
        attempt indicates there may be an outage and will be raised.

        .. code-block:: python

            try:
                with db.watch([{"$match": {"operationType": "insert"}}]) as stream:
                    for insert_change in stream:
                        print(insert_change)
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                logging.error("...")

        For a precise description of the resume process see the
        `change streams specification`_.

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

        :return: A :class:`~pymongo.change_stream.DatabaseChangeStream` cursor.

        .. versionchanged:: 4.3
           Added `show_expanded_events` parameter.

        .. versionchanged:: 4.2
            Added ``full_document_before_change`` parameter.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.9
           Added the ``start_after`` parameter.

        .. versionadded:: 3.7

        .. seealso:: The MongoDB documentation on `changeStreams <https://mongodb.com/docs/manual/changeStreams/>`_.

        .. _change streams specification:
            https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md
        """
        return DatabaseChangeStream(
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
            show_expanded_events=show_expanded_events,
        )

    @overload
    def _command(
        self,
        conn: Connection,
        command: Union[str, MutableMapping[str, Any]],
        value: int = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: _ServerMode = ReadPreference.PRIMARY,
        codec_options: CodecOptions[dict[str, Any]] = DEFAULT_CODEC_OPTIONS,
        write_concern: Optional[WriteConcern] = None,
        parse_write_concern_error: bool = False,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        ...

    @overload
    def _command(
        self,
        conn: Connection,
        command: Union[str, MutableMapping[str, Any]],
        value: int = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: _ServerMode = ReadPreference.PRIMARY,
        codec_options: CodecOptions[_CodecDocumentType] = ...,
        write_concern: Optional[WriteConcern] = None,
        parse_write_concern_error: bool = False,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> _CodecDocumentType:
        ...

    def _command(
        self,
        conn: Connection,
        command: Union[str, MutableMapping[str, Any]],
        value: int = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: _ServerMode = ReadPreference.PRIMARY,
        codec_options: Union[
            CodecOptions[dict[str, Any]], CodecOptions[_CodecDocumentType]
        ] = DEFAULT_CODEC_OPTIONS,
        write_concern: Optional[WriteConcern] = None,
        parse_write_concern_error: bool = False,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> Union[dict[str, Any], _CodecDocumentType]:
        """Internal command helper."""
        if isinstance(command, str):
            command = {command: value}

        command.update(kwargs)
        with self.__client._tmp_session(session) as s:
            return conn.command(
                self.__name,
                command,
                read_preference,
                codec_options,
                check,
                allowable_errors,
                write_concern=write_concern,
                parse_write_concern_error=parse_write_concern_error,
                session=s,
                client=self.__client,
            )

    @overload
    def command(
        self,
        command: Union[str, MutableMapping[str, Any]],
        value: Any = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: Optional[_ServerMode] = None,
        codec_options: None = None,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        ...

    @overload
    def command(
        self,
        command: Union[str, MutableMapping[str, Any]],
        value: Any = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: Optional[_ServerMode] = None,
        codec_options: CodecOptions[_CodecDocumentType] = ...,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> _CodecDocumentType:
        ...

    @_csot.apply
    def command(
        self,
        command: Union[str, MutableMapping[str, Any]],
        value: Any = 1,
        check: bool = True,
        allowable_errors: Optional[Sequence[Union[str, int]]] = None,
        read_preference: Optional[_ServerMode] = None,
        codec_options: Optional[bson.codec_options.CodecOptions[_CodecDocumentType]] = None,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> Union[dict[str, Any], _CodecDocumentType]:
        """Issue a MongoDB command.

        Send command `command` to the database and return the
        response. If `command` is an instance of :class:`str`
        then the command {`command`: `value`} will be sent.
        Otherwise, `command` must be an instance of
        :class:`dict` and will be sent as is.

        Any additional keyword arguments will be added to the final
        command document before it is sent.

        For example, a command like ``{buildinfo: 1}`` can be sent
        using:

        >>> db.command("buildinfo")
        OR
        >>> db.command({"buildinfo": 1})

        For a command where the value matters, like ``{count:
        collection_name}`` we can do:

        >>> db.command("count", collection_name)
        OR
        >>> db.command({"count": collection_name})

        For commands that take additional arguments we can use
        kwargs. So ``{count: collection_name, query: query}`` becomes:

        >>> db.command("count", collection_name, query=query)
        OR
        >>> db.command({"count": collection_name, "query": query})

        :param command: document representing the command to be issued,
            or the name of the command (for simple commands only).

            .. note:: the order of keys in the `command` document is
               significant (the "verb" must come first), so commands
               which require multiple keys (e.g. `findandmodify`)
               should be done with this in mind.

        :param value: value to use for the command verb when
            `command` is passed as a string
        :param check: check the response for errors, raising
            :class:`~pymongo.errors.OperationFailure` if there are any
        :param allowable_errors: if `check` is ``True``, error messages
            in this list will be ignored by error-checking
        :param read_preference: The read preference for this
            operation. See :mod:`~pymongo.read_preferences` for options.
            If the provided `session` is in a transaction, defaults to the
            read preference configured for the transaction.
            Otherwise, defaults to
            :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
        :param codec_options: A :class:`~bson.codec_options.CodecOptions`
            instance.
        :param session: A
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: additional keyword arguments will
            be added to the command document before it is sent


        .. note:: :meth:`command` does **not** obey this Database's
           :attr:`read_preference` or :attr:`codec_options`. You must use the
           ``read_preference`` and ``codec_options`` parameters instead.

        .. note:: :meth:`command` does **not** apply any custom TypeDecoders
           when decoding the command response.

        .. note:: If this client has been configured to use MongoDB Stable
           API (see :ref:`versioned-api-ref`), then :meth:`command` will
           automatically add API versioning options to the given command.
           Explicitly adding API versioning options in the command and
           declaring an API version on the client is not supported.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.0
           Removed the `as_class`, `fields`, `uuid_subtype`, `tag_sets`,
           and `secondary_acceptable_latency_ms` option.
           Removed `compile_re` option: PyMongo now always represents BSON
           regular expressions as :class:`~bson.regex.Regex` objects. Use
           :meth:`~bson.regex.Regex.try_compile` to attempt to convert from a
           BSON regular expression to a Python regular expression object.
           Added the ``codec_options`` parameter.

        .. seealso:: The MongoDB documentation on `commands <https://dochub.mongodb.org/core/commands>`_.
        """
        opts = codec_options or DEFAULT_CODEC_OPTIONS
        if comment is not None:
            kwargs["comment"] = comment

        if isinstance(command, str):
            command_name = command
        else:
            command_name = next(iter(command))

        if read_preference is None:
            read_preference = (session and session._txn_read_preference()) or ReadPreference.PRIMARY
        with self.__client._conn_for_reads(read_preference, session, operation=command_name) as (
            connection,
            read_preference,
        ):
            return self._command(
                connection,
                command,
                value,
                check,
                allowable_errors,
                read_preference,
                opts,
                session=session,
                **kwargs,
            )

    @_csot.apply
    def cursor_command(
        self,
        command: Union[str, MutableMapping[str, Any]],
        value: Any = 1,
        read_preference: Optional[_ServerMode] = None,
        codec_options: Optional[bson.codec_options.CodecOptions[_CodecDocumentType]] = None,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        max_await_time_ms: Optional[int] = None,
        **kwargs: Any,
    ) -> CommandCursor[_DocumentType]:
        """Issue a MongoDB command and parse the response as a cursor.

        If the response from the server does not include a cursor field, an error will be thrown.

        Otherwise, behaves identically to issuing a normal MongoDB command.

        :param command: document representing the command to be issued,
            or the name of the command (for simple commands only).

            .. note:: the order of keys in the `command` document is
               significant (the "verb" must come first), so commands
               which require multiple keys (e.g. `findandmodify`)
               should use an instance of :class:`~bson.son.SON` or
               a string and kwargs instead of a Python `dict`.

        :param value: value to use for the command verb when
          `command` is passed as a string
        :param read_preference: The read preference for this
          operation. See :mod:`~pymongo.read_preferences` for options.
          If the provided `session` is in a transaction, defaults to the
          read preference configured for the transaction.
          Otherwise, defaults to
          :attr:`~pymongo.read_preferences.ReadPreference.PRIMARY`.
        :param codec_options`: A :class:`~bson.codec_options.CodecOptions`
          instance.
        :param session: A
          :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to future getMores for this
          command.
        :param max_await_time_ms: The number of ms to wait for more data on future getMores for this command.
        :param kwargs: additional keyword arguments will
          be added to the command document before it is sent

        .. note:: :meth:`command` does **not** obey this Database's
           :attr:`read_preference` or :attr:`codec_options`. You must use the
           ``read_preference`` and ``codec_options`` parameters instead.

        .. note:: :meth:`command` does **not** apply any custom TypeDecoders
           when decoding the command response.

        .. note:: If this client has been configured to use MongoDB Stable
           API (see :ref:`versioned-api-ref`), then :meth:`command` will
           automatically add API versioning options to the given command.
           Explicitly adding API versioning options in the command and
           declaring an API version on the client is not supported.

        .. seealso:: The MongoDB documentation on `commands <https://dochub.mongodb.org/core/commands>`_.
        """
        if isinstance(command, str):
            command_name = command
        else:
            command_name = next(iter(command))

        with self.__client._tmp_session(session, close=False) as tmp_session:
            opts = codec_options or DEFAULT_CODEC_OPTIONS

            if read_preference is None:
                read_preference = (
                    tmp_session and tmp_session._txn_read_preference()
                ) or ReadPreference.PRIMARY
            with self.__client._conn_for_reads(read_preference, tmp_session, command_name) as (
                conn,
                read_preference,
            ):
                response = self._command(
                    conn,
                    command,
                    value,
                    True,
                    None,
                    read_preference,
                    opts,
                    session=tmp_session,
                    **kwargs,
                )
                coll = self.get_collection("$cmd", read_preference=read_preference)
                if response.get("cursor"):
                    cmd_cursor = CommandCursor(
                        coll,
                        response["cursor"],
                        conn.address,
                        max_await_time_ms=max_await_time_ms,
                        session=tmp_session,
                        explicit_session=session is not None,
                        comment=comment,
                    )
                    cmd_cursor._maybe_pin_connection(conn)
                    return cmd_cursor
                else:
                    raise InvalidOperation("Command does not return a cursor.")

    def _retryable_read_command(
        self,
        command: Union[str, MutableMapping[str, Any]],
        operation: str,
        session: Optional[ClientSession] = None,
    ) -> dict[str, Any]:
        """Same as command but used for retryable read commands."""
        read_preference = (session and session._txn_read_preference()) or ReadPreference.PRIMARY

        def _cmd(
            session: Optional[ClientSession],
            _server: Server,
            conn: Connection,
            read_preference: _ServerMode,
        ) -> dict[str, Any]:
            return self._command(
                conn,
                command,
                read_preference=read_preference,
                session=session,
            )

        return self.__client._retryable_read(_cmd, read_preference, session, operation)

    def _list_collections(
        self,
        conn: Connection,
        session: Optional[ClientSession],
        read_preference: _ServerMode,
        **kwargs: Any,
    ) -> CommandCursor[MutableMapping[str, Any]]:
        """Internal listCollections helper."""
        coll = cast(
            Collection[MutableMapping[str, Any]],
            self.get_collection("$cmd", read_preference=read_preference),
        )
        cmd = {"listCollections": 1, "cursor": {}}
        cmd.update(kwargs)
        with self.__client._tmp_session(session, close=False) as tmp_session:
            cursor = self._command(conn, cmd, read_preference=read_preference, session=tmp_session)[
                "cursor"
            ]
            cmd_cursor = CommandCursor(
                coll,
                cursor,
                conn.address,
                session=tmp_session,
                explicit_session=session is not None,
                comment=cmd.get("comment"),
            )
        cmd_cursor._maybe_pin_connection(conn)
        return cmd_cursor

    def list_collections(
        self,
        session: Optional[ClientSession] = None,
        filter: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> CommandCursor[MutableMapping[str, Any]]:
        """Get a cursor over the collections of this database.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param filter:  A query document to filter the list of
            collections returned from the listCollections command.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: Optional parameters of the
            `listCollections command
            <https://mongodb.com/docs/manual/reference/command/listCollections/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.


        :return: An instance of :class:`~pymongo.command_cursor.CommandCursor`.

        .. versionadded:: 3.6
        """
        if filter is not None:
            kwargs["filter"] = filter
        read_pref = (session and session._txn_read_preference()) or ReadPreference.PRIMARY
        if comment is not None:
            kwargs["comment"] = comment

        def _cmd(
            session: Optional[ClientSession],
            _server: Server,
            conn: Connection,
            read_preference: _ServerMode,
        ) -> CommandCursor[MutableMapping[str, Any]]:
            return self._list_collections(conn, session, read_preference=read_preference, **kwargs)

        return self.__client._retryable_read(
            _cmd, read_pref, session, operation=_Op.LIST_COLLECTIONS
        )

    def list_collection_names(
        self,
        session: Optional[ClientSession] = None,
        filter: Optional[Mapping[str, Any]] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> list[str]:
        """Get a list of all the collection names in this database.

        For example, to list all non-system collections::

            filter = {"name": {"$regex": r"^(?!system\\.)"}}
            db.list_collection_names(filter=filter)

        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param filter:  A query document to filter the list of
            collections returned from the listCollections command.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: Optional parameters of the
            `listCollections command
            <https://mongodb.com/docs/manual/reference/command/listCollections/>`_
            can be passed as keyword arguments to this method. The supported
            options differ by server version.


        .. versionchanged:: 3.8
           Added the ``filter`` and ``**kwargs`` parameters.

        .. versionadded:: 3.6
        """
        if comment is not None:
            kwargs["comment"] = comment
        if filter is None:
            kwargs["nameOnly"] = True

        else:
            # The enumerate collections spec states that "drivers MUST NOT set
            # nameOnly if a filter specifies any keys other than name."
            common.validate_is_mapping("filter", filter)
            kwargs["filter"] = filter
            if not filter or (len(filter) == 1 and "name" in filter):
                kwargs["nameOnly"] = True

        return [result["name"] for result in self.list_collections(session=session, **kwargs)]

    def _drop_helper(
        self, name: str, session: Optional[ClientSession] = None, comment: Optional[Any] = None
    ) -> dict[str, Any]:
        command = {"drop": name}
        if comment is not None:
            command["comment"] = comment

        with self.__client._conn_for_writes(session, operation=_Op.DROP) as connection:
            return self._command(
                connection,
                command,
                allowable_errors=["ns not found", 26],
                write_concern=self._write_concern_for(session),
                parse_write_concern_error=True,
                session=session,
            )

    @_csot.apply
    def drop_collection(
        self,
        name_or_collection: Union[str, Collection[_DocumentTypeArg]],
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        encrypted_fields: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        """Drop a collection.

        :param name_or_collection: the name of a collection to drop or the
            collection object itself
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param encrypted_fields: **(BETA)** Document that describes the encrypted fields for
            Queryable Encryption. For example::

                {
                  "escCollection": "enxcol_.encryptedCollection.esc",
                  "ecocCollection": "enxcol_.encryptedCollection.ecoc",
                  "fields": [
                      {
                          "path": "firstName",
                          "keyId": Binary.from_uuid(UUID('00000000-0000-0000-0000-000000000000')),
                          "bsonType": "string",
                          "queries": {"queryType": "equality"}
                      },
                      {
                          "path": "ssn",
                          "keyId": Binary.from_uuid(UUID('04104104-1041-0410-4104-104104104104')),
                          "bsonType": "string"
                      }
                  ]

                }


        .. note:: The :attr:`~pymongo.database.Database.write_concern` of
           this database is automatically applied to this operation.

        .. versionchanged:: 4.2
           Added ``encrypted_fields`` parameter.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.4
           Apply this database's write concern automatically to this operation
           when connected to MongoDB >= 3.4.

        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, str):
            raise TypeError("name_or_collection must be an instance of str")
        encrypted_fields = self._get_encrypted_fields(
            {"encryptedFields": encrypted_fields},
            name,
            True,
        )
        if encrypted_fields:
            common.validate_is_mapping("encrypted_fields", encrypted_fields)
            self._drop_helper(
                _esc_coll_name(encrypted_fields, name), session=session, comment=comment
            )
            self._drop_helper(
                _ecoc_coll_name(encrypted_fields, name), session=session, comment=comment
            )

        return self._drop_helper(name, session, comment)

    def validate_collection(
        self,
        name_or_collection: Union[str, Collection[_DocumentTypeArg]],
        scandata: bool = False,
        full: bool = False,
        session: Optional[ClientSession] = None,
        background: Optional[bool] = None,
        comment: Optional[Any] = None,
    ) -> dict[str, Any]:
        """Validate a collection.

        Returns a dict of validation info. Raises CollectionInvalid if
        validation fails.

        See also the MongoDB documentation on the `validate command`_.

        :param name_or_collection: A Collection object or the name of a
            collection to validate.
        :param scandata: Do extra checks beyond checking the overall
            structure of the collection.
        :param full: Have the server do a more thorough scan of the
            collection. Use with `scandata` for a thorough scan
            of the structure of the collection and the individual
            documents.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param background: A boolean flag that determines whether
            the command runs in the background. Requires MongoDB 4.4+.
        :param comment: A user-provided comment to attach to this
            command.

        .. versionchanged:: 4.1
           Added ``comment`` parameter.

        .. versionchanged:: 3.11
           Added ``background`` parameter.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. _validate command: https://mongodb.com/docs/manual/reference/command/validate/
        """
        name = name_or_collection
        if isinstance(name, Collection):
            name = name.name

        if not isinstance(name, str):
            raise TypeError("name_or_collection must be an instance of str or Collection")
        cmd = {"validate": name, "scandata": scandata, "full": full}
        if comment is not None:
            cmd["comment"] = comment

        if background is not None:
            cmd["background"] = background

        result = self.command(cmd, session=session)

        valid = True
        # Pre 1.9 results
        if "result" in result:
            info = result["result"]
            if info.find("exception") != -1 or info.find("corrupt") != -1:
                raise CollectionInvalid(f"{name} invalid: {info}")
        # Sharded results
        elif "raw" in result:
            for _, res in result["raw"].items():
                if "result" in res:
                    info = res["result"]
                    if info.find("exception") != -1 or info.find("corrupt") != -1:
                        raise CollectionInvalid(f"{name} invalid: {info}")
                elif not res.get("valid", False):
                    valid = False
                    break
        # Post 1.9 non-sharded results.
        elif not result.get("valid", False):
            valid = False

        if not valid:
            raise CollectionInvalid(f"{name} invalid: {result!r}")

        return result

    # See PYTHON-3084.
    __iter__ = None

    def __next__(self) -> NoReturn:
        raise TypeError("'Database' object is not iterable")

    next = __next__

    def __bool__(self) -> NoReturn:
        raise NotImplementedError(
            "Database objects do not implement truth "
            "value testing or bool(). Please compare "
            "with None instead: database is not None"
        )

    def dereference(
        self,
        dbref: DBRef,
        session: Optional[ClientSession] = None,
        comment: Optional[Any] = None,
        **kwargs: Any,
    ) -> Optional[_DocumentType]:
        """Dereference a :class:`~bson.dbref.DBRef`, getting the
        document it points to.

        Raises :class:`TypeError` if `dbref` is not an instance of
        :class:`~bson.dbref.DBRef`. Returns a document, or ``None`` if
        the reference does not point to a valid document.  Raises
        :class:`ValueError` if `dbref` has a database specified that
        is different from the current database.

        :param dbref: the reference
        :param session: a
            :class:`~pymongo.client_session.ClientSession`.
        :param comment: A user-provided comment to attach to this
            command.
        :param kwargs: any additional keyword arguments
            are the same as the arguments to
            :meth:`~pymongo.collection.Collection.find`.


        .. versionchanged:: 4.1
           Added ``comment`` parameter.
        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        if not isinstance(dbref, DBRef):
            raise TypeError("cannot dereference a %s" % type(dbref))
        if dbref.database is not None and dbref.database != self.__name:
            raise ValueError(
                "trying to dereference a DBRef that points to "
                f"another database ({dbref.database!r} not {self.__name!r})"
            )
        return self[dbref.collection].find_one(
            {"_id": dbref.id}, session=session, comment=comment, **kwargs
        )
