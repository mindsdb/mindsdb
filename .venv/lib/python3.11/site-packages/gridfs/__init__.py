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

"""GridFS is a specification for storing large objects in Mongo.

The :mod:`gridfs` package is an implementation of GridFS on top of
:mod:`pymongo`, exposing a file-like interface.

.. seealso:: The MongoDB documentation on `gridfs <https://dochub.mongodb.org/core/gridfs>`_.
"""
from __future__ import annotations

from collections import abc
from typing import Any, Mapping, Optional, cast

from bson.objectid import ObjectId
from gridfs.errors import NoFile
from gridfs.grid_file import (
    DEFAULT_CHUNK_SIZE,
    GridIn,
    GridOut,
    GridOutCursor,
    _clear_entity_type_registry,
    _disallow_transactions,
)
from pymongo import ASCENDING, DESCENDING, _csot
from pymongo.client_session import ClientSession
from pymongo.collection import Collection
from pymongo.common import validate_string
from pymongo.database import Database
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import _ServerMode
from pymongo.write_concern import WriteConcern

__all__ = [
    "GridFS",
    "GridFSBucket",
    "NoFile",
    "DEFAULT_CHUNK_SIZE",
    "GridIn",
    "GridOut",
    "GridOutCursor",
]


class GridFS:
    """An instance of GridFS on top of a single Database."""

    def __init__(self, database: Database, collection: str = "fs"):
        """Create a new instance of :class:`GridFS`.

        Raises :class:`TypeError` if `database` is not an instance of
        :class:`~pymongo.database.Database`.

        :param database: database to use
        :param collection: root collection to use

        .. versionchanged:: 4.0
           Removed the `disable_md5` parameter. See
           :ref:`removed-gridfs-checksum` for details.

        .. versionchanged:: 3.11
           Running a GridFS operation in a transaction now always raises an
           error. GridFS does not support multi-document transactions.

        .. versionchanged:: 3.7
           Added the `disable_md5` parameter.

        .. versionchanged:: 3.1
           Indexes are only ensured on the first write to the DB.

        .. versionchanged:: 3.0
           `database` must use an acknowledged
           :attr:`~pymongo.database.Database.write_concern`

        .. seealso:: The MongoDB documentation on `gridfs <https://dochub.mongodb.org/core/gridfs>`_.
        """
        if not isinstance(database, Database):
            raise TypeError("database must be an instance of Database")

        database = _clear_entity_type_registry(database)

        if not database.write_concern.acknowledged:
            raise ConfigurationError("database must use acknowledged write_concern")

        self.__collection = database[collection]
        self.__files = self.__collection.files
        self.__chunks = self.__collection.chunks

    def new_file(self, **kwargs: Any) -> GridIn:
        """Create a new file in GridFS.

        Returns a new :class:`~gridfs.grid_file.GridIn` instance to
        which data can be written. Any keyword arguments will be
        passed through to :meth:`~gridfs.grid_file.GridIn`.

        If the ``"_id"`` of the file is manually specified, it must
        not already exist in GridFS. Otherwise
        :class:`~gridfs.errors.FileExists` is raised.

        :param kwargs: keyword arguments for file creation
        """
        return GridIn(self.__collection, **kwargs)

    def put(self, data: Any, **kwargs: Any) -> Any:
        """Put data in GridFS as a new file.

        Equivalent to doing::

          with fs.new_file(**kwargs) as f:
              f.write(data)

        `data` can be either an instance of :class:`bytes` or a file-like
        object providing a :meth:`read` method. If an `encoding` keyword
        argument is passed, `data` can also be a :class:`str` instance, which
        will be encoded as `encoding` before being written. Any keyword
        arguments will be passed through to the created file - see
        :meth:`~gridfs.grid_file.GridIn` for possible arguments. Returns the
        ``"_id"`` of the created file.

        If the ``"_id"`` of the file is manually specified, it must
        not already exist in GridFS. Otherwise
        :class:`~gridfs.errors.FileExists` is raised.

        :param data: data to be written as a file.
        :param kwargs: keyword arguments for file creation

        .. versionchanged:: 3.0
           w=0 writes to GridFS are now prohibited.
        """
        with GridIn(self.__collection, **kwargs) as grid_file:
            grid_file.write(data)
            return grid_file._id

    def get(self, file_id: Any, session: Optional[ClientSession] = None) -> GridOut:
        """Get a file from GridFS by ``"_id"``.

        Returns an instance of :class:`~gridfs.grid_file.GridOut`,
        which provides a file-like interface for reading.

        :param file_id: ``"_id"`` of the file to get
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        gout = GridOut(self.__collection, file_id, session=session)

        # Raise NoFile now, instead of on first attribute access.
        gout._ensure_file()
        return gout

    def get_version(
        self,
        filename: Optional[str] = None,
        version: Optional[int] = -1,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> GridOut:
        """Get a file from GridFS by ``"filename"`` or metadata fields.

        Returns a version of the file in GridFS whose filename matches
        `filename` and whose metadata fields match the supplied keyword
        arguments, as an instance of :class:`~gridfs.grid_file.GridOut`.

        Version numbering is a convenience atop the GridFS API provided
        by MongoDB. If more than one file matches the query (either by
        `filename` alone, by metadata fields, or by a combination of
        both), then version ``-1`` will be the most recently uploaded
        matching file, ``-2`` the second most recently
        uploaded, etc. Version ``0`` will be the first version
        uploaded, ``1`` the second version, etc. So if three versions
        have been uploaded, then version ``0`` is the same as version
        ``-3``, version ``1`` is the same as version ``-2``, and
        version ``2`` is the same as version ``-1``.

        Raises :class:`~gridfs.errors.NoFile` if no such version of
        that file exists.

        :param filename: ``"filename"`` of the file to get, or `None`
        :param version: version of the file to get (defaults
            to -1, the most recent version uploaded)
        :param session: a
            :class:`~pymongo.client_session.ClientSession`
        :param kwargs: find files by custom metadata.

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.1
           ``get_version`` no longer ensures indexes.
        """
        query = kwargs
        if filename is not None:
            query["filename"] = filename

        _disallow_transactions(session)
        cursor = self.__files.find(query, session=session)
        if version is None:
            version = -1
        if version < 0:
            skip = abs(version) - 1
            cursor.limit(-1).skip(skip).sort("uploadDate", DESCENDING)
        else:
            cursor.limit(-1).skip(version).sort("uploadDate", ASCENDING)
        try:
            doc = next(cursor)
            return GridOut(self.__collection, file_document=doc, session=session)
        except StopIteration:
            raise NoFile("no version %d for filename %r" % (version, filename)) from None

    def get_last_version(
        self, filename: Optional[str] = None, session: Optional[ClientSession] = None, **kwargs: Any
    ) -> GridOut:
        """Get the most recent version of a file in GridFS by ``"filename"``
        or metadata fields.

        Equivalent to calling :meth:`get_version` with the default
        `version` (``-1``).

        :param filename: ``"filename"`` of the file to get, or `None`
        :param session: a
            :class:`~pymongo.client_session.ClientSession`
        :param kwargs: find files by custom metadata.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        return self.get_version(filename=filename, session=session, **kwargs)

    # TODO add optional safe mode for chunk removal?
    def delete(self, file_id: Any, session: Optional[ClientSession] = None) -> None:
        """Delete a file from GridFS by ``"_id"``.

        Deletes all data belonging to the file with ``"_id"``:
        `file_id`.

        .. warning:: Any processes/threads reading from the file while
           this method is executing will likely see an invalid/corrupt
           file. Care should be taken to avoid concurrent reads to a file
           while it is being deleted.

        .. note:: Deletes of non-existent files are considered successful
           since the end result is the same: no file with that _id remains.

        :param file_id: ``"_id"`` of the file to delete
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.1
           ``delete`` no longer ensures indexes.
        """
        _disallow_transactions(session)
        self.__files.delete_one({"_id": file_id}, session=session)
        self.__chunks.delete_many({"files_id": file_id}, session=session)

    def list(self, session: Optional[ClientSession] = None) -> list[str]:
        """List the names of all files stored in this instance of
        :class:`GridFS`.

        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.

        .. versionchanged:: 3.1
           ``list`` no longer ensures indexes.
        """
        _disallow_transactions(session)
        # With an index, distinct includes documents with no filename
        # as None.
        return [
            name for name in self.__files.distinct("filename", session=session) if name is not None
        ]

    def find_one(
        self,
        filter: Optional[Any] = None,
        session: Optional[ClientSession] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Optional[GridOut]:
        """Get a single file from gridfs.

        All arguments to :meth:`find` are also valid arguments for
        :meth:`find_one`, although any `limit` argument will be
        ignored. Returns a single :class:`~gridfs.grid_file.GridOut`,
        or ``None`` if no matching file is found. For example:

        .. code-block: python

            file = fs.find_one({"filename": "lisa.txt"})

        :param filter: a dictionary specifying
            the query to be performing OR any other type to be used as
            the value for a query for ``"_id"`` in the file collection.
        :param args: any additional positional arguments are
            the same as the arguments to :meth:`find`.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`
        :param kwargs: any additional keyword arguments
            are the same as the arguments to :meth:`find`.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        if filter is not None and not isinstance(filter, abc.Mapping):
            filter = {"_id": filter}

        _disallow_transactions(session)
        for f in self.find(filter, *args, session=session, **kwargs):
            return f

        return None

    def find(self, *args: Any, **kwargs: Any) -> GridOutCursor:
        """Query GridFS for files.

        Returns a cursor that iterates across files matching
        arbitrary queries on the files collection. Can be combined
        with other modifiers for additional control. For example::

          for grid_out in fs.find({"filename": "lisa.txt"},
                                  no_cursor_timeout=True):
              data = grid_out.read()

        would iterate through all versions of "lisa.txt" stored in GridFS.
        Note that setting no_cursor_timeout to True may be important to
        prevent the cursor from timing out during long multi-file processing
        work.

        As another example, the call::

          most_recent_three = fs.find().sort("uploadDate", -1).limit(3)

        would return a cursor to the three most recently uploaded files
        in GridFS.

        Follows a similar interface to
        :meth:`~pymongo.collection.Collection.find`
        in :class:`~pymongo.collection.Collection`.

        If a :class:`~pymongo.client_session.ClientSession` is passed to
        :meth:`find`, all returned :class:`~gridfs.grid_file.GridOut` instances
        are associated with that session.

        :param filter: A query document that selects which files
            to include in the result set. Can be an empty document to include
            all files.
        :param skip: the number of files to omit (from
            the start of the result set) when returning the results
        :param limit: the maximum number of results to
            return
        :param no_cursor_timeout: if False (the default), any
            returned cursor is closed by the server after 10 minutes of
            inactivity. If set to True, the returned cursor will never
            time out on the server. Care should be taken to ensure that
            cursors with no_cursor_timeout turned on are properly closed.
        :param sort: a list of (key, direction) pairs
            specifying the sort order for this query. See
            :meth:`~pymongo.cursor.Cursor.sort` for details.

        Raises :class:`TypeError` if any of the arguments are of
        improper type. Returns an instance of
        :class:`~gridfs.grid_file.GridOutCursor`
        corresponding to this query.

        .. versionchanged:: 3.0
           Removed the read_preference, tag_sets, and
           secondary_acceptable_latency_ms options.
        .. versionadded:: 2.7
        .. seealso:: The MongoDB documentation on `find <https://dochub.mongodb.org/core/find>`_.
        """
        return GridOutCursor(self.__collection, *args, **kwargs)

    def exists(
        self,
        document_or_id: Optional[Any] = None,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> bool:
        """Check if a file exists in this instance of :class:`GridFS`.

        The file to check for can be specified by the value of its
        ``_id`` key, or by passing in a query document. A query
        document can be passed in as dictionary, or by using keyword
        arguments. Thus, the following three calls are equivalent:

        >>> fs.exists(file_id)
        >>> fs.exists({"_id": file_id})
        >>> fs.exists(_id=file_id)

        As are the following two calls:

        >>> fs.exists({"filename": "mike.txt"})
        >>> fs.exists(filename="mike.txt")

        And the following two:

        >>> fs.exists({"foo": {"$gt": 12}})
        >>> fs.exists(foo={"$gt": 12})

        Returns ``True`` if a matching file exists, ``False``
        otherwise. Calls to :meth:`exists` will not automatically
        create appropriate indexes; application developers should be
        sure to create indexes if needed and as appropriate.

        :param document_or_id: query document, or _id of the
            document to check for
        :param session: a
            :class:`~pymongo.client_session.ClientSession`
        :param kwargs: keyword arguments are used as a
            query document, if they're present.

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        _disallow_transactions(session)
        if kwargs:
            f = self.__files.find_one(kwargs, ["_id"], session=session)
        else:
            f = self.__files.find_one(document_or_id, ["_id"], session=session)

        return f is not None


class GridFSBucket:
    """An instance of GridFS on top of a single Database."""

    def __init__(
        self,
        db: Database,
        bucket_name: str = "fs",
        chunk_size_bytes: int = DEFAULT_CHUNK_SIZE,
        write_concern: Optional[WriteConcern] = None,
        read_preference: Optional[_ServerMode] = None,
    ) -> None:
        """Create a new instance of :class:`GridFSBucket`.

        Raises :exc:`TypeError` if `database` is not an instance of
        :class:`~pymongo.database.Database`.

        Raises :exc:`~pymongo.errors.ConfigurationError` if `write_concern`
        is not acknowledged.

        :param database: database to use.
        :param bucket_name: The name of the bucket. Defaults to 'fs'.
        :param chunk_size_bytes: The chunk size in bytes. Defaults
            to 255KB.
        :param write_concern: The
            :class:`~pymongo.write_concern.WriteConcern` to use. If ``None``
            (the default) db.write_concern is used.
        :param read_preference: The read preference to use. If
            ``None`` (the default) db.read_preference is used.

        .. versionchanged:: 4.0
           Removed the `disable_md5` parameter. See
           :ref:`removed-gridfs-checksum` for details.

        .. versionchanged:: 3.11
           Running a GridFSBucket operation in a transaction now always raises
           an error. GridFSBucket does not support multi-document transactions.

        .. versionchanged:: 3.7
           Added the `disable_md5` parameter.

        .. versionadded:: 3.1

        .. seealso:: The MongoDB documentation on `gridfs <https://dochub.mongodb.org/core/gridfs>`_.
        """
        if not isinstance(db, Database):
            raise TypeError("database must be an instance of Database")

        db = _clear_entity_type_registry(db)

        wtc = write_concern if write_concern is not None else db.write_concern
        if not wtc.acknowledged:
            raise ConfigurationError("write concern must be acknowledged")

        self._bucket_name = bucket_name
        self._collection = db[bucket_name]
        self._chunks: Collection = self._collection.chunks.with_options(
            write_concern=write_concern, read_preference=read_preference
        )

        self._files: Collection = self._collection.files.with_options(
            write_concern=write_concern, read_preference=read_preference
        )

        self._chunk_size_bytes = chunk_size_bytes
        self._timeout = db.client.options.timeout

    def open_upload_stream(
        self,
        filename: str,
        chunk_size_bytes: Optional[int] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
    ) -> GridIn:
        """Opens a Stream that the application can write the contents of the
        file to.

        The user must specify the filename, and can choose to add any
        additional information in the metadata field of the file document or
        modify the chunk size.
        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          with fs.open_upload_stream(
                "test_file", chunk_size_bytes=4,
                metadata={"contentType": "text/plain"}) as grid_in:
              grid_in.write("data I want to store!")
          # uploaded on close

        Returns an instance of :class:`~gridfs.grid_file.GridIn`.

        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.

        :param filename: The name of the file to upload.
        :param chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes in :class:`GridFSBucket`.
        :param metadata: User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        validate_string("filename", filename)

        opts = {
            "filename": filename,
            "chunk_size": (
                chunk_size_bytes if chunk_size_bytes is not None else self._chunk_size_bytes
            ),
        }
        if metadata is not None:
            opts["metadata"] = metadata

        return GridIn(self._collection, session=session, **opts)

    def open_upload_stream_with_id(
        self,
        file_id: Any,
        filename: str,
        chunk_size_bytes: Optional[int] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
    ) -> GridIn:
        """Opens a Stream that the application can write the contents of the
        file to.

        The user must specify the file id and filename, and can choose to add
        any additional information in the metadata field of the file document
        or modify the chunk size.
        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          with fs.open_upload_stream_with_id(
                ObjectId(),
                "test_file",
                chunk_size_bytes=4,
                metadata={"contentType": "text/plain"}) as grid_in:
              grid_in.write("data I want to store!")
          # uploaded on close

        Returns an instance of :class:`~gridfs.grid_file.GridIn`.

        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.

        :param file_id: The id to use for this file. The id must not have
            already been used for another file.
        :param filename: The name of the file to upload.
        :param chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes in :class:`GridFSBucket`.
        :param metadata: User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        validate_string("filename", filename)

        opts = {
            "_id": file_id,
            "filename": filename,
            "chunk_size": (
                chunk_size_bytes if chunk_size_bytes is not None else self._chunk_size_bytes
            ),
        }
        if metadata is not None:
            opts["metadata"] = metadata

        return GridIn(self._collection, session=session, **opts)

    @_csot.apply
    def upload_from_stream(
        self,
        filename: str,
        source: Any,
        chunk_size_bytes: Optional[int] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
    ) -> ObjectId:
        """Uploads a user file to a GridFS bucket.

        Reads the contents of the user file from `source` and uploads
        it to the file `filename`. Source can be a string or file-like object.
        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          file_id = fs.upload_from_stream(
              "test_file",
              "data I want to store!",
              chunk_size_bytes=4,
              metadata={"contentType": "text/plain"})

        Returns the _id of the uploaded file.

        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.

        :param filename: The name of the file to upload.
        :param source: The source stream of the content to be uploaded. Must be
            a file-like object that implements :meth:`read` or a string.
        :param chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes of :class:`GridFSBucket`.
        :param metadata: User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        with self.open_upload_stream(filename, chunk_size_bytes, metadata, session=session) as gin:
            gin.write(source)

        return cast(ObjectId, gin._id)

    @_csot.apply
    def upload_from_stream_with_id(
        self,
        file_id: Any,
        filename: str,
        source: Any,
        chunk_size_bytes: Optional[int] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Uploads a user file to a GridFS bucket with a custom file id.

        Reads the contents of the user file from `source` and uploads
        it to the file `filename`. Source can be a string or file-like object.
        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          file_id = fs.upload_from_stream(
              ObjectId(),
              "test_file",
              "data I want to store!",
              chunk_size_bytes=4,
              metadata={"contentType": "text/plain"})

        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.
        Raises :exc:`~ValueError` if `filename` is not a string.

        :param file_id: The id to use for this file. The id must not have
            already been used for another file.
        :param filename: The name of the file to upload.
        :param source: The source stream of the content to be uploaded. Must be
            a file-like object that implements :meth:`read` or a string.
        :param chunk_size_bytes` (options): The number of bytes per chunk of this
            file. Defaults to the chunk_size_bytes of :class:`GridFSBucket`.
        :param metadata: User data for the 'metadata' field of the
            files collection document. If not provided the metadata field will
            be omitted from the files collection document.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        with self.open_upload_stream_with_id(
            file_id, filename, chunk_size_bytes, metadata, session=session
        ) as gin:
            gin.write(source)

    def open_download_stream(
        self, file_id: Any, session: Optional[ClientSession] = None
    ) -> GridOut:
        """Opens a Stream from which the application can read the contents of
        the stored file specified by file_id.

        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # get _id of file to read.
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          grid_out = fs.open_download_stream(file_id)
          contents = grid_out.read()

        Returns an instance of :class:`~gridfs.grid_file.GridOut`.

        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.

        :param file_id: The _id of the file to be downloaded.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        gout = GridOut(self._collection, file_id, session=session)

        # Raise NoFile now, instead of on first attribute access.
        gout._ensure_file()
        return gout

    @_csot.apply
    def download_to_stream(
        self, file_id: Any, destination: Any, session: Optional[ClientSession] = None
    ) -> None:
        """Downloads the contents of the stored file specified by file_id and
        writes the contents to `destination`.

        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get _id of file to read
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          # Get file to write to
          file = open('myfile','wb+')
          fs.download_to_stream(file_id, file)
          file.seek(0)
          contents = file.read()

        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.

        :param file_id: The _id of the file to be downloaded.
        :param destination: a file-like object implementing :meth:`write`.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        with self.open_download_stream(file_id, session=session) as gout:
            while True:
                chunk = gout.readchunk()
                if not len(chunk):
                    break
                destination.write(chunk)

    @_csot.apply
    def delete(self, file_id: Any, session: Optional[ClientSession] = None) -> None:
        """Given an file_id, delete this stored file's files collection document
        and associated chunks from a GridFS bucket.

        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get _id of file to delete
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          fs.delete(file_id)

        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.

        :param file_id: The _id of the file to be deleted.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        _disallow_transactions(session)
        res = self._files.delete_one({"_id": file_id}, session=session)
        self._chunks.delete_many({"files_id": file_id}, session=session)
        if not res.deleted_count:
            raise NoFile("no file could be deleted because none matched %s" % file_id)

    def find(self, *args: Any, **kwargs: Any) -> GridOutCursor:
        """Find and return the files collection documents that match ``filter``

        Returns a cursor that iterates across files matching
        arbitrary queries on the files collection. Can be combined
        with other modifiers for additional control.

        For example::

          for grid_data in fs.find({"filename": "lisa.txt"},
                                  no_cursor_timeout=True):
              data = grid_data.read()

        would iterate through all versions of "lisa.txt" stored in GridFS.
        Note that setting no_cursor_timeout to True may be important to
        prevent the cursor from timing out during long multi-file processing
        work.

        As another example, the call::

          most_recent_three = fs.find().sort("uploadDate", -1).limit(3)

        would return a cursor to the three most recently uploaded files
        in GridFS.

        Follows a similar interface to
        :meth:`~pymongo.collection.Collection.find`
        in :class:`~pymongo.collection.Collection`.

        If a :class:`~pymongo.client_session.ClientSession` is passed to
        :meth:`find`, all returned :class:`~gridfs.grid_file.GridOut` instances
        are associated with that session.

        :param filter: Search query.
        :param batch_size: The number of documents to return per
            batch.
        :param limit: The maximum number of documents to return.
        :param no_cursor_timeout: The server normally times out idle
            cursors after an inactivity period (10 minutes) to prevent excess
            memory use. Set this option to True prevent that.
        :param skip: The number of documents to skip before
            returning.
        :param sort: The order by which to sort results. Defaults to
            None.
        """
        return GridOutCursor(self._collection, *args, **kwargs)

    def open_download_stream_by_name(
        self, filename: str, revision: int = -1, session: Optional[ClientSession] = None
    ) -> GridOut:
        """Opens a Stream from which the application can read the contents of
        `filename` and optional `revision`.

        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          grid_out = fs.open_download_stream_by_name("test_file")
          contents = grid_out.read()

        Returns an instance of :class:`~gridfs.grid_file.GridOut`.

        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.

        Raises :exc:`~ValueError` filename is not a string.

        :param filename: The name of the file to read from.
        :param revision: Which revision (documents with the same
            filename and different uploadDate) of the file to retrieve.
            Defaults to -1 (the most recent revision).
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        :Note: Revision numbers are defined as follows:

          - 0 = the original stored file
          - 1 = the first revision
          - 2 = the second revision
          - etc...
          - -2 = the second most recent revision
          - -1 = the most recent revision

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        validate_string("filename", filename)
        query = {"filename": filename}
        _disallow_transactions(session)
        cursor = self._files.find(query, session=session)
        if revision < 0:
            skip = abs(revision) - 1
            cursor.limit(-1).skip(skip).sort("uploadDate", DESCENDING)
        else:
            cursor.limit(-1).skip(revision).sort("uploadDate", ASCENDING)
        try:
            grid_file = next(cursor)
            return GridOut(self._collection, file_document=grid_file, session=session)
        except StopIteration:
            raise NoFile("no version %d for filename %r" % (revision, filename)) from None

    @_csot.apply
    def download_to_stream_by_name(
        self,
        filename: str,
        destination: Any,
        revision: int = -1,
        session: Optional[ClientSession] = None,
    ) -> None:
        """Write the contents of `filename` (with optional `revision`) to
        `destination`.

        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get file to write to
          file = open('myfile','wb')
          fs.download_to_stream_by_name("test_file", file)

        Raises :exc:`~gridfs.errors.NoFile` if no such version of
        that file exists.

        Raises :exc:`~ValueError` if `filename` is not a string.

        :param filename: The name of the file to read from.
        :param destination: A file-like object that implements :meth:`write`.
        :param revision: Which revision (documents with the same
            filename and different uploadDate) of the file to retrieve.
            Defaults to -1 (the most recent revision).
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        :Note: Revision numbers are defined as follows:

          - 0 = the original stored file
          - 1 = the first revision
          - 2 = the second revision
          - etc...
          - -2 = the second most recent revision
          - -1 = the most recent revision

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        with self.open_download_stream_by_name(filename, revision, session=session) as gout:
            while True:
                chunk = gout.readchunk()
                if not len(chunk):
                    break
                destination.write(chunk)

    def rename(
        self, file_id: Any, new_filename: str, session: Optional[ClientSession] = None
    ) -> None:
        """Renames the stored file with the specified file_id.

        For example::

          my_db = MongoClient().test
          fs = GridFSBucket(my_db)
          # Get _id of file to rename
          file_id = fs.upload_from_stream("test_file", "data I want to store!")
          fs.rename(file_id, "new_test_name")

        Raises :exc:`~gridfs.errors.NoFile` if no file with file_id exists.

        :param file_id: The _id of the file to be renamed.
        :param new_filename: The new name of the file.
        :param session: a
            :class:`~pymongo.client_session.ClientSession`

        .. versionchanged:: 3.6
           Added ``session`` parameter.
        """
        _disallow_transactions(session)
        result = self._files.update_one(
            {"_id": file_id}, {"$set": {"filename": new_filename}}, session=session
        )
        if not result.matched_count:
            raise NoFile(
                "no files could be renamed %r because none "
                "matched file_id %i" % (new_filename, file_id)
            )
