# Copyright 2019-present MongoDB, Inc.
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

"""Support for explicit client-side field level encryption."""
from __future__ import annotations

import contextlib
import enum
import socket
import uuid
import weakref
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterator,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    cast,
)

try:
    from pymongocrypt.auto_encrypter import AutoEncrypter  # type:ignore[import]
    from pymongocrypt.errors import MongoCryptError  # type:ignore[import]
    from pymongocrypt.explicit_encrypter import ExplicitEncrypter  # type:ignore[import]
    from pymongocrypt.mongocrypt import MongoCryptOptions  # type:ignore[import]
    from pymongocrypt.state_machine import MongoCryptCallback  # type:ignore[import]

    _HAVE_PYMONGOCRYPT = True
except ImportError:
    _HAVE_PYMONGOCRYPT = False
    MongoCryptCallback = object

from bson import _dict_to_bson, decode, encode
from bson.binary import STANDARD, UUID_SUBTYPE, Binary
from bson.codec_options import CodecOptions
from bson.errors import BSONError
from bson.raw_bson import DEFAULT_RAW_BSON_OPTIONS, RawBSONDocument, _inflate_bson
from pymongo import _csot
from pymongo.collection import Collection
from pymongo.common import CONNECT_TIMEOUT
from pymongo.cursor import Cursor
from pymongo.daemon import _spawn_daemon
from pymongo.database import Database
from pymongo.encryption_options import AutoEncryptionOpts, RangeOpts
from pymongo.errors import (
    ConfigurationError,
    EncryptedCollectionError,
    EncryptionError,
    InvalidOperation,
    PyMongoError,
    ServerSelectionTimeoutError,
)
from pymongo.mongo_client import MongoClient
from pymongo.network import BLOCKING_IO_ERRORS
from pymongo.operations import UpdateOne
from pymongo.pool import PoolOptions, _configured_socket, _raise_connection_failure
from pymongo.read_concern import ReadConcern
from pymongo.results import BulkWriteResult, DeleteResult
from pymongo.ssl_support import get_ssl_context
from pymongo.typings import _DocumentType, _DocumentTypeArg
from pymongo.uri_parser import parse_host
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from pymongocrypt.mongocrypt import MongoCryptKmsContext

_HTTPS_PORT = 443
_KMS_CONNECT_TIMEOUT = CONNECT_TIMEOUT  # CDRIVER-3262 redefined this value to CONNECT_TIMEOUT
_MONGOCRYPTD_TIMEOUT_MS = 10000

_DATA_KEY_OPTS: CodecOptions[dict[str, Any]] = CodecOptions(
    document_class=Dict[str, Any], uuid_representation=STANDARD
)
# Use RawBSONDocument codec options to avoid needlessly decoding
# documents from the key vault.
_KEY_VAULT_OPTS = CodecOptions(document_class=RawBSONDocument)


@contextlib.contextmanager
def _wrap_encryption_errors() -> Iterator[None]:
    """Context manager to wrap encryption related errors."""
    try:
        yield
    except BSONError:
        # BSON encoding/decoding errors are unrelated to encryption so
        # we should propagate them unchanged.
        raise
    except Exception as exc:
        raise EncryptionError(exc) from exc


class _EncryptionIO(MongoCryptCallback):  # type: ignore[misc]
    def __init__(
        self,
        client: Optional[MongoClient[_DocumentTypeArg]],
        key_vault_coll: Collection[_DocumentTypeArg],
        mongocryptd_client: Optional[MongoClient[_DocumentTypeArg]],
        opts: AutoEncryptionOpts,
    ):
        """Internal class to perform I/O on behalf of pymongocrypt."""
        self.client_ref: Any
        # Use a weak ref to break reference cycle.
        if client is not None:
            self.client_ref = weakref.ref(client)
        else:
            self.client_ref = None
        self.key_vault_coll: Optional[Collection[RawBSONDocument]] = cast(
            Collection[RawBSONDocument],
            key_vault_coll.with_options(
                codec_options=_KEY_VAULT_OPTS,
                read_concern=ReadConcern(level="majority"),
                write_concern=WriteConcern(w="majority"),
            ),
        )
        self.mongocryptd_client = mongocryptd_client
        self.opts = opts
        self._spawned = False

    def kms_request(self, kms_context: MongoCryptKmsContext) -> None:
        """Complete a KMS request.

        :param kms_context: A :class:`MongoCryptKmsContext`.

        :return: None
        """
        endpoint = kms_context.endpoint
        message = kms_context.message
        provider = kms_context.kms_provider
        ctx = self.opts._kms_ssl_contexts.get(provider)
        if ctx is None:
            # Enable strict certificate verification, OCSP, match hostname, and
            # SNI using the system default CA certificates.
            ctx = get_ssl_context(
                None,  # certfile
                None,  # passphrase
                None,  # ca_certs
                None,  # crlfile
                False,  # allow_invalid_certificates
                False,  # allow_invalid_hostnames
                False,
            )  # disable_ocsp_endpoint_check
        # CSOT: set timeout for socket creation.
        connect_timeout = max(_csot.clamp_remaining(_KMS_CONNECT_TIMEOUT), 0.001)
        opts = PoolOptions(
            connect_timeout=connect_timeout,
            socket_timeout=connect_timeout,
            ssl_context=ctx,
        )
        host, port = parse_host(endpoint, _HTTPS_PORT)
        try:
            conn = _configured_socket((host, port), opts)
            try:
                conn.sendall(message)
                while kms_context.bytes_needed > 0:
                    # CSOT: update timeout.
                    conn.settimeout(max(_csot.clamp_remaining(_KMS_CONNECT_TIMEOUT), 0))
                    data = conn.recv(kms_context.bytes_needed)
                    if not data:
                        raise OSError("KMS connection closed")
                    kms_context.feed(data)
            except BLOCKING_IO_ERRORS:
                raise socket.timeout("timed out") from None
            finally:
                conn.close()
        except (PyMongoError, MongoCryptError):
            raise  # Propagate pymongo errors directly.
        except Exception as error:
            # Wrap I/O errors in PyMongo exceptions.
            _raise_connection_failure((host, port), error)

    def collection_info(
        self, database: Database[Mapping[str, Any]], filter: bytes
    ) -> Optional[bytes]:
        """Get the collection info for a namespace.

        The returned collection info is passed to libmongocrypt which reads
        the JSON schema.

        :param database: The database on which to run listCollections.
        :param filter: The filter to pass to listCollections.

        :return: The first document from the listCollections command response as BSON.
        """
        with self.client_ref()[database].list_collections(filter=RawBSONDocument(filter)) as cursor:
            for doc in cursor:
                return _dict_to_bson(doc, False, _DATA_KEY_OPTS)
            return None

    def spawn(self) -> None:
        """Spawn mongocryptd.

        Note this method is thread safe; at most one mongocryptd will start
        successfully.
        """
        self._spawned = True
        args = [self.opts._mongocryptd_spawn_path or "mongocryptd"]
        args.extend(self.opts._mongocryptd_spawn_args)
        _spawn_daemon(args)

    def mark_command(self, database: str, cmd: bytes) -> bytes:
        """Mark a command for encryption.

        :param database: The database on which to run this command.
        :param cmd: The BSON command to run.

        :return: The marked command response from mongocryptd.
        """
        if not self._spawned and not self.opts._mongocryptd_bypass_spawn:
            self.spawn()
        # Database.command only supports mutable mappings so we need to decode
        # the raw BSON command first.
        inflated_cmd = _inflate_bson(cmd, DEFAULT_RAW_BSON_OPTIONS)
        assert self.mongocryptd_client is not None
        try:
            res = self.mongocryptd_client[database].command(
                inflated_cmd, codec_options=DEFAULT_RAW_BSON_OPTIONS
            )
        except ServerSelectionTimeoutError:
            if self.opts._mongocryptd_bypass_spawn:
                raise
            self.spawn()
            res = self.mongocryptd_client[database].command(
                inflated_cmd, codec_options=DEFAULT_RAW_BSON_OPTIONS
            )
        return res.raw

    def fetch_keys(self, filter: bytes) -> Iterator[bytes]:
        """Yields one or more keys from the key vault.

        :param filter: The filter to pass to find.

        :return: A generator which yields the requested keys from the key vault.
        """
        assert self.key_vault_coll is not None
        with self.key_vault_coll.find(RawBSONDocument(filter)) as cursor:
            for key in cursor:
                yield key.raw

    def insert_data_key(self, data_key: bytes) -> Binary:
        """Insert a data key into the key vault.

        :param data_key: The data key document to insert.

        :return: The _id of the inserted data key document.
        """
        raw_doc = RawBSONDocument(data_key, _KEY_VAULT_OPTS)
        data_key_id = raw_doc.get("_id")
        if not isinstance(data_key_id, Binary) or data_key_id.subtype != UUID_SUBTYPE:
            raise TypeError("data_key _id must be Binary with a UUID subtype")

        assert self.key_vault_coll is not None
        self.key_vault_coll.insert_one(raw_doc)
        return data_key_id

    def bson_encode(self, doc: MutableMapping[str, Any]) -> bytes:
        """Encode a document to BSON.

        A document can be any mapping type (like :class:`dict`).

        :param doc: mapping type representing a document

        :return: The encoded BSON bytes.
        """
        return encode(doc)

    def close(self) -> None:
        """Release resources.

        Note it is not safe to call this method from __del__ or any GC hooks.
        """
        self.client_ref = None
        self.key_vault_coll = None
        if self.mongocryptd_client:
            self.mongocryptd_client.close()
            self.mongocryptd_client = None


class RewrapManyDataKeyResult:
    """Result object returned by a :meth:`~ClientEncryption.rewrap_many_data_key` operation.

    .. versionadded:: 4.2
    """

    def __init__(self, bulk_write_result: Optional[BulkWriteResult] = None) -> None:
        self._bulk_write_result = bulk_write_result

    @property
    def bulk_write_result(self) -> Optional[BulkWriteResult]:
        """The result of the bulk write operation used to update the key vault
        collection with one or more rewrapped data keys. If
        :meth:`~ClientEncryption.rewrap_many_data_key` does not find any matching keys to rewrap,
        no bulk write operation will be executed and this field will be
        ``None``.
        """
        return self._bulk_write_result

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._bulk_write_result!r})"


class _Encrypter:
    """Encrypts and decrypts MongoDB commands.

    This class is used to support automatic encryption and decryption of
    MongoDB commands.
    """

    def __init__(self, client: MongoClient[_DocumentTypeArg], opts: AutoEncryptionOpts):
        """Create a _Encrypter for a client.

        :param client: The encrypted MongoClient.
        :param opts: The encrypted client's :class:`AutoEncryptionOpts`.
        """
        if opts._schema_map is None:
            schema_map = None
        else:
            schema_map = _dict_to_bson(opts._schema_map, False, _DATA_KEY_OPTS)

        if opts._encrypted_fields_map is None:
            encrypted_fields_map = None
        else:
            encrypted_fields_map = _dict_to_bson(opts._encrypted_fields_map, False, _DATA_KEY_OPTS)
        self._bypass_auto_encryption = opts._bypass_auto_encryption
        self._internal_client = None

        def _get_internal_client(
            encrypter: _Encrypter, mongo_client: MongoClient[_DocumentTypeArg]
        ) -> MongoClient[_DocumentTypeArg]:
            if mongo_client.options.pool_options.max_pool_size is None:
                # Unlimited pool size, use the same client.
                return mongo_client
            # Else - limited pool size, use an internal client.
            if encrypter._internal_client is not None:
                return encrypter._internal_client
            internal_client = mongo_client._duplicate(minPoolSize=0, auto_encryption_opts=None)
            encrypter._internal_client = internal_client
            return internal_client

        if opts._key_vault_client is not None:
            key_vault_client = opts._key_vault_client
        else:
            key_vault_client = _get_internal_client(self, client)

        if opts._bypass_auto_encryption:
            metadata_client = None
        else:
            metadata_client = _get_internal_client(self, client)

        db, coll = opts._key_vault_namespace.split(".", 1)
        key_vault_coll = key_vault_client[db][coll]

        mongocryptd_client: MongoClient[Mapping[str, Any]] = MongoClient(
            opts._mongocryptd_uri, connect=False, serverSelectionTimeoutMS=_MONGOCRYPTD_TIMEOUT_MS
        )

        io_callbacks = _EncryptionIO(  # type:ignore[misc]
            metadata_client, key_vault_coll, mongocryptd_client, opts
        )
        self._auto_encrypter = AutoEncrypter(
            io_callbacks,
            MongoCryptOptions(
                opts._kms_providers,
                schema_map,
                crypt_shared_lib_path=opts._crypt_shared_lib_path,
                crypt_shared_lib_required=opts._crypt_shared_lib_required,
                bypass_encryption=opts._bypass_auto_encryption,
                encrypted_fields_map=encrypted_fields_map,
                bypass_query_analysis=opts._bypass_query_analysis,
            ),
        )
        self._closed = False

    def encrypt(
        self, database: str, cmd: Mapping[str, Any], codec_options: CodecOptions[_DocumentTypeArg]
    ) -> dict[str, Any]:
        """Encrypt a MongoDB command.

        :param database: The database for this command.
        :param cmd: A command document.
        :param codec_options: The CodecOptions to use while encoding `cmd`.

        :return: The encrypted command to execute.
        """
        self._check_closed()
        encoded_cmd = _dict_to_bson(cmd, False, codec_options)
        with _wrap_encryption_errors():
            encrypted_cmd = self._auto_encrypter.encrypt(database, encoded_cmd)
            # TODO: PYTHON-1922 avoid decoding the encrypted_cmd.
            return _inflate_bson(encrypted_cmd, DEFAULT_RAW_BSON_OPTIONS)

    def decrypt(self, response: bytes) -> Optional[bytes]:
        """Decrypt a MongoDB command response.

        :param response: A MongoDB command response as BSON.

        :return: The decrypted command response.
        """
        self._check_closed()
        with _wrap_encryption_errors():
            return cast(bytes, self._auto_encrypter.decrypt(response))

    def _check_closed(self) -> None:
        if self._closed:
            raise InvalidOperation("Cannot use MongoClient after close")

    def close(self) -> None:
        """Cleanup resources."""
        self._closed = True
        self._auto_encrypter.close()
        if self._internal_client:
            self._internal_client.close()
            self._internal_client = None


class Algorithm(str, enum.Enum):
    """An enum that defines the supported encryption algorithms."""

    AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
    """AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic."""
    AEAD_AES_256_CBC_HMAC_SHA_512_Random = "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
    """AEAD_AES_256_CBC_HMAC_SHA_512_Random."""
    INDEXED = "Indexed"
    """Indexed.

    .. versionadded:: 4.2
    """
    UNINDEXED = "Unindexed"
    """Unindexed.

    .. versionadded:: 4.2
    """
    RANGEPREVIEW = "RangePreview"
    """RangePreview.

    .. note:: Support for Range queries is in beta.
       Backwards-breaking changes may be made before the final release.

    .. versionadded:: 4.4
    """


class QueryType(str, enum.Enum):
    """An enum that defines the supported values for explicit encryption query_type.

    .. versionadded:: 4.2
    """

    EQUALITY = "equality"
    """Used to encrypt a value for an equality query."""

    RANGEPREVIEW = "rangePreview"
    """Used to encrypt a value for a range query.

    .. note:: Support for Range queries is in beta.
       Backwards-breaking changes may be made before the final release.
"""


class ClientEncryption(Generic[_DocumentType]):
    """Explicit client-side field level encryption."""

    def __init__(
        self,
        kms_providers: Mapping[str, Any],
        key_vault_namespace: str,
        key_vault_client: MongoClient[_DocumentTypeArg],
        codec_options: CodecOptions[_DocumentTypeArg],
        kms_tls_options: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Explicit client-side field level encryption.

        The ClientEncryption class encapsulates explicit operations on a key
        vault collection that cannot be done directly on a MongoClient. Similar
        to configuring auto encryption on a MongoClient, it is constructed with
        a MongoClient (to a MongoDB cluster containing the key vault
        collection), KMS provider configuration, and keyVaultNamespace. It
        provides an API for explicitly encrypting and decrypting values, and
        creating data keys. It does not provide an API to query keys from the
        key vault collection, as this can be done directly on the MongoClient.

        See :ref:`explicit-client-side-encryption` for an example.

        :param kms_providers: Map of KMS provider options. The `kms_providers`
            map values differ by provider:

              - `aws`: Map with "accessKeyId" and "secretAccessKey" as strings.
                These are the AWS access key ID and AWS secret access key used
                to generate KMS messages. An optional "sessionToken" may be
                included to support temporary AWS credentials.
              - `azure`: Map with "tenantId", "clientId", and "clientSecret" as
                strings. Additionally, "identityPlatformEndpoint" may also be
                specified as a string (defaults to 'login.microsoftonline.com').
                These are the Azure Active Directory credentials used to
                generate Azure Key Vault messages.
              - `gcp`: Map with "email" as a string and "privateKey"
                as `bytes` or a base64 encoded string.
                Additionally, "endpoint" may also be specified as a string
                (defaults to 'oauth2.googleapis.com'). These are the
                credentials used to generate Google Cloud KMS messages.
              - `kmip`: Map with "endpoint" as a host with required port.
                For example: ``{"endpoint": "example.com:443"}``.
              - `local`: Map with "key" as `bytes` (96 bytes in length) or
                a base64 encoded string which decodes
                to 96 bytes. "key" is the master key used to encrypt/decrypt
                data keys. This key should be generated and stored as securely
                as possible.

            KMS providers may be specified with an optional name suffix
            separated by a colon, for example "kmip:name" or "aws:name".
            Named KMS providers do not support :ref:`CSFLE on-demand credentials`.
        :param key_vault_namespace: The namespace for the key vault collection.
            The key vault collection contains all data keys used for encryption
            and decryption. Data keys are stored as documents in this MongoDB
            collection. Data keys are protected with encryption by a KMS
            provider.
        :param key_vault_client: A MongoClient connected to a MongoDB cluster
            containing the `key_vault_namespace` collection.
        :param codec_options: An instance of
            :class:`~bson.codec_options.CodecOptions` to use when encoding a
            value for encryption and decoding the decrypted BSON value. This
            should be the same CodecOptions instance configured on the
            MongoClient, Database, or Collection used to access application
            data.
        :param kms_tls_options: A map of KMS provider names to TLS
            options to use when creating secure connections to KMS providers.
            Accepts the same TLS options as
            :class:`pymongo.mongo_client.MongoClient`. For example, to
            override the system default CA file::

              kms_tls_options={'kmip': {'tlsCAFile': certifi.where()}}

            Or to supply a client certificate::

              kms_tls_options={'kmip': {'tlsCertificateKeyFile': 'client.pem'}}

        .. versionchanged:: 4.0
           Added the `kms_tls_options` parameter and the "kmip" KMS provider.

        .. versionadded:: 3.9
        """
        if not _HAVE_PYMONGOCRYPT:
            raise ConfigurationError(
                "client-side field level encryption requires the pymongocrypt "
                "library: install a compatible version with: "
                "python -m pip install 'pymongo[encryption]'"
            )

        if not isinstance(codec_options, CodecOptions):
            raise TypeError("codec_options must be an instance of bson.codec_options.CodecOptions")

        self._kms_providers = kms_providers
        self._key_vault_namespace = key_vault_namespace
        self._key_vault_client = key_vault_client
        self._codec_options = codec_options

        db, coll = key_vault_namespace.split(".", 1)
        key_vault_coll = key_vault_client[db][coll]

        opts = AutoEncryptionOpts(
            kms_providers, key_vault_namespace, kms_tls_options=kms_tls_options
        )
        self._io_callbacks: Optional[_EncryptionIO] = _EncryptionIO(
            None, key_vault_coll, None, opts
        )
        self._encryption = ExplicitEncrypter(
            self._io_callbacks, MongoCryptOptions(kms_providers, None)
        )
        # Use the same key vault collection as the callback.
        assert self._io_callbacks.key_vault_coll is not None
        self._key_vault_coll = self._io_callbacks.key_vault_coll

    def create_encrypted_collection(
        self,
        database: Database[_DocumentTypeArg],
        name: str,
        encrypted_fields: Mapping[str, Any],
        kms_provider: Optional[str] = None,
        master_key: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> tuple[Collection[_DocumentTypeArg], Mapping[str, Any]]:
        """Create a collection with encryptedFields.

        .. warning::
            This function does not update the encryptedFieldsMap in the client's
            AutoEncryptionOpts, thus the user must create a new client after calling this function with
            the encryptedFields returned.

        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.EncryptionError` will be
        raised if the collection already exists.

        :param name: the name of the collection to create
        :param encrypted_fields: Document that describes the encrypted fields for
            Queryable Encryption. The "keyId" may be set to ``None`` to auto-generate the data keys.  For example:

            .. code-block: python

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

          :param kms_provider: the KMS provider to be used
          :param master_key: Identifies a KMS-specific key used to encrypt the
            new data key. If the kmsProvider is "local" the `master_key` is
            not applicable and may be omitted.
          :param kwargs: additional keyword arguments are the same as "create_collection".

        All optional `create collection command`_ parameters should be passed
        as keyword arguments to this method.
        See the documentation for :meth:`~pymongo.database.Database.create_collection` for all valid options.

        :raises: - :class:`~pymongo.errors.EncryptedCollectionError`: When either data-key creation or creating the collection fails.

        .. versionadded:: 4.4

        .. _create collection command:
            https://mongodb.com/docs/manual/reference/command/create

        """
        encrypted_fields = deepcopy(encrypted_fields)
        for i, field in enumerate(encrypted_fields["fields"]):
            if isinstance(field, dict) and field.get("keyId") is None:
                try:
                    encrypted_fields["fields"][i]["keyId"] = self.create_data_key(
                        kms_provider=kms_provider,  # type:ignore[arg-type]
                        master_key=master_key,
                    )
                except EncryptionError as exc:
                    raise EncryptedCollectionError(exc, encrypted_fields) from exc
        kwargs["encryptedFields"] = encrypted_fields
        kwargs["check_exists"] = False
        try:
            return (
                database.create_collection(name=name, **kwargs),
                encrypted_fields,
            )
        except Exception as exc:
            raise EncryptedCollectionError(exc, encrypted_fields) from exc

    def create_data_key(
        self,
        kms_provider: str,
        master_key: Optional[Mapping[str, Any]] = None,
        key_alt_names: Optional[Sequence[str]] = None,
        key_material: Optional[bytes] = None,
    ) -> Binary:
        """Create and insert a new data key into the key vault collection.

        :param kms_provider: The KMS provider to use. Supported values are
            "aws", "azure", "gcp", "kmip", "local", or a named provider like
            "kmip:name".
        :param master_key: Identifies a KMS-specific key used to encrypt the
            new data key. If the kmsProvider is "local" the `master_key` is
            not applicable and may be omitted.

            If the `kms_provider` type is "aws" it is required and has the
            following fields::

              - `region` (string): Required. The AWS region, e.g. "us-east-1".
              - `key` (string): Required. The Amazon Resource Name (ARN) to
                 the AWS customer.
              - `endpoint` (string): Optional. An alternate host to send KMS
                requests to. May include port number, e.g.
                "kms.us-east-1.amazonaws.com:443".

            If the `kms_provider` type is "azure" it is required and has the
            following fields::

              - `keyVaultEndpoint` (string): Required. Host with optional
                 port, e.g. "example.vault.azure.net".
              - `keyName` (string): Required. Key name in the key vault.
              - `keyVersion` (string): Optional. Version of the key to use.

            If the `kms_provider` type is "gcp" it is required and has the
            following fields::

              - `projectId` (string): Required. The Google cloud project ID.
              - `location` (string): Required. The GCP location, e.g. "us-east1".
              - `keyRing` (string): Required. Name of the key ring that contains
                the key to use.
              - `keyName` (string): Required. Name of the key to use.
              - `keyVersion` (string): Optional. Version of the key to use.
              - `endpoint` (string): Optional. Host with optional port.
                Defaults to "cloudkms.googleapis.com".

            If the `kms_provider` type is "kmip" it is optional and has the
            following fields::

              - `keyId` (string): Optional. `keyId` is the KMIP Unique
                Identifier to a 96 byte KMIP Secret Data managed object. If
                keyId is omitted, the driver creates a random 96 byte KMIP
                Secret Data managed object.
              - `endpoint` (string): Optional. Host with optional
                 port, e.g. "example.vault.azure.net:".

        :param key_alt_names: An optional list of string alternate
            names used to reference a key. If a key is created with alternate
            names, then encryption may refer to the key by the unique alternate
            name instead of by ``key_id``. The following example shows creating
            and referring to a data key by alternate name::

              client_encryption.create_data_key("local", key_alt_names=["name1"])
              # reference the key with the alternate name
              client_encryption.encrypt("457-55-5462", key_alt_name="name1",
                                        algorithm=Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Random)
        :param key_material: Sets the custom key material to be used
            by the data key for encryption and decryption.

        :return: The ``_id`` of the created data key document as a
          :class:`~bson.binary.Binary` with subtype
          :data:`~bson.binary.UUID_SUBTYPE`.

        .. versionchanged:: 4.2
           Added the `key_material` parameter.
        """
        self._check_closed()
        with _wrap_encryption_errors():
            return cast(
                Binary,
                self._encryption.create_data_key(
                    kms_provider,
                    master_key=master_key,
                    key_alt_names=key_alt_names,
                    key_material=key_material,
                ),
            )

    def _encrypt_helper(
        self,
        value: Any,
        algorithm: str,
        key_id: Optional[Union[Binary, uuid.UUID]] = None,
        key_alt_name: Optional[str] = None,
        query_type: Optional[str] = None,
        contention_factor: Optional[int] = None,
        range_opts: Optional[RangeOpts] = None,
        is_expression: bool = False,
    ) -> Any:
        self._check_closed()
        if isinstance(key_id, uuid.UUID):
            key_id = Binary.from_uuid(key_id)
        if key_id is not None and not (
            isinstance(key_id, Binary) and key_id.subtype == UUID_SUBTYPE
        ):
            raise TypeError("key_id must be a bson.binary.Binary with subtype 4")

        doc = encode(
            {"v": value},
            codec_options=self._codec_options,
        )
        range_opts_bytes = None
        if range_opts:
            range_opts_bytes = encode(
                range_opts.document,
                codec_options=self._codec_options,
            )
        with _wrap_encryption_errors():
            encrypted_doc = self._encryption.encrypt(
                value=doc,
                algorithm=algorithm,
                key_id=key_id,
                key_alt_name=key_alt_name,
                query_type=query_type,
                contention_factor=contention_factor,
                range_opts=range_opts_bytes,
                is_expression=is_expression,
            )
            return decode(encrypted_doc)["v"]

    def encrypt(
        self,
        value: Any,
        algorithm: str,
        key_id: Optional[Union[Binary, uuid.UUID]] = None,
        key_alt_name: Optional[str] = None,
        query_type: Optional[str] = None,
        contention_factor: Optional[int] = None,
        range_opts: Optional[RangeOpts] = None,
    ) -> Binary:
        """Encrypt a BSON value with a given key and algorithm.

        Note that exactly one of ``key_id`` or  ``key_alt_name`` must be
        provided.

        :param value: The BSON value to encrypt.
        :param algorithm` (string): The encryption algorithm to use. See
            :class:`Algorithm` for some valid options.
        :param key_id: Identifies a data key by ``_id`` which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
        :param key_alt_name: Identifies a key vault document by 'keyAltName'.
        :param query_type` (str): The query type to execute. See :class:`QueryType` for valid options.
        :param contention_factor` (int): The contention factor to use
            when the algorithm is :attr:`Algorithm.INDEXED`.  An integer value
            *must* be given when the :attr:`Algorithm.INDEXED` algorithm is
            used.
        :param range_opts: Experimental only, not intended for public use.

        :return: The encrypted value, a :class:`~bson.binary.Binary` with subtype 6.

        .. versionchanged:: 4.7
           ``key_id`` can now be passed in as a :class:`uuid.UUID`.

        .. versionchanged:: 4.2
           Added the `query_type` and `contention_factor` parameters.
        """
        return cast(
            Binary,
            self._encrypt_helper(
                value=value,
                algorithm=algorithm,
                key_id=key_id,
                key_alt_name=key_alt_name,
                query_type=query_type,
                contention_factor=contention_factor,
                range_opts=range_opts,
                is_expression=False,
            ),
        )

    def encrypt_expression(
        self,
        expression: Mapping[str, Any],
        algorithm: str,
        key_id: Optional[Union[Binary, uuid.UUID]] = None,
        key_alt_name: Optional[str] = None,
        query_type: Optional[str] = None,
        contention_factor: Optional[int] = None,
        range_opts: Optional[RangeOpts] = None,
    ) -> RawBSONDocument:
        """Encrypt a BSON expression with a given key and algorithm.

        Note that exactly one of ``key_id`` or  ``key_alt_name`` must be
        provided.

        :param expression: The BSON aggregate or match expression to encrypt.
        :param algorithm` (string): The encryption algorithm to use. See
            :class:`Algorithm` for some valid options.
        :param key_id: Identifies a data key by ``_id`` which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
        :param key_alt_name: Identifies a key vault document by 'keyAltName'.
        :param query_type` (str): The query type to execute. See
            :class:`QueryType` for valid options.
        :param contention_factor` (int): The contention factor to use
            when the algorithm is :attr:`Algorithm.INDEXED`.  An integer value
            *must* be given when the :attr:`Algorithm.INDEXED` algorithm is
            used.
        :param range_opts: Experimental only, not intended for public use.

        :return: The encrypted expression, a :class:`~bson.RawBSONDocument`.

        .. versionchanged:: 4.7
           ``key_id`` can now be passed in as a :class:`uuid.UUID`.

        .. versionadded:: 4.4
        """
        return cast(
            RawBSONDocument,
            self._encrypt_helper(
                value=expression,
                algorithm=algorithm,
                key_id=key_id,
                key_alt_name=key_alt_name,
                query_type=query_type,
                contention_factor=contention_factor,
                range_opts=range_opts,
                is_expression=True,
            ),
        )

    def decrypt(self, value: Binary) -> Any:
        """Decrypt an encrypted value.

        :param value` (Binary): The encrypted value, a
            :class:`~bson.binary.Binary` with subtype 6.

        :return: The decrypted BSON value.
        """
        self._check_closed()
        if not (isinstance(value, Binary) and value.subtype == 6):
            raise TypeError("value to decrypt must be a bson.binary.Binary with subtype 6")

        with _wrap_encryption_errors():
            doc = encode({"v": value})
            decrypted_doc = self._encryption.decrypt(doc)
            return decode(decrypted_doc, codec_options=self._codec_options)["v"]

    def get_key(self, id: Binary) -> Optional[RawBSONDocument]:
        """Get a data key by id.

        :param id` (Binary): The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).

        :return: The key document.

        .. versionadded:: 4.2
        """
        self._check_closed()
        assert self._key_vault_coll is not None
        return self._key_vault_coll.find_one({"_id": id})

    def get_keys(self) -> Cursor[RawBSONDocument]:
        """Get all of the data keys.

        :return: An instance of :class:`~pymongo.cursor.Cursor` over the data key
          documents.

        .. versionadded:: 4.2
        """
        self._check_closed()
        assert self._key_vault_coll is not None
        return self._key_vault_coll.find({})

    def delete_key(self, id: Binary) -> DeleteResult:
        """Delete a key document in the key vault collection that has the given ``key_id``.

        :param id` (Binary): The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).

        :return: The delete result.

        .. versionadded:: 4.2
        """
        self._check_closed()
        assert self._key_vault_coll is not None
        return self._key_vault_coll.delete_one({"_id": id})

    def add_key_alt_name(self, id: Binary, key_alt_name: str) -> Any:
        """Add ``key_alt_name`` to the set of alternate names in the key document with UUID ``key_id``.

        :param `id`: The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
        :param `key_alt_name`: The key alternate name to add.

        :return: The previous version of the key document.

        .. versionadded:: 4.2
        """
        self._check_closed()
        update = {"$addToSet": {"keyAltNames": key_alt_name}}
        assert self._key_vault_coll is not None
        return self._key_vault_coll.find_one_and_update({"_id": id}, update)

    def get_key_by_alt_name(self, key_alt_name: str) -> Optional[RawBSONDocument]:
        """Get a key document in the key vault collection that has the given ``key_alt_name``.

        :param key_alt_name: (str): The key alternate name of the key to get.

        :return: The key document.

        .. versionadded:: 4.2
        """
        self._check_closed()
        assert self._key_vault_coll is not None
        return self._key_vault_coll.find_one({"keyAltNames": key_alt_name})

    def remove_key_alt_name(self, id: Binary, key_alt_name: str) -> Optional[RawBSONDocument]:
        """Remove ``key_alt_name`` from the set of keyAltNames in the key document with UUID ``id``.

        Also removes the ``keyAltNames`` field from the key document if it would otherwise be empty.

        :param `id`: The UUID of a key a which must be a
            :class:`~bson.binary.Binary` with subtype 4 (
            :attr:`~bson.binary.UUID_SUBTYPE`).
        :param `key_alt_name`: The key alternate name to remove.

        :return: Returns the previous version of the key document.

        .. versionadded:: 4.2
        """
        self._check_closed()
        pipeline = [
            {
                "$set": {
                    "keyAltNames": {
                        "$cond": [
                            {"$eq": ["$keyAltNames", [key_alt_name]]},
                            "$$REMOVE",
                            {
                                "$filter": {
                                    "input": "$keyAltNames",
                                    "cond": {"$ne": ["$$this", key_alt_name]},
                                }
                            },
                        ]
                    }
                }
            }
        ]
        assert self._key_vault_coll is not None
        return self._key_vault_coll.find_one_and_update({"_id": id}, pipeline)

    def rewrap_many_data_key(
        self,
        filter: Mapping[str, Any],
        provider: Optional[str] = None,
        master_key: Optional[Mapping[str, Any]] = None,
    ) -> RewrapManyDataKeyResult:
        """Decrypts and encrypts all matching data keys in the key vault with a possibly new `master_key` value.

        :param filter: A document used to filter the data keys.
        :param provider: The new KMS provider to use to encrypt the data keys,
            or ``None`` to use the current KMS provider(s).
        :param `master_key`: The master key fields corresponding to the new KMS
            provider when ``provider`` is not ``None``.

        :return: A :class:`RewrapManyDataKeyResult`.

        This method allows you to re-encrypt all of your data-keys with a new CMK, or master key.
        Note that this does *not* require re-encrypting any of the data in your encrypted collections,
        but rather refreshes the key that protects the keys that encrypt the data:

        .. code-block:: python

           client_encryption.rewrap_many_data_key(
               filter={"keyAltNames": "optional filter for which keys you want to update"},
               master_key={
                   "provider": "azure",  # replace with your cloud provider
                   "master_key": {
                       # put the rest of your master_key options here
                       "key": "<your new key>"
                   },
               },
           )

        .. versionadded:: 4.2
        """
        if master_key is not None and provider is None:
            raise ConfigurationError("A provider must be given if a master_key is given")
        self._check_closed()
        with _wrap_encryption_errors():
            raw_result = self._encryption.rewrap_many_data_key(filter, provider, master_key)
            if raw_result is None:
                return RewrapManyDataKeyResult()

        raw_doc = RawBSONDocument(raw_result, DEFAULT_RAW_BSON_OPTIONS)
        replacements = []
        for key in raw_doc["v"]:
            update_model = {
                "$set": {"keyMaterial": key["keyMaterial"], "masterKey": key["masterKey"]},
                "$currentDate": {"updateDate": True},
            }
            op = UpdateOne({"_id": key["_id"]}, update_model)
            replacements.append(op)
        if not replacements:
            return RewrapManyDataKeyResult()
        assert self._key_vault_coll is not None
        result = self._key_vault_coll.bulk_write(replacements)
        return RewrapManyDataKeyResult(result)

    def __enter__(self) -> ClientEncryption[_DocumentType]:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _check_closed(self) -> None:
        if self._encryption is None:
            raise InvalidOperation("Cannot use closed ClientEncryption")

    def close(self) -> None:
        """Release resources.

        Note that using this class in a with-statement will automatically call
        :meth:`close`::

            with ClientEncryption(...) as client_encryption:
                encrypted = client_encryption.encrypt(value, ...)
                decrypted = client_encryption.decrypt(encrypted)

        """
        if self._io_callbacks:
            self._io_callbacks.close()
            self._encryption.close()
            self._io_callbacks = None
            self._encryption = None
