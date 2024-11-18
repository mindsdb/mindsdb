import ast
from datetime import timedelta
import uuid

import pandas as pd
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import UnAmbiguousTimeoutException
from couchbase.options import ClusterOptions
from couchbase.exceptions import CouchbaseException
from typing import List, Union

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    TableField,
    VectorStoreHandler,
)


logger = log.getLogger(__name__)


class CouchbaseVectorHandler(VectorStoreHandler):
    """
    This handler handles connection and execution of the Couchbase statements.
    """

    name = "couchbasevector"
    DEFAULT_TIMEOUT_SECONDS = 5

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data")

        self.scope = self.connection_data.get("scope") or "_default"

        self.bucket_name = self.connection_data.get("bucket")
        self.cluster = None

        self.is_connected = False

    def connect(self):
        """
        Set up connections required by the handler.

        Returns:
            The connected cluster.
        """
        if self.is_connected:
            return self.cluster

        auth = PasswordAuthenticator(
            self.connection_data.get("user"),
            self.connection_data.get("password"),
            # NOTE: If using SSL/TLS, add the certificate path.
            # We strongly reccomend this for production use.
            # cert_path=cert_path
        )

        options = ClusterOptions(auth)

        conn_str = self.connection_data.get("connection_string")
        # wan_development is used to avoid latency issues while connecting to Couchbase over the internet
        options.apply_profile("wan_development")
        # connect to the cluster
        cluster = Cluster(
            conn_str,
            options,
        )

        try:
            # wait until the cluster is ready for use
            cluster.wait_until_ready(
                timedelta(seconds=self.DEFAULT_TIMEOUT_SECONDS)
            )
            self.is_connected = cluster.connected
            self.cluster = cluster
        except UnAmbiguousTimeoutException:
            self.is_connected = False
            raise

        return self.cluster

    def disconnect(self):
        """Close any existing connections
        Should switch self.is_connected.
        """
        if self.is_connected is False:
            return
        self.is_connected = self.cluster.connected
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Couchbase bucket
        :return: success status and error message if error occurs
        """
        result = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            cluster = self.connect()
            result.success = cluster.connected
        except UnAmbiguousTimeoutException as e:
            logger.error(
                f'Error connecting to Couchbase {self.connection_data["bucket"]}, {e}!'
            )
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False
        return result

    def _translate_conditions(
        self, conditions: List[FilterCondition]
    ) -> Union[dict, None]:
        """
        Translate filter conditions to a dictionary
        """
        if conditions is None:
            return {}

        return {
            condition.column: {
                "op": condition.op.value,
                "value": condition.value,
            }
            for condition in conditions
        }

    def _construct_full_after_from_query(
        self,
        where_query: str,
        limit_query: str,
        offset_query: str,
        search_query: str,
    ) -> str:

        return f"{where_query} {search_query} {limit_query} {offset_query} "

    def _construct_where_query(self, filter_conditions=None):
        """
        Construct where querys from filter conditions
        """
        if filter_conditions is None:
            return ""

        where_querys = []
        metadata_conditions = {
            key: value
            for key, value in filter_conditions.items()
            if not key.startswith(TableField.EMBEDDINGS.value)
        }
        for key, value in metadata_conditions.items():
            if value["op"].lower() == "in":
                values = list(repr(i) for i in value["value"])
                value["value"] = "({})".format(", ".join(values))
            else:
                value["value"] = repr(value["value"])
            where_querys.append(f'{key} {value["op"]} {value["value"]}')

        if len(where_querys) > 1:
            return f"WHERE {' AND '.join(where_querys)}"
        elif len(where_querys) == 1:
            return f"WHERE {where_querys[0]}"
        else:
            return ""

    def _construct_search_query(
        self, table_name: str, field: str, vector: list, k: int, condition: str
    ):
        """
        Construct a SEARCH query for KNN
        :param table_name: Name of the table
        :param field: The field on which to perform the search (e.g., embeddings)
        :param vector: The vector to search against
        :param k: The number of nearest neighbors to return, default: 2
        :return: The SEARCH query as a string
        """
        k_value = k if k is not None else 2
        search_query = f"""
        {condition} SEARCH({table_name}, {{
            "fields": ["*"],
            "query": {{
                "match_none": ""
            }},
            "knn": [
            {{
                "k": {k_value},
                "field": "{field}",
                "vector": {vector}
            }}
            ]
        }})
        """
        return search_query.strip()

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        filter_conditions = self._translate_conditions(conditions)
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope)
        documents, metadatas, embeddings = [], [], []

        vector_filter = (
            next(
                (
                    condition
                    for condition in conditions
                    if condition.column == TableField.EMBEDDINGS.value
                ),
                None,
            )
            if conditions
            else None
        )
        limit_query = f"LIMIT {limit}" if limit else ""
        offset_query = f"OFFSET {offset}" if offset else ""
        if vector_filter:
            vector = vector_filter.value
            if not isinstance(vector, list):
                vector = ast.literal_eval(vector)

            where_query = self._construct_where_query(filter_conditions)
            if where_query == "":
                search_query = self._construct_search_query(
                    table_name,
                    TableField.EMBEDDINGS.value,
                    vector_filter.value,
                    limit,
                    "WHERE",
                )
            else:
                search_query = self._construct_search_query(
                    table_name,
                    TableField.EMBEDDINGS.value,
                    vector_filter.value,
                    limit,
                    "AND",
                )
            after_from_query = self._construct_full_after_from_query(
                where_query, limit_query, offset_query, search_query
            )

            if columns is None:
                targets = "id, content, embeddings, metadata"
            else:
                targets = ", ".join(columns)
            query = f"SELECT SEARCH_SCORE() AS score, {targets} FROM {table_name} {after_from_query}"
            try:
                result = scope.query(query)
            except CouchbaseException as e:
                raise Exception(f"Error while executing query: '{e}'")

            # Process results
            ids, documents, distances = [], [], []
            for hit in result.rows():
                ids.append(hit.get("id", ""))
                documents.append(hit.get("content", ""))
                embeddings.append(hit.get("embeddings", []))
                metadatas.append(hit.get("metadata", {}))
                distances.append(hit.get("score", ""))
        else:

            where_query = self._construct_where_query(filter_conditions)
            after_from_query = self._construct_full_after_from_query(
                where_query, limit_query, offset_query, ""
            )

            if columns is None:
                targets = "id, content, embeddings, metadata"
            else:
                targets = ", ".join(columns)

            query = f"SELECT {targets} FROM {table_name} {after_from_query}"
            try:
                result = scope.query(query)
            except CouchbaseException as e:
                raise Exception(f"Error while executing query: '{e}'")

            ids = []
            documents = []
            for hit in result.rows():
                ids.append(hit.get("id", ""))
                documents.append(hit.get("content", ""))
                embeddings.append(hit.get("embeddings", []))
                metadatas.append(hit.get("metadata", {}))

            distances = None

        # Prepare the payload
        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: [doc for doc in documents],
            TableField.METADATA.value: [doc for doc in metadatas],
            TableField.EMBEDDINGS.value: [doc for doc in embeddings],
        }
        if columns:
            payload = {
                column: payload[column]
                for column in columns
                if column in payload
            }
        if distances is not None:
            payload[TableField.DISTANCE.value] = distances
        return pd.DataFrame(payload)

    def insert(self, table_name: str, data: pd.DataFrame) -> Response:
        """
        Insert data into Couchbase.
        """

        data.dropna(axis=1, inplace=True)
        # Convert DataFrame to list of dictionaries
        records = data.to_dict(orient="records")
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope)
        collection = scope.collection(table_name)

        for record in records:
            doc_id = record.get(TableField.ID.value, str(uuid.uuid4()))
            document = {TableField.ID.value: doc_id}

            if TableField.CONTENT.value in record:
                document[TableField.CONTENT.value] = record[
                    TableField.CONTENT.value
                ]

            if TableField.EMBEDDINGS.value in record:
                document[TableField.EMBEDDINGS.value] = record[
                    TableField.EMBEDDINGS.value
                ]
                if not isinstance(document[TableField.EMBEDDINGS.value], list):
                    document[TableField.EMBEDDINGS.value] = ast.literal_eval(
                        document[TableField.EMBEDDINGS.value]
                    )

            if TableField.METADATA.value in record:
                document[TableField.METADATA.value] = record[
                    TableField.METADATA.value
                ]
            document_key = f"{table_name}::{doc_id}"

            collection.upsert(document_key, document)
        return Response(resp_type=RESPONSE_TYPE.OK)

    def upsert(self, table_name: str, data: pd.DataFrame):
        return self.insert(table_name, data)

    def update(
        self,
        table_name: str,
        data: pd.DataFrame,
        key_columns: List[str] = None,
    ):
        """
        Update data in Couchbase.
        """
        # Convert DataFrame to list of dictionaries
        records = data.to_dict(orient="records")
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope)
        collection = scope.collection(table_name)
        try:
            for record in records:
                doc_id = record.get(TableField.ID.value)
                if doc_id:
                    existing_doc = self.collection.get(doc_id)
                    if existing_doc:
                        updated_doc = existing_doc.content
                        updated_doc.update(record)
                        collection.replace(doc_id, updated_doc)
        except CouchbaseException as e:
            raise Exception(f"Error while updating document: '{e}'")

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        """
        Delete documents in Couchbase based on conditions.
        """
        filter_conditions = self._translate_conditions(conditions)
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope)
        where_query = self._construct_where_query(filter_conditions)

        query = f"DELETE FROM {table_name} {where_query}"
        try:
            _ = scope.query(query)
        except CouchbaseException as e:
            raise Exception(
                f"Error while performing delete query index: '{e}'"
            )

    def create_table(self, table_name: str, if_not_exists=True):
        """
        In Couchbase, tables are represented as collections within a bucket.
        This method creates a new collection if it doesn't exist.
        """
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        try:
            bucket.collections().create_collection(
                scope_name=self.scope, collection_name=table_name
            )
        except Exception as e:
            raise Exception(f"Error while creating table: '{e}'")

    def drop_table(self, table_name: str, if_exists=True) -> Response:
        """
        Drop a collection in Couchbase.
        """
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        scope = bucket.scope(self.scope)
        _ = scope.collection(table_name)
        try:
            bucket.collections().drop_collection(table_name)
            return Response(resp_type=RESPONSE_TYPE.OK)
        except Exception as e:
            return Response(resp_type=RESPONSE_TYPE.ERROR, error_message=e)

    def get_tables(self) -> Response:
        """
        Get the list of collections in the Couchbase bucket.
        """
        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        collections = bucket.collections().get_all_scopes()
        collection_names = [
            coll.name for scope in collections for coll in scope.collections
        ]
        collections_df = pd.DataFrame(
            columns=["table_name"], data=collection_names
        )
        return Response(
            resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_df
        )

    def get_columns(self, table_name: str) -> Response:
        """
        Get the columns (fields) of a Couchbase collection.
        """
        try:
            cluster = self.connect()
            bucket = cluster.bucket(self.bucket_name)
            scope = bucket.scope(self.scope)
            _ = scope.collection(table_name)
        except Exception:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        return super().get_columns(table_name)
