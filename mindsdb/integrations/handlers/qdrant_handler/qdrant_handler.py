import ast
from typing import Any, List, Optional
from itertools import zip_longest

from qdrant_client import QdrantClient, models
import pandas as pd

from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class QdrantHandler(VectorStoreHandler):
    """Handles connection and execution of the Qdrant statements."""

    name = "qdrant"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        connection_data = kwargs.get("connection_data").copy()
        # Qdrant offers several configuration and optmization options at the time of collection creation
        # Since the create table statement doesn't have a way to pass these options
        # We are requiring the user to pass these options in the connection_data
        # These options are documented here. https://qdrant.github.io/qdrant/redoc/index.html#tag/collections/operation/create_collection
        self.collection_config = connection_data.pop("collection_config")
        self.connect(**connection_data)

    def connect(self, **kwargs):
        """Connect to a Qdrant instance.
        A Qdrant client can be instantiated with a REST, GRPC interface or in-memory for testing.
        To use the in-memory instance, specify the location argument as ':memory:'."""
        if self.is_connected:
            return self._client

        try:
            self._client = QdrantClient(**kwargs)
            self.is_connected = True
            return self._client
        except Exception as e:
            logger.error(f"Error instantiating a Qdrant client: {e}")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""
        if self.is_connected:
            self._client.close()
            self._client = None
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """Check the connection to the Qdrant database.

        Returns:
            StatusResponse: Indicates if the connection is alive
        """
        need_to_close = not self.is_connected

        try:
            # Using a trivial operation to get the connection status
            # As there isn't a universal ping method for the REST, GRPC and in-memory interface
            self._client.get_locks()
            response_code = StatusResponse(True)
        except Exception as e:
            logger.error(f"Error connecting to a Qdrant instance: {e}")
            response_code = StatusResponse(False, error_message=str(e))
        finally:
            if response_code.success and need_to_close:
                self.disconnect()
            if not response_code.success and self.is_connected:
                self.is_connected = False

        return response_code

    def drop_table(self, table_name: str, if_exists=True):
        """Delete a collection from the Qdrant Instance.

        Args:
            table_name (str): The name of the collection to be dropped
            if_exists (bool, optional): Throws an error if this value is set to false and the collection doesn't exist. Defaults to True.

        Returns:
            HandlerResponse: _description_
        """
        result = self._client.delete_collection(table_name)
        if not (result or if_exists):
            raise Exception(f"Table {table_name} does not exist!")

    def get_tables(self) -> HandlerResponse:
        """Get the list of collections in the Qdrant instance.

        Returns:
            HandlerResponse: The common query handler response with a list of table names
        """
        collection_response = self._client.get_collections()
        collections_name = pd.DataFrame(
            columns=["table_name"],
            data=[collection.name for collection in collection_response.collections],
        )
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=collections_name)

    def get_columns(self, table_name: str) -> HandlerResponse:
        try:
            _ = self._client.get_collection(table_name)
        except ValueError:
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Table {table_name} does not exist!",
            )
        return super().get_columns(table_name)

    def insert(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        """Handler for the insert query

        Args:
            table_name (str): The name of the table to be inserted into
            data (pd.DataFrame): The data to be inserted
            columns (List[str], optional): Columns to be inserted into. Unused as the values are derived from the "data" argument. Defaults to None.

        Returns:
            HandlerResponse: The common query handler response
        """
        assert len(data[TableField.ID.value]) == len(data[TableField.EMBEDDINGS.value]), "Number of ids and embeddings must be equal"

        # Qdrant doesn't have a distinction between documents and metadata
        # Any data that is to be stored should be placed in the "payload" field
        data = data.to_dict(orient="list")
        payloads = []
        content_list = data[TableField.CONTENT.value]
        if TableField.METADATA.value in data:
            metadata_list = data[TableField.METADATA.value]
        else:
            metadata_list = [None] * len(data)
        for document, metadata in zip_longest(content_list, metadata_list, fillvalue=None):
            payload = {}

            # Insert the document with a "document" key in the payload
            if document is not None:
                payload["document"] = document

            # Unpack all the metadata fields into the payload
            if metadata is not None:
                if isinstance(metadata, str):
                    metadata = ast.literal_eval(metadata)
                payload = {**payload, **metadata}

            if payload:
                payloads.append(payload)

        # IDs can be either integers or strings(UUIDs)
        # The following step ensures proper type of numberic values
        ids = [int(id) if str(id).isdigit() else id for id in data[TableField.ID.value]]
        self._client.upsert(table_name, points=models.Batch(
            ids=ids,
            vectors=data[TableField.EMBEDDINGS.value],
            payloads=payloads
        ))

    def create_table(self, table_name: str, if_not_exists=True):
        """Create a collection with the given name in the Qdrant database.

        Args:
            table_name (str): Name of the table(Collection) to be created
            if_not_exists (bool, optional): Throws an error if this value is set to false and the collection already exists. Defaults to True.

        Returns:
            HandlerResponse: The common query handler response
        """
        try:
            # Create a collection with the collection name and collection_config set during __init__
            self._client.create_collection(table_name, self.collection_config)
        except ValueError as e:
            if if_not_exists is False:
                raise e

    def _get_qdrant_filter(self, operator: FilterOperator, value: Any) -> dict:
        """ Map the filter operator to the Qdrant filter
            We use a match and not a dict so as to conditionally construct values
            With a dict, all the values the values will be constructed
            Generating models.Range() with a str type value fails

        Args:
            operator (FilterOperator): FilterOperator specified in the query. Eg >=, <=, =
            value (Any): Value specified in the query

        Raises:
            Exception: If an unsupported operator is specified

        Returns:
            dict: A dict of Qdrant filtering clauses
        """
        if operator == FilterOperator.EQUAL:
            return {"match": models.MatchValue(value=value)}
        elif operator == FilterOperator.NOT_EQUAL:
            return {"match": models.MatchExcept(**{"except": [value]})}
        elif operator == FilterOperator.LESS_THAN:
            return {"range": models.Range(lt=value)}
        elif operator == FilterOperator.LESS_THAN_OR_EQUAL:
            return {"range": models.Range(lte=value)}
        elif operator == FilterOperator.GREATER_THAN:
            return {"range": models.Range(gt=value)}
        elif operator == FilterOperator.GREATER_THAN_OR_EQUAL:
            return {"range": models.Range(gte=value)}
        else:
            raise Exception(f"Operator {operator} is not supported by Qdrant!")

    def _translate_filter_conditions(
        self, conditions: List[FilterCondition]
    ) -> Optional[dict]:
        """
        Translate a list of FilterCondition objects a dict that can be used by Qdrant.
        Filtering clause docs can be found here: https://qdrant.tech/documentation/concepts/filtering/
        E.g.,
        [
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.LESS_THAN,
                value=7132423,
            ),
            FilterCondition(
                column="metadata.created_at",
                op=FilterOperator.GREATER_THAN,
                value=2323432,
            )
        ]
        -->
        models.Filter(
        must=[
            models.FieldCondition(
                key="created_at",
                match=models.Range(lt=7132423),
            ),
            models.FieldCondition(
                key="created_at",
                match=models.Range(gt=2323432),
            ),
          ]
        )
        """
        # We ignore all non-metadata conditions
        if conditions is None:
            return None
        filter_conditions = [
            condition
            for condition in conditions
            if condition.column.startswith(TableField.METADATA.value)
        ]
        if len(filter_conditions) == 0:
            return None

        qdrant_filters = []
        for condition in filter_conditions:
            payload_key = condition.column.split(".")[-1]
            qdrant_filters.append(
                models.FieldCondition(key=payload_key, **self._get_qdrant_filter(condition.op, condition.value))
            )

        return models.Filter(must=qdrant_filters) if qdrant_filters else None

    def update(
        self, table_name: str, data: pd.DataFrame, columns: List[str] = None
    ):
        # insert makes upsert
        return self.insert(table_name, data)

    def select(self, table_name: str, columns: Optional[List[str]] = None,
               conditions: Optional[List[FilterCondition]] = None, offset: int = 0, limit: int = 10) -> pd.DataFrame:
        """Select query handler
           Eg: SELECT * FROM qdrant.test_table

        Args:
            table_name (str): The name of the table to be queried
            columns (Optional[List[str]], optional): List of column names specified in the query. Defaults to None.
            conditions (Optional[List[FilterCondition]], optional): List of "where" conditionals. Defaults to None.
            offset (int, optional): Offset the results by the provided value. Defaults to 0.
            limit (int, optional): Number of results to return. Defaults to 10.

        Returns:
            HandlerResponse: The common query handler response
        """

        # Validate and set offset and limit as None is passed if not set in the query
        offset = offset if offset is not None else 0
        limit = limit if limit is not None else 10

        # Full scroll if no where conditions are specified
        if not conditions:
            results = self._client.scroll(table_name, limit=limit, offset=offset)
            payload = self._process_select_results(results[0], columns)
            return payload

        # Filter conditions
        vector_filter = [condition.value for condition in conditions if condition.column == TableField.SEARCH_VECTOR.value]
        id_filters = [condition.value for condition in conditions if condition.column == TableField.ID.value]
        query_filters = self._translate_filter_conditions(conditions)

        # Prefer returning results by IDs first
        if id_filters:

            if len(id_filters) > 0:
                # is wrapped to a list
                if isinstance(id_filters[0], list):
                    id_filters = id_filters[0]
            # convert to int if possible
            id_filters = [int(id) if isinstance(id, str) and id.isdigit() else id for id in id_filters]

            results = self._client.retrieve(table_name, ids=id_filters)
        # Followed by the search_vector value
        elif vector_filter:
            # Perform a similarity search with the first vector filter
            results = self._client.search(table_name, query_vector=vector_filter[0], query_filter=query_filters or None, limit=limit, offset=offset)
        elif query_filters:
            results = self._client.scroll(table_name, scroll_filter=query_filters, limit=limit, offset=offset)[0]

        # Process results
        payload = self._process_select_results(results, columns)
        return payload

    def _process_select_results(self, results=None, columns=None):
        """Private method to process the results of a select query

        Args:
            results: A List[Records] or List[ScoredPoint]. Defaults to None
            columns: List of column names specified in the query. Defaults to None

        Returns:
            Dataframe: A processed pandas dataframe
        """
        ids, documents, metadata, distances = [], [], [], []

        for result in results:
            ids.append(result.id)
            # The documents and metadata are stored as a dict in the payload
            documents.append(result.payload["document"])
            metadata.append({k: v for k, v in result.payload.items() if k != "document"})

            # Score is only available for similarity search results
            if "score" in result:
                distances.append(result.score)

        payload = {
            TableField.ID.value: ids,
            TableField.CONTENT.value: documents,
            TableField.METADATA.value: metadata,
        }

        # Filter result columns
        if columns:
            payload = {
                column: payload[column]
                for column in columns
                if column != TableField.EMBEDDINGS.value and column in payload
            }

        # If the distance list is empty, don't add it to the result
        if distances:
            payload[TableField.DISTANCE.value] = distances

        return pd.DataFrame(payload)

    def delete(
        self, table_name: str, conditions: List[FilterCondition] = None
    ):
        """Delete query handler

        Args:
            table_name (str): List of column names specified in the query. Defaults to None.
            conditions (List[FilterCondition], optional): List of "where" conditionals. Defaults to None.

        Raises:
            Exception: If no conditions are specified

        Returns:
            HandlerResponse: The common query handler response
        """
        filters = self._translate_filter_conditions(conditions)
        # Get id filters
        ids = [
            condition.value
            for condition in conditions
            if condition.column == TableField.ID.value
        ] or None

        if filters is None and ids is None:
            raise Exception("Delete query must have at least one condition!")

        if ids:
            self._client.delete(table_name, points_selector=models.PointIdsList(points=ids))

        if filters:
            self._client.delete(table_name, points_selector=models.FilterSelector(filter=filters))
