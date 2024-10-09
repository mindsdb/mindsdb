import os
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd

from mindsdb.integrations.libs.response import RESPONSE_TYPE, HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition, FilterOperator, TableField, VectorStoreHandler
)
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities import log
from typing import List

logger = log.getLogger(__name__)


class FirestoreConfig:
    def __init__(self, credentials_file: str, project_id: str):
        self.credentials_file = credentials_file
        self.project_id = project_id

        # Basic validation of the config parameters
        if not self.credentials_file or not os.path.exists(self.credentials_file):
            raise ValueError("Invalid credentials file path provided!")
        if not self.project_id:
            raise ValueError("Project ID cannot be empty!")


class FirestoreHandler(VectorStoreHandler):
    """This handler manages connection and execution of Firestore operations."""

    name = "firestore"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.handler_storage = HandlerStorage(kwargs.get("integration_id"))
        self._client = None
        self.is_connected = False

        config = self.validate_connection_parameters(name, **kwargs)

        self.credentials_file = config.credentials_file
        self.project_id = config.project_id

        self.connect()

    def validate_connection_parameters(self, name, **kwargs):
        """Validate and set up connection parameters."""
        _config = kwargs.get("connection_data")

        # FirestoreConfig will include validation for project_id and credentials
        return FirestoreConfig(**_config)

    def connect(self):
        """Connect to Firestore."""
        if self.is_connected:
            return self._client

        try:
            cred = credentials.Certificate(self.credentials_file)
            firebase_admin.initialize_app(cred, {"projectId": self.project_id})
            self._client = firestore.client()
            self.is_connected = True
            return self._client
        except Exception as e:
            self.is_connected = False
            raise Exception(f"Error connecting to Firestore: {e}!")

    def disconnect(self):
        """Disconnect from Firestore (Firestore does not require explicit disconnect)."""
        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check Firestore connection."""
        try:
            _ = self._client.collections()
            return StatusResponse(success=True)
        except Exception as e:
            logger.error(f"Error connecting to Firestore: {e}!")
            return StatusResponse(success=False, error_message=str(e))

    def _get_firestore_operator(self, operator: FilterOperator) -> str:
        """Map MindsDB filter operators to Firestore operators."""
        mapping = {
            FilterOperator.EQUAL: "==",
            FilterOperator.NOT_EQUAL: "!=",
            FilterOperator.LESS_THAN: "<",
            FilterOperator.LESS_THAN_OR_EQUAL: "<=",
            FilterOperator.GREATER_THAN: ">",
            FilterOperator.GREATER_THAN_OR_EQUAL: ">="
        }

        if operator not in mapping:
            raise Exception(f"Operator {operator} is not supported by Firestore!")

        return mapping[operator]

    def _translate_conditions(self, conditions: List[FilterCondition]):
        """Translate MindsDB conditions into Firestore query filters."""
        if not conditions:
            return None

        firestore_conditions = []
        for condition in conditions:
            firestore_conditions.append({
                "field": condition.column,
                "operator": self._get_firestore_operator(condition.op),
                "value": condition.value
            })

        return firestore_conditions

    def select(self, collection_name: str, columns: List[str] = None, conditions: List[FilterCondition] = None,
               offset: int = None, limit: int = None) -> pd.DataFrame:
        """Select data from Firestore collection."""
        collection = self._client.collection(collection_name)
        firestore_conditions = self._translate_conditions(conditions)
        query = collection

        # Apply filters
        if firestore_conditions:
            for condition in firestore_conditions:
                query = query.where(condition["field"], condition["operator"], condition["value"])

        # Apply limit
        if limit is not None:
            query = query.limit(limit)

        # Apply offset
        if offset is not None:
            query = query.offset(offset)

        docs = query.stream()

        result = []
        for doc in docs:
            data = doc.to_dict()
            if columns:
                filtered_data = {col: data.get(col, None) for col in columns}
                result.append(filtered_data)
            else:
                result.append(data)

        return pd.DataFrame(result)

    def insert(self, collection_name: str, data: pd.DataFrame):
        """Insert data into Firestore."""
        collection = self._client.collection(collection_name)
        data_dict = data.to_dict(orient="records")

        for record in data_dict:
            doc_ref = collection.document()
            doc_ref.set(record)

    def upsert(self, collection_name: str, data: pd.DataFrame):
        """Upsert (insert or update) Firestore data."""
        self.insert(collection_name, data)

    def update(self, collection_name: str, data: pd.DataFrame, key_columns: List[str] = None):
        """Update Firestore data."""
        collection = self._client.collection(collection_name)
        data_dict = data.to_dict(orient="records")

        for record in data_dict:
            # Find document by name field, assuming "name" is not the ID but a field
            name = record.get("name")
            if not name:
                raise ValueError("Document ID (name) must be provided for updating.")

            # Query Firestore to find the document by the 'name' field using keyword argument for 'filter'
            query = collection.where("name", "==", name)
            docs = query.stream()

            # Check if the document exists
            doc = next(docs, None)
            if not doc:
                raise ValueError(f"Document with name '{name}' not found!")

            # Update the document using its ID
            doc_ref = collection.document(doc.id)
            doc_ref.update(record)

    def delete(self, collection_name: str, conditions: List[FilterCondition] = None):
        """Delete documents from Firestore."""
        firestore_conditions = self._translate_conditions(conditions)
        collection = self._client.collection(collection_name)
        query = collection

        if firestore_conditions:
            for condition in firestore_conditions:
                query = query.where(condition["field"], condition["operator"], condition["value"])

        docs = query.stream()

        for doc in docs:
            doc.reference.delete()

    def get_tables(self) -> Response:
        """Get all Firestore collections."""
        collections = self._client.collections()
        collection_names = [collection.id for collection in collections]
        return Response(resp_type=RESPONSE_TYPE.TABLE,
                        data_frame=pd.DataFrame(collection_names, columns=["collection_name"]))

    def get_columns(self, collection_name: str) -> Response:
        """Get document field names in Firestore collection."""
        collection = self._client.collection(collection_name)
        doc = next(collection.stream(), None)

        if doc is None:
            return Response(resp_type=RESPONSE_TYPE.ERROR,
                            error_message=f"Collection {collection_name} does not exist!")

        doc_fields = doc.to_dict().keys()
        return Response(resp_type=RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame(doc_fields, columns=["field_name"]))
