import json
import pandas as pd
from typing import List

from faunadb import query as q
from faunadb.client import FaunaClient
from mindsdb_sql_parser import Select, Insert, CreateTable, Delete
from mindsdb_sql_parser.ast.select.star import Star
from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.integrations.libs.response import (
    RESPONSE_TYPE,
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class FaunaDBHandler(DatabaseHandler):
    """This handler handles connection and execution of the FaunaDB statements."""

    name = "faunadb"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)

        self._connection_data = kwargs.get("connection_data")

        self._client_config = {
            "fauna_secret": self._connection_data.get("fauna_secret"),
            "fauna_scheme": self._connection_data.get("fauna_scheme"),
            "fauna_domain": self._connection_data.get("fauna_domain"),
            "fauna_port": self._connection_data.get("fauna_port"),
            "fauna_endpoint": self._connection_data.get("fauna_endpoint"),
        }

        scheme, domain, port, endpoint = (
            self._client_config["fauna_scheme"],
            self._client_config["fauna_domain"],
            self._client_config["fauna_port"],
            self._client_config["fauna_endpoint"],
        )

        # should have the secret
        if (self._client_config["fauna_secret"]) is None:
            raise Exception("FaunaDB secret is required for FaunaDB connection!")
        # either scheme + domain + port or endpoint is required
        # but not both
        if not endpoint and not (scheme and domain and port):
            raise Exception(
                "Either scheme + domain + port or endpoint is required for FaunaDB connection!"
            )
        elif endpoint and (scheme or domain or port):
            raise Exception(
                "Either scheme + domain + port or endpoint is required for FaunaDB connection, but not both!"
            )

        self._client = None
        self.is_connected = False
        self.connect()

    def _get_client(self):
        client_config = self._client_config
        if client_config is None:
            raise Exception("Client config is not set!")

        if client_config["fauna_endpoint"] is not None:
            return FaunaClient(
                secret=client_config["fauna_secret"],
                endpoint=client_config["fauna_endpoint"],
            )
        else:
            return FaunaClient(
                secret=client_config["fauna_secret"],
                scheme=client_config["fauna_scheme"],
                domain=client_config["fauna_domain"],
                port=client_config["fauna_port"],
            )

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """Connect to a FaunaDB database."""
        if self.is_connected is True:
            return self._client

        try:
            self._client = self._get_client()
            self.is_connected = True
            return self._client
        except Exception as e:
            logger.error(f"Error connecting to FaunaDB client, {e}!")
            self.is_connected = False

    def disconnect(self):
        """Close the database connection."""

        if self.is_connected is False:
            return

        self._client = None
        self.is_connected = False

    def check_connection(self):
        """Check the connection to the FaunaDB database."""
        response_code = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self._client.ping()
            response_code.success = True
        except Exception as e:
            logger.error(f"Error connecting to FaunaDB , {e}!")
            response_code.error_message = str(e)
        finally:
            if response_code.success is True and need_to_close:
                self.disconnect()
            if response_code.success is False and self.is_connected is True:
                self.is_connected = False

        return response_code

    def query(self, query: ASTNode) -> Response:
        """Render and execute a SQL query.

        Args:
            query (ASTNode): The SQL query.

        Returns:
            Response: The query result.
        """

        if isinstance(query, Select):
            collection = str(query.from_table)
            fields = query.targets
            conditions = query.where
            offset = query.offset
            limit = query.limit
            # TODO: research how to parse individual columns from document
            # fields = [f.to_string().split()[2] for f in fields]
            result = self.select(collection, fields, conditions, offset, limit)

        elif isinstance(query, Insert):
            collection = str(query.table)
            fields = [col.name for col in query.columns]
            values = query.values
            self.insert(collection, fields, values)
            return Response(resp_type=RESPONSE_TYPE.OK)

        elif isinstance(query, CreateTable):
            collection_name = str(query.name)
            self.create_table(collection_name)
            return Response(resp_type=RESPONSE_TYPE.OK)

        elif isinstance(query, Delete):
            collection_name = str(query.table)
            conditions = query.where
            self.delete_document(collection_name, conditions)
            return Response(resp_type=RESPONSE_TYPE.OK)
        """
        # NOT Working for integration tables yet
        elif isinstance(query, DropTables):
            collection_name = str(query.tables)
            self.drop_table(collection_name)
        """

        df = pd.json_normalize(result['data'])
        return Response(RESPONSE_TYPE.TABLE, df)

    def select(
        self,
        table_name: str,
        columns: List[str] = None,
        conditions: List[str] = None,
        offset: int = None,
        limit: int = None,
    ) -> dict:
        # select * from db_name.collection_name
        if len(columns) > 0 and isinstance(columns[0], Star):
            fauna_query = q.map_(
                q.lambda_("ref", q.get(q.var("ref"))),
                q.paginate(q.documents(q.collection(str(table_name)))),
            )
        else:
            # select id, name ,etc from db_name.collection_name
            fauna_query = q.map_(
                q.lambda_("doc", {"data": q.select(columns, q.var("doc"))}),
                q.paginate(q.documents(q.collection(table_name))),
            )

        return self._client.query(fauna_query)

    def insert(self, table_name: str, fields, values) -> Response:
        if len(fields) == 1 and fields[0] == "data":
            for value in values:
                value = json.loads(value[0])
                if isinstance(value, dict):
                    value = [value]
                for data in value:
                    self._client.query(
                        q.create(
                            q.collection(table_name),
                            {"data": data},
                        )
                    )
        else:
            for value in values:
                data = {f: v for f, v in zip(fields, value)}
                self._client.query(
                    q.create(
                        q.collection(table_name),
                        {"data": data},
                    )
                )

    def create_table(self, table_name: str, if_not_exists=True) -> Response:
        """
        Create a collection with the given name in the FaunaDB database.
        """
        fauna_query = q.create_collection({"name": table_name})
        self._client.query(fauna_query)

    def delete_document(self, table_name: str, conditions: List[str]) -> Response:
        """
        Delete a document with the given id in the FaunaDB database.
        """
        # get the id of the document (only = operator supported right now, can add more)
        ref = conditions.args[1].value
        fauna_query = q.delete(q.ref(q.collection(table_name), ref))
        self._client.query(fauna_query)

    def drop_table(self, table_name: str, if_exists=True) -> Response:
        """
        Delete a collection from the FaunaDB database.
        """
        fauna_query = q.delete(q.collection(table_name))
        self._client.query(fauna_query)
        return Response(resp_type=RESPONSE_TYPE.OK)

    def get_tables(self) -> Response:
        """
        Get the list of collections in the FaunaDB database.
        """
        try:
            result = self._client.query(q.paginate(q.collections()))
            collections = []
            for collection in result["data"]:
                collections.append(collection.id())
            return Response(
                resp_type=RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(
                    collections,
                    columns=["table_name"],
                ),
            )
        except Exception as e:
            logger.error(f"Error getting tables from FaunaDB: {e}")
            return Response(
                resp_type=RESPONSE_TYPE.ERROR,
                error_message=f"Error getting tables from FaunaDB: {e}",
            )
