from typing import Optional

import pandas as pd
import duckdb

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from pyairtable import Api

from mindsdb.integrations.libs.api_handler import APIResource, APIHandler
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

logger = log.getLogger(__name__)


class ListFilesTable(APIResource):

    def list(
        self,
        targets: List[str] = None,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        *args,
        **kwargs,
    ) -> pd.DataFrame:

        buckets = None
        for condition in conditions:
            if condition.column == "bucket":
                if condition.op == FilterOperator.IN:
                    buckets = condition.value
                elif condition.op == FilterOperator.EQUAL:
                    buckets = [condition.value]
                condition.applied = True

        data = []
        for obj in self.handler.get_objects(limit=limit, buckets=buckets):
            path = obj["Key"]
            path = path.replace("`", "")
            item = {
                "path": path,
                "bucket": obj["Bucket"],
                "name": path[path.rfind("/") + 1 :],
                "extension": path[path.rfind(".") + 1 :],
            }

            data.append(item)

        return pd.DataFrame(data=data, columns=self.get_columns())

    def get_columns(self) -> List[str]:
        return ["path", "name", "extension", "bucket", "content"]


class FileTable(APIResource):

    def list(
        self, targets: List[str] = None, table_name=None, *args, **kwargs
    ) -> pd.DataFrame:
        return self.handler.read_as_table(table_name)

    def add(self, data, table_name=None):
        df = pd.DataFrame(data)
        return self.handler.add_data_to_table(table_name, df)


class AirtableHandler(APIHandler):
    """
    This handler handles connection and execution of the Airtable statements.
    """

    name = "airtable"

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = "airtable"
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected is True:
            return self.connection

        api = Api(self.connection_data["api_key"])

        # response = requests.get(url, headers=headers)
        # response = response.json()
        # records = response['records']

        # new_records = True
        # while new_records:
        #     try:
        #         if response['offset']:
        #             params = {"offset": response['offset']}
        #             response = requests.get(url, params=params, headers=headers)
        #             response = response.json()

        #             new_records = response['records']
        #             records = records + new_records
        #     except Exception:
        #         new_records = False

        # rows = [record['fields'] for record in records]
        # globals()[self.connection_data['table_name']] = pd.DataFrame(rows)

        # self.connection = duckdb.connect()
        # self.is_connected = True

        # return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(
                f'Error connecting to Airtable base {self.connection_data["base_id"]}, {e}!'
            )
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            if result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        result, columns=[x[0] for x in cursor.description]
                    ),
                )

            else:
                response = Response(RESPONSE_TYPE.OK)
                connection.commit()
        except Exception as e:
            logger.error(
                f'Error running query: {query} on table {self.connection_data["table_name"]} in base {self.connection_data["base_id"]}!'
            )
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                [self.connection_data["table_name"]], columns=["table_name"]
            ),
        )

        return response

    def get_columns(self) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    "column_name": list(
                        globals()[self.connection_data["table_name"]].columns
                    ),
                    "data_type": globals()[self.connection_data["table_name"]].dtypes,
                }
            ),
        )

        return response
