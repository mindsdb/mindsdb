from typing import Dict, Optional, Text, cast
import pandas as pd

from prometheus_api_client import PrometheusConnect
from mindsdb_sql.parser.ast.select.select import Select
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.utilities.handlers.query_utilities.select_query_utilities import SELECTQueryParser
from mindsdb.utilities import log

from mindsdb_sql.parser import ast

from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)

logger = log.getLogger(__name__)


def convert_conditions_to_promql(conditions: list):
    """
    Util that convets a list of parsed SQL conditions to a PromQL-filter string.
    """
    out = ",".join([f"{val[1]}{val[0]}'{str(val[2])}'" for val in conditions])
    if out:
        return "{" + out + "}"
    return ""


class PrometheusHandler(DatabaseHandler):
    """
    A class for handling connections and interactions with the Prometheus API.
    """
    name = 'prometheus'

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Prometheus server.
            kwargs: Arbitrary keyword arguments.
        """

        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected:
            self.disconnect()

    def connect(self) -> PrometheusConnect:
        """
        Establishes a connection to the Prometheus host.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            PrometheusConnect: A connection object to the Prometheus host.
        """
        if self.is_connected:
            return self.connection

        has_user = "user" in self.connection_data
        has_pass = "password" in self.connection_data
        if has_user != has_pass:
            raise ValueError(
                "Both user and password should be provided if one of them is provided!"
            )

        try:
            disable_ssl = (
                self.connection_data["disable_ssl"]
                if "disable_ssl" in self.connection_data
                else False
            )
            auth = (
                (self.connection_data["user"], self.connection_data["password"])
                if has_user
                else ()
            )
            self.connection = PrometheusConnect(
                url=self.connection_data["host"],
                auth=auth,
                disable_ssl=disable_ssl,
            )
            self.is_connected = True
            return self.connection
        except ConnectionError as conn_error:
            logger.error(
                f"Connection error when connecting to Prometheus: {conn_error}"
            )
            raise
        except Exception as unknown_error:
            logger.error(
                f"Unknown error when connecting to Prometheus: {unknown_error}"
            )
            raise

    def disconnect(self):
        """
        Closes the connection to the Prometheus host if it's currently open.
        """

        if not self.is_connected:
            return

        del self.connection
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Prometheus host.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        connection = self.connect()
        ok = connection.check_prometheus_connection()
        response.success = ok

        if not ok:
            logger.error("Error connecting to Prometheus!")

        return response

    def native_query(self, query: Text, limit: Optional[int] = None) -> Response:
        """
        Executes a native PromQL query on the Prometheus host and returns the result.

        Args:
            query (str): The PromQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        response = Response(RESPONSE_TYPE.TABLE, False)
        need_to_close = self.is_connected is False
        connection = self.connect()

        try:
            result = connection.custom_query(query)

            rows = []
            for row in result:
                rows.append(row["metric"])

            df = pd.DataFrame(rows)
            if "__name__" in df.columns:
                df = df.drop(["__name__"], axis=1)
            response.data_frame = df
        except Exception as error:
            logger.error(f'Unknown error running query: {query} on Prometheus, {error}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(error)
            )

        if need_to_close:
            self.disconnect()
        elif not self.is_connected:
            self.is_connected = False

        return response

    def query(self, query: ast.ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Prometheus host and retrieves the data.
        Currently, only SELECT statements with WHERE and AND are supported.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result.
        """
        if not isinstance(query, Select):
            raise ValueError("Only select queries are supported")

        select_query = cast(Select, query)
        from_table = select_query.from_table
        conditions = extract_comparison_conditions(select_query.where)
        conditions = convert_conditions_to_promql(conditions)

        select_statement_parser = SELECTQueryParser(
            query,
            select_query.from_table,
            self.get_columns(select_query.from_table)
        )

        # TODO: use the orderbys returned by this parser
        _, _, _, result_limit = select_statement_parser.parse_query()

        return self.native_query(str(from_table) + conditions, limit=result_limit)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all "tables" (i.e., metrics) in the Prometheus host.

        Returns:
            Response: A response object containing a list of tables.
        """
        all_metrics = self.connection.all_metrics()

        response = Response(RESPONSE_TYPE.TABLE)
        tables = pd.DataFrame(all_metrics, columns=pd.Index(["table_name"]))
        response.data_frame = tables

        return response

    def get_columns(self, _: Text) -> Response:
        """
        Retrieves a list of all "columns" (i.e., labels) in the Prometheus host.

        Returns:
            Response: A response object containing a list of columns.
        """
        all_labels = self.connection.get_label_names()

        response = Response(RESPONSE_TYPE.TABLE)
        columns = pd.DataFrame(all_labels, columns=pd.Index(["column_name"]))
        response.data_frame = columns

        return response
