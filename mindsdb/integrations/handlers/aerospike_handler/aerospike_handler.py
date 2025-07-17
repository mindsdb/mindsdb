import re
from typing import Optional

import duckdb
import aerospike
import pandas as pd
# from sqlalchemy import create_engine

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast.base import ASTNode

# from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class AerospikeHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Solr SQL statements.
    """
    name = 'aerospike'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'aerospike'
        self.connection_data = connection_data
        self.kwargs = kwargs
        if not self.connection_data.get('host'):
            raise Exception("The host parameter should be provided!")
        if not self.connection_data.get('port'):
            raise Exception("The port parameter should be provided!")

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected is True:
            return self.connection

        user = self.connection_data.get('user', None)
        password = self.connection_data.get('password', None)
        config = {
            'user': user,
            'password': password,
            'hosts': [(self.connection_data.get('host'), self.connection_data.get('port'))],
        }
        connection = aerospike.client(config).connect()
        self.is_connected = True
        self.connection = connection.connect()
        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """
        if self.is_connected is False:
            return
        self.connection.close()
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the Aerospike database
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            response.error_message = str(e)

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False
        return response

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            # where is not supported
            selected_bins, aero_ns, aero_set = self.parse_aql_query(query)
            aero_ns = aero_ns.lower()
            aero_set = aero_set.lower()
            scan = connection.scan(aero_ns.lower(), aero_set.lower())
            res = scan.results()
            data_df = pd.DataFrame.from_records([r[2] for r in res])
            if ' where ' in query or ' WHERE ' in query or '*' not in selected_bins:
                new_query = re.sub(r'FROM [\w\.]+', 'FROM ' + 'data_df', query, 1)
                new_query = new_query.replace(f'{aero_set}.', '')
                data_df = duckdb.query(new_query).to_df()

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_df
            )
        except Exception as e:
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement.
        """
        return self.native_query(query.to_string())

    def parse_aql_query(self, aql_query):
        # Split the AQL query into tokens
        tokens = [t.replace(',', '').upper() for t in re.split(r'\s+', aql_query)]
        # Extract the relevant components
        select_index = tokens.index("SELECT")
        from_index = tokens.index("FROM")
        # where_index = tokens.index("WHERE")

        selected_bins = tokens[select_index + 1:from_index]
        namespace_set = tokens[from_index + 1]
        aero_ns, aero_set = namespace_set.split('.') if '.' in namespace_set else None, namespace_set
        if not aero_ns:
            aero_ns = self.connection_data.get('namespace')
        # filter_condition = " ".join(tokens[where_index + 1:])
        return selected_bins, aero_ns, aero_set

    def get_tables(self) -> Response:
        """
        Get a list with all of the tables in Aerospike
        """
        need_to_close = self.is_connected is False
        connection = self.connect()

        data_lst = []
        request = "sets"

        try:
            for node, (err, res) in list(connection.info_all(request).items()):
                if res:
                    entries = [entry.strip() for entry in res.strip().split(';') if entry.strip()]
                    for entry in entries:
                        data = [d for d in entry.split('=') if ':set' in d or ':objects' in d]
                        ele = [None, None, None]
                        for d in data:
                            if ':set' in d:
                                ele[0] = d.split(':')[0]
                            if ':objects' in d:
                                ele[1] = d.split(':')[0]
                            if d[0] or d[1]:
                                ele[2] = request
                        data_lst.append(ele)

            response = Response(
                RESPONSE_TYPE.TABLE,
                pd.DataFrame(data_lst, columns=['table_schema', 'table_name', 'table_type'])
            )
        except Exception as e:
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def get_columns(self, table_name: str) -> Response:
        """
        Show details about the table
        """
        need_to_close = self.is_connected is False
        connection = self.connect()

        column_df = pd.DataFrame([], columns=['column_name', 'data_type'])

        try:
            response_table = self.get_tables()
            df = response_table.data_frame
            if not len(df):
                return column_df
            df = df[df['table_name'] == table_name]
            tbl_dtl_arr = df.iloc[0][['table_schema', 'table_name']]
            scan = connection.scan(tbl_dtl_arr[0], tbl_dtl_arr[1])
            res = scan.results()
            data_df = pd.DataFrame.from_records([r[2] for r in res])
            column_df = pd.DataFrame(data_df.dtypes).reset_index()
            column_df.columns = ['column_name', 'data_type']
            response = Response(
                RESPONSE_TYPE.TABLE,
                column_df
            )
        except Exception as e:
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response
