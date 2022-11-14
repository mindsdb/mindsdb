from collections import OrderedDict
from mindsdb.integrations.libs.base import DatabaseHandler

from mindsdb.utilities import log
from mindsdb_sql.parser.ast.base import ASTNode
from couchbase.n1ql import N1QLQuery
import pandas as pd

from typing import Optional

from datetime import timedelta

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE



from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import UnAmbiguousTimeoutException
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,
                               QueryOptions)

from couchbase.exceptions import QueryErrorContext,KeyspaceNotFoundException,CouchbaseException

class CouchbaseHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Couchbase statements. 
    """

    name = 'couchbase'

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data")
        
        self.scope = self.connection_data.get('scope') or '_default'
        
        self.bucket_name = self.connection_data.get('bucket')
        self.cluster = None
        
        self.is_connected = False

    def connect(self):
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        """
        if self.is_connected is True:
            return self.cluster
        
        # User Input ends here.
        endpoint = self.connection_data.get('host')
        username = self.connection_data.get('user')
        password = self.connection_data.get('password')
        bucket_name = self.connection_data.get('bucket')
        # Connect options - global timeout opts
        timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))
        
        auth = PasswordAuthenticator(
            username,
            password,
            # NOTE: If using SSL/TLS, add the certificate path.
            # We strongly reccomend this for production use.
            # cert_path=cert_path
        )


        cluster = Cluster.connect(f'couchbase://{endpoint}', ClusterOptions(auth))
        
        # # Wait until the cluster is ready for use.
        cluster.wait_until_ready(timedelta(seconds=5))
        
        self.is_connected = cluster.connected
        self.cluster = cluster
        return self.cluster

    
    def disconnect(self):
        """ Close any existing connections
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
            print(self.is_connected)
            result.success = cluster.connected
        except UnAmbiguousTimeoutException as e:
            log.logger.error(f'Error connecting to Couchbase {self.connection_data["bucket"]}, {e}!')
            result.error_message = str(e)

        if result.success is True and need_to_close:
            self.disconnect()
        if result.success is False and self.is_connected is True:
            self.is_connected = False
        return result
    
    def native_query(self, query: str) -> Response:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """

        self.connect()
        cluster = self.cluster
        bucket = cluster.bucket(self.bucket_name)
        cb = bucket.scope(self.scope)
        try:
            q = query
            row_iter = cb.query(q)
            data = {}
            # keys = []
            for collection in row_iter:
                # data.append()
                # print(type(collection))
                for collection_name, row in collection.items():
                    if isinstance(row, dict):
                        for k, v in row.items():
                            if data.get(k) is None:
                                data[k] = []
                            data[k].append(v)
                    else:
                        for k, v in collection.items():
                                if data.get(k) is None: 
                                    data[k] = []
                                data[k].append(v)
            if len(data) > 0:
                df = pd.DataFrame(data)
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    df
                )
            else:
                response = Response(RESPONSE_TYPE.OK)
        except CouchbaseException as e:
            print(f'Error: {e.error_context.first_error_message}')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=f'{e.error_context.first_error_message}'
            )
        return response
    
    def query(self, query:ASTNode)-> Response:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
        return self.native_query(query.to_string())
    
    def get_tables(self) -> Response:
        """
        Get a list with of collection in database
        """

        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
 
        collections = []

        for _scope in bucket.collections().get_all_scopes():
                for __collections in _scope.collections:
                    collections.append(__collections.name)
        collections_ar = [
            [i] for i in collections
        ]
        
        df = pd.DataFrame(collections_ar, columns=['TABLE_NAME'])
        
        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )
        
        return response

    def get_columns(self, table_name) -> Response:
        """ Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """
        
        response = Response(False)

        cluster = self.connect()
        bucket = cluster.bucket(self.bucket_name)
        cb = bucket.scope(self.scope)

        try:
            q = f'SELECT * FROM `{table_name}` limit 1'
            row_iter = cb.query(q)
            # print(row_iter.execute())
            data = []
            for row in row_iter:   
                for k, v in row[table_name].items():
                    data.append([k, type(v).__name__])
            df = pd.DataFrame(data, columns=['Field', 'Type'])
            response = Response(
                RESPONSE_TYPE.TABLE,
                df
            )
        except KeyspaceNotFoundException as e:
            print(f'Error: {e.error_context.first_error_message}')
            response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=f'Error: {e.error_context.first_error_message}'
                )
                
        return response



connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the Couchbase server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the Couchbase server.'
    },
    bucket={
        'type': ARG_TYPE.STR,
        'description': 'The database/bucket name to use when connecting with the Couchbase server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': '--your-instance--.dp.cloud.couchbase.com or IP address of the Couchbase server.'
    },
    scope={
        'type': ARG_TYPE.STR,
        'description': 'The scope use in the query context in Couchbase server. If blank, scope will be "_default".'
    }
)
connection_args_example = OrderedDict(
    host='127.0.0.1',
    user='root',
    password='password',
    bucket='bucket'
)
