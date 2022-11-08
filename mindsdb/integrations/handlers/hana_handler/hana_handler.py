from textwrap import dedent
from collections import OrderedDict

from pandas import DataFrame

from hdbcli import dbapi
import sqlalchemy_hana.dialect as hana_dialect

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities import log

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class HanaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SAP Hana statements.
    """

    name = 'hana'

    def __init__(self, name: str, connection_data: dict, **kwargs):
        super().__init__(name)

        self.dialect = 'hana'
        self.parser = parse_sql
        self.connection_data = connection_data
        self.renderer = SqlalchemyRender(hana_dialect.HANAHDBCLIDialect)

        self.address = self.connection_data.get('host')
        self.port = self.connection_data.get('port')
        self.user = self.connection_data.get('user')
        self.password = self.connection_data.get('password')
        self.autocommit = self.connection_data.get('autocommit', True)
        self.properties = self.connection_data.get('properties')
        self.currentSchema = self.connection_data.get('schema', 'CURRENTUSER')
        self.databaseName = self.connection_data.get('database')
        self.encrypt = self.connection_data.get('encrypt', False)
        self.sslHostNameInCertificate = self.connection_data.get('sslHostNameInCertificate')
        self.sslValidateCertificate = self.connection_data.get('sslValidateCertificate', False)
        self.sslCryptoProvider = self.connection_data.get('sslCryptoProvider')
        self.sslTrustStore = self.connection_data.get('sslTrustStore')
        self.sslKeyStore = self.connection_data.get('sslKeyStore')
        self.cseKeyStorePassword = self.connection_data.get('cseKeyStorePassword')
        self.sslSNIHostname = self.connection_data.get('sslSNIHostname')
        self.sslSNIRequest = self.connection_data.get('sslSNIRequest', True)
        self.siteType = self.connection_data.get('siteType')
        self.splitBatchCommands = self.connection_data.get('splitBatchCommands', True)
        self.routeDirectExecute = self.connection_data.get('routeDirectExecute', False)
        self.secondarySessionFallback = self.connection_data.get('secondarySessionFallback', True)

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self):
        """
        Handles the connection to a SAP Hana database insance.
        """

        if self.is_connected is True:
            return self.connection

        connection = dbapi.connect(
            address=self.address,
            port=self.port,
            user=self.user,
            password=self.password,
            autocommit=self.autocommit,
            properties=self.properties,
            currentSchema=self.currentSchema,
            databaseName=self.databaseName,
            encrypt=self.encrypt,
            sslHostNameInCertificate=self.sslHostNameInCertificate,
            sslValidateCertificate=self.sslValidateCertificate,
            sslCryptoProvider=self.sslCryptoProvider,
            sslTrustStore=self.sslTrustStore,
            sslKeyStore=self.sslKeyStore,
            cseKeyStorePassword=self.cseKeyStorePassword,
            sslSNIHostname=self.sslSNIHostname,
            sslSNIRequest=self.sslSNIRequest,
            siteType=self.siteType,
            splitBatchCommands=self.splitBatchCommands,
            routeDirectExecute=self.routeDirectExecute,
            secondarySessionFallback=self.secondarySessionFallback
        )

        self.is_connected = True
        self.connection = connection
        return self.connection

    def disconnect(self):
        """
        Disconnects from the SAP HANA database
        """

        if self.is_connected is True:
            self.connection.close()
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the SAP HANA database
        :return: success status and error message if error occurs
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            with connection.cursor() as cur:
                cur.execute('SELECT 1 FROM SYS.DUMMY')
            response.success = True
        except dbapi.Error as e:
            log.logger.error(f'Error connecting to SAP HANA {self.address}, {e}!')
            response.error_message = e

        if response.success is True and need_to_close:
            self.disconnect()
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def native_query(self, query: str) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in SAP HANA
        :return: returns the records from the current recordset
        """

        need_to_close = self.is_connected is False

        connection = self.connect()
        with connection.cursor() as cur:
            try:
                cur.execute(query)
                if not cur.description:
                    response = Response(RESPONSE_TYPE.OK)
                else:
                    result = cur.fetchall()
                    response = Response(
                        RESPONSE_TYPE.TABLE,
                        DataFrame(
                            result,
                            columns=[x[0] for x in cur.description]
                        )
                    )
                connection.commit()
            except Exception as e:
                log.logger.error(f'Error running query: {query} on {self.address}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )
                connection.rollback()

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """

        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        List all tables in SAP HANA in the current schema
        """

        return self.native_query(f"""
            SELECT SCHEMA_NAME,
                   TABLE_NAME,
                   TABLE_TYPE
            FROM
                SYS.TABLES
            WHERE IS_SYSTEM_TABLE = 'FALSE'  
              AND IS_USER_DEFINED_TYPE = 'FALSE'
              AND IS_TEMPORARY = 'FALSE'
        """)

    def get_columns(self, table_name: str) -> Response:
        """
        List all columns in a table in SAP HANA in the current schema
        :param table_name: the table name for which to list the columns
        :return: returns the columns in the table
        """

        return self.renderer.dialect.get_columns(table_name)


connection_args = OrderedDict(
    host={
        'type': ARG_TYPE.STR,
        'description': 'The IP address/host name of the SAP HANA instance host.'
    },
    port={
        'type': ARG_TYPE.STR,
        'description': 'The port number of the SAP HANA instance.'
    },
    user={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the user name.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the password for the user.'
    },
    schema={
        'type': ARG_TYPE.STR,
        'description': 'Sets the current schema, which is used for identifiers without a schema.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the name of the database to connect to. (Not used for SAP HANA Cloud)'
    },
    autocommit={
        'type': ARG_TYPE.BOOL,
        'description': 'Sets the autocommit mode for the connection.'
    },
    properties={
        'type': ARG_TYPE.STR,
        'description': 'Additional dictionary with special properties of the connection.'
    },
    encrypt={
        'type': ARG_TYPE.BOOL,
        'description': 'Enables or disables TLS encryption.'
    },
    sslHostNameInCertificate={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the host name used to verify server\'s identity.'
    },
    sslValidateCertificate={
        'type': ARG_TYPE.BOOL,
        'description': 'Specifies whether to validate the server\'s certificate.'
    },
    sslCryptoProvider={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the cryptographic library provider used for TLS communication.'
    },
    sslTrustStore={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the path to a trust store file that contains the server\'s public certificates.'
    },
    sslKeyStore={
        'type': ARG_TYPE.STR,
        'description': 'Specifies the path to the keystore file that contains the client\'s identity.'
    },
    cseKeyStorePassword={
        'type': ARG_TYPE.STR,
        'description': 'Provides the password for the local key store.'
    },
    sslSNIHostname={
        'type': ARG_TYPE.STR,
        'description': dedent("""Specifies the name of the host that is attempting to connect at the start of
        the TLS handshaking process.""")
    },
    sslSNIRequest={
        'type': ARG_TYPE.BOOL,
        'description': 'Specifies whether SNI requests are enabled for TLS connections: TRUE/FALSE.'
    },
    siteType={
        'type': ARG_TYPE.STR,
        'description': dedent("""Specifies whether the connection is made to either the PRIMARY or SECONDARY
        site in an Active/Active (read enabled) system.""")
    },
    splitBatchCommands={
        'type': ARG_TYPE.BOOL,
        'description': 'Allows split and parallel execution of batch commands on partitioned tables.'
    },
    routeDirectExecute={
        'type': ARG_TYPE.BOOL,
        'description': dedent("""Converts direct execute into prepare and execute (routed execute) if the
        number of index servers is more than one and if statement routing is enabled.""")
    },
    secondarySessionFallback={
        'type': ARG_TYPE.BOOL,
        'description': dedent("""Forces the ongoing transaction on a non-anchor connection to fall back
        to the anchor/primary connection if this connection is dropped by the network or server.""")
    }
)

connection_args_example = OrderedDict(
    host='<uuid>.hana.trial-us10.hanacloud.ondemand.com',
    port=30013,
    user='DBADMIN',
    password='password',
    schema='MINDSDB',
)
