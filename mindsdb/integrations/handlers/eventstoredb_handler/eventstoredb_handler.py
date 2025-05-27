from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb_sql_parser.ast import Select
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from .utils.helpers import get_auth_string, build_basic_url, build_health_url, build_stream_url, build_streams_url, build_next_url, entry_to_df, build_stream_url_last_event
import requests
import pandas as pd
import re

logger = log.getLogger(__name__)


class EventStoreDB(DatabaseHandler):
    """
    Handler for EventStoreDB
    The handler uses the Atom Pub Over HTTP of EventStoreDB.
    This means that you need to enable AtomPuvOverHTTP on your EventStoreDB instance if you are using v20+

    Why not gRPC? At the moment we cannot use https://pypi.org/project/esdbclient/ which uses the gRPC endpoint
    because mysql-connector-python 8.0.32 requires protobuf<=3.20.3,>=3.11.0, but esdbclient
    requires protobuf 4.22.1.

    Why not TCP? At the moment https://github.com/epoplavskis/photon-pump only works in insecure mode and
    configuration is limited.

    Third reason, there is no official Python client at the moment of writing of this handler.
    But once there is better ESDB Python support for gRPC, we should move this integration from AtomPub to gRPC.
    """

    name = 'eventstoredb'
    # defaults to an insecure localhost single node
    scheme = 'http'
    host = 'localhost'
    port = '2113'
    is_connected = None
    basic_url = ""
    read_batch_size = 500  # should be adjusted based on use case
    headers = {
        'Accept': 'application/json',
        'ES-ResolveLinkTo': "True"
    }
    tlsverify = False

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.parser = parse_sql
        connection_data = kwargs['connection_data']
        username = connection_data.get('username')
        password = connection_data.get('password')
        self.host = connection_data.get('host')
        if connection_data.get('tls') is not None and isinstance(connection_data.get('tls'), bool) \
                and connection_data.get('tls'):
            self.scheme = 'https'
        if connection_data.get('port') is not None and isinstance(connection_data.get('port'), int):
            self.port = connection_data.get('port')
        if connection_data.get('page_size') is not None:
            if isinstance(connection_data.get('page_size'), int) and connection_data.get('page_size') > 0:
                self.read_batch_size = connection_data.get('page_size')
        if username is not None and password is not None:
            if isinstance(username, str) and isinstance(password, str):
                self.headers['authorization'] = get_auth_string(username, password)
        if connection_data.get('tlsverify') is not None and isinstance(connection_data.get('tlsverify'), bool):
            self.tlsverify = connection_data.get('tlsverify')

        self.basic_url = build_basic_url(self.scheme, self.host, self.port)

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self, **kwargs):
        if self.check_connection() == StatusResponse(True):
            self.is_connected = True
            logger.info(f'{self.name} connection successful!')
            return StatusResponse(True)
        logger.info(f'{self.name} connection could not be made.')
        return StatusResponse(False)

    def disconnect(self, **kwargs):
        return

    def check_connection(self) -> StatusResponse:
        try:
            response = requests.get(build_health_url(self.basic_url), verify=self.tlsverify)
            if response.status_code == 204:
                return StatusResponse(True)
        except Exception as e:
            logger.error(f'{self.name} check connection failed with: {e}!')
        return StatusResponse(False)

    def query(self, query: ASTNode) -> Response:
        if type(query) == Select:
            stream_name = query.from_table.parts[-1]  # i.e. table name
            params = {
                'embed': 'tryharder'
            }
            stream_endpoint = build_stream_url(self.basic_url, stream_name)
            response = requests.get(stream_endpoint, params=params, headers=self.headers, verify=self.tlsverify)
            entries = []
            if response is not None and response.status_code == 200:
                json_response = response.json()
                for entry in json_response["entries"]:
                    entry = entry_to_df(entry)
                    entries.append(entry)
                while True:
                    end_of_stream = True
                    if 'links' in json_response:
                        for link in json_response['links']:
                            if 'relation' in link:
                                if link['relation'] == 'next':
                                    end_of_stream = False
                                    response = requests.get(build_next_url(link['uri'], self.read_batch_size),
                                                            params=params, headers=self.headers, verify=self.tlsverify)
                                    json_response = response.json()
                                    for entry in json_response["entries"]:
                                        entry = entry_to_df(entry)
                                        entries.append(entry)
                    if end_of_stream:
                        break

            df = pd.concat(entries)

            return Response(
                RESPONSE_TYPE.TABLE,
                df
            )
        else:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message="Only 'select' queries are supported for EventStoreDB"
            )

    def native_query(self, query: str) -> Response:
        ast = self.parser(query)
        return self.query(ast)

    def get_tables(self) -> Response:
        """
        List all streams i.e tables
        """
        params = {
            'embed': 'tryharder'
        }
        stream_endpoint = build_streams_url(self.basic_url)
        response = requests.get(stream_endpoint, params=params, headers=self.headers, verify=self.tlsverify)
        streams = []
        if response is not None and response.status_code == 200:
            json_response = response.json()
            for entry in json_response["entries"]:
                if "title" in entry:
                    streams.append(entry["title"].split('@')[1])
            while True:
                end_of_stream = True
                if 'links' in json_response:
                    for link in json_response['links']:
                        if 'relation' in link:
                            if link['relation'] == 'next':
                                end_of_stream = False
                                response = requests.get(build_next_url(link['uri'], self.read_batch_size),
                                                        params=params, headers=self.headers, verify=self.tlsverify)
                                json_response = response.json()
                                for entry in json_response["entries"]:
                                    if "title" in entry:
                                        streams.append(entry["title"].split('@')[1])
                if end_of_stream:
                    break

        df = pd.DataFrame(streams,
                          columns=['table_name'])
        return Response(
            RESPONSE_TYPE.TABLE,
            df
        )

    def get_columns(self, table_name) -> Response:
        params = {
            'embed': 'tryharder'
        }
        stream_endpoint = build_stream_url_last_event(self.basic_url, table_name)
        response = requests.get(stream_endpoint, params=params, headers=self.headers, verify=self.tlsverify)
        entry = None
        if response is not None and response.status_code == 200:
            json_response = response.json()
            if json_response is not None and len(json_response) > 0:
                entry = entry_to_df(json_response["entries"][0])
        if entry is None:
            return Response(
                RESPONSE_TYPE.ERROR,
                "Could not retrieve JSON event data to infer column types."
            )
        data = []
        for k, v in entry.items():
            data.append([k, v.dtypes.name])
        df = pd.DataFrame(data, columns=['Field', 'Type'])
        return Response(
            RESPONSE_TYPE.TABLE,
            df
        )


def parse_sql(sql, dialect='sqlite'):
    # remove ending semicolon and spaces
    sql = re.sub(r'[\s;]+$', '', sql)

    from mindsdb_sql_parser.lexer import MindsDBLexer
    from mindsdb_sql_parser.parser import MindsDBParser
    lexer, parser = MindsDBLexer(), MindsDBParser()

    tokens = lexer.tokenize(sql)
    ast = parser.parse(tokens)
    return ast
