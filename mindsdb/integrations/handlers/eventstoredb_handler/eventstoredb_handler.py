from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from .utils.helpers import *
import requests
import pandas as pd

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
    #defaults to an insecure localhost single node
    scheme = 'http'
    host = 'localhost'
    port = '2113'
    is_connected = None
    basic_url = ""
    read_batch_size = 500 #should be adjusted based on use case

    def __init__(self, name, **kwargs):
        super().__init__(name)
        connection_data = kwargs['connection_data']
        self.host = connection_data.get('host')
        if connection_data.get('tls') is not None and connection_data.get('tls') == 'True':
            self.scheme = 'https'
        if connection_data.get('port') is not None:
            self.port = connection_data.get('port')
        if connection_data.get('page_size') is not None:
            if isinstance(connection_data.get('page_size'), int) and connection_data.get('page_size') > 0:
                self.read_batch_size = connection_data.get('page_size')

        log.logger.info(f'scheme: {self.scheme} host: {self.host}  port:{self.port} page: {self.read_batch_size}')
        self.basic_url = build_basic_url(self.scheme, self.host, self.port)


    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self, **kwargs):
        if self.check_connection() == StatusResponse(True):
            self.is_connected = True
            log.logger.info(f'{self.name} connection successful!')
            return StatusResponse(True)
        log.logger.info(f'{self.name} connection could not be made.')
        return StatusResponse(False)

    def disconnect(self, **kwargs):
        return

    def check_connection(self) -> StatusResponse:
        try:
            response = requests.get(build_health_url(self.basic_url))
            if response.status_code == 204:
                return StatusResponse(True)
        except Exception as e:
            log.logger.Error(f'{self.name} check connection failed with: {e}!')
        return StatusResponse(False)

    def query(self, query: ASTNode) -> Response:
        if type(query) == Select:
            stream_name = query.from_table.parts[-1] #i.e. table name
            headers = {
                'Accept': 'application/json',
                'ES-ResolveLinkTo': "True"
            }

            params = {
                'embed': 'tryharder'
            }
            stream_endpoint = build_stream_url(self.basic_url, stream_name)
            response = requests.get(stream_endpoint, params=params, headers=headers)
            entries = []
            if response is not None and response.status_code == 200:
                jsonResponse = response.json()
                for entry in jsonResponse["entries"]:
                    entry = entry_to_df(entry)
                    entries.append(entry)
                while True:
                    endOfStream = True
                    if 'links' in jsonResponse:
                        for link in jsonResponse['links']:
                            if 'relation' in link:
                                if link['relation'] == 'next':
                                    endOfStream = False
                                    response = requests.get(build_next_url(link['uri'],self.read_batch_size),
                                                            params=params, headers=headers)
                                    jsonResponse = response.json()
                                    for entry in jsonResponse["entries"]:
                                        entry = entry_to_df(entry)
                                        entries.append(entry)
                    if endOfStream:
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

    def get_tables(self) -> Response:
        """
        List all streams i.e tables
        """
        headers = {
            'Accept': 'application/json',
            'ES-ResolveLinkTo': "True"
        }

        params = {
            'embed': 'tryharder'
        }
        stream_endpoint = build_streams_url(self.basic_url)
        response = requests.get(stream_endpoint, params=params, headers=headers)
        streams = []
        if response is not None and response.status_code == 200:
            jsonResponse = response.json()
            for entry in jsonResponse["entries"]:
                if "title" in entry:
                    streams.append(entry["title"].split('@')[1])
            while True:
                endOfStream = True
                if 'links' in jsonResponse:
                    for link in jsonResponse['links']:
                        if 'relation' in link:
                            if link['relation'] == 'next':
                                endOfStream = False
                                response = requests.get(build_next_url(link['uri'],self.read_batch_size),
                                                        params=params, headers=headers)
                                jsonResponse = response.json()
                                for entry in jsonResponse["entries"]:
                                    if "title" in entry:
                                        streams.append(entry["title"].split('@')[1])
                if endOfStream:
                    break

        df = pd.DataFrame(streams,
                          columns=['table_name'])
        return Response(
            RESPONSE_TYPE.TABLE,
            df
        )

    def get_columns(self, table_name) -> Response:
        # TODO: get the last event from the stream: table_name
        # TODO: infer type?
        return Response(
            RESPONSE_TYPE.OK,
            error_message="Not implemented yet"
        )
