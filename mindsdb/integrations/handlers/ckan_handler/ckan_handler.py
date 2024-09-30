from ckanapi import RemoteCKAN as rc
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIHandler, APITable, APIResource
from mindsdb.integrations.libs.response import HandlerStatusResponse, HandlerResponse, RESPONSE_TYPE
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class CkanHandler(APIHandler):
    """
    This handler handles connection and consumption of the CKAN API.
    """
    name = "ckan"

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        self.connection_args = kwargs.get('connection_data')

    def connect(self) -> HandlerStatusResponse:
        """
        Handles the connection to a CKAN remote portal instance.
        """
        url = self.connection_args.get('url')
        api_key = self.connection_args.get('api_key')
        try:
            self.connection = rc(url) if api_key is None else rc(url, apikey=api_key)
            self.is_connected = True
            return HandlerStatusResponse(True)
        except Exception as e:
            return HandlerStatusResponse(False, f'Failed to connect to CKAN: {e}')

    def disconnect(self):
        self.is_connected = False

    def check_connection(self) -> HandlerStatusResponse:
        response = HandlerStatusResponse(False)
        try:
            self.connect()
            result = self.ckan.action.status_show()
            if 'datastore' not in result.get('extensions'):
                """
                If the CKAN instance does not have the datastore extension,
                we can't use it.
                """
                response.message = 'CKAN datastore is not enabled'
                response.status = False
                self.is_connected = False
                return response

        except Exception as e:
            logger.error(f'Error connecting to CKAN: {e}!')
            self.is_connected = False
            response.error_message = e

        if response.success is False and self.is_connected is True:
            self.is_connected = False

        response.success = True
        return response

    def query(self, query: ASTNode) -> HandlerResponse:
        if not self.ckan:
            self.connect()
        query_str = self.renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def native_query(self, query_str: str) -> HandlerResponse:
        """
        Execute a query on the CKAN instance.
        :param query_str: The query to execute.
        :return: The response of the query.
        """
        if not self.ckan:
            self.connect()
        result = self.ckan.action.datastore_search_sql(sql=query_str)
        if len(result.get('records')) > 0:
            df = pd.DataFrame(result['records'])
            response = HandlerResponse(RESPONSE_TYPE.TABLE, df)
        else:
            response = HandlerResponse(RESPONSE_TYPE.TABLE, None)
        return response

    def get_tables(self) -> HandlerResponse:
        if not self.ckan:
            self.connect()
        result = self.ckan.action.datastore_search(resource_id='_table_metadata')
        df = pd.DataFrame(result['records'])
        response = HandlerResponse(RESPONSE_TYPE.TABLE, df)
        return response
