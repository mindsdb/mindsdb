import pandas as pd
from ckanapi import RemoteCKAN
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.libs.response import (HandlerStatusResponse,
                                                HandlerResponse,
                                                RESPONSE_TYPE)
from mindsdb_sql.parser import ast
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class DynamicResourceTable(APITable):
    def __init__(self, handler, resource_id, fields):
        super().__init__(handler)
        self.resource_id = resource_id
        self.fields = fields

    def select(self, query: ast.Select) -> HandlerResponse:
        params = {
            'resource_id': self.resource_id,
            'limit': 100
        }

        # Handle WHERE conditions
        if query.where:
            conditions = query.where.get_conditions()
            for op, arg1, arg2 in conditions:
                if op == '=':
                    params[arg1] = arg2
                elif op in ('>', '<', '>=', '<='):
                    params[f'{arg1}{op}'] = arg2

        # Handle LIMIT clause
        # Currently gets the first 100 records
        # TODO: Implement proper LIMIT handling
        if query.limit:
            params['limit'] = query.limit.value

        result = self.handler.call_ckan_api('datastore_search', params)

        # Collect the columns to return in the result
        # Double check if the columns are valid
        columns = [target.parts[-1] for target in query.targets
                   if isinstance(target, ast.Identifier)]

        if not columns or '*' in columns:
            columns = result.columns

        return result[columns]

    def get_columns(self):
        return [field['id'] for field in self.fields]


class CkanHandler(APIHandler):
    name = 'ckan'

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.connection = None
        self.is_connected = False
        self.connection_args = kwargs.get('connection_data', {})

        self.connect()
        self._create_tables()

    def connect(self):
        if self.is_connected:
            return self.connection

        url = self.connection_args.get('url')
        api_key = self.connection_args.get('api_key')
        if not url:
            raise ValueError('CKAN URL is required')
        try:
            self.connection = RemoteCKAN(url, apikey=api_key)
            self.is_connected = True
        except Exception as e:
            logger.error(f'Error connecting to CKAN: {e}')
            raise ConnectionError(f'Failed to connect to CKAN: {e}')

        return self.connection

    def _create_tables(self):
        metadata = self.call_ckan_api('datastore_search',
                                      {'resource_id': '_table_metadata'})
        for resource in metadata.to_dict('records'):
            resource_id = resource['name']
            try:
                resource_info = self.call_ckan_api('datastore_search',
                                                   {'resource_id': resource_id,
                                                    'limit': 0})
                fields = resource_info.get('fields', [])
                table = DynamicResourceTable(self, resource_id, fields)
                self._register_table(resource_id, table)
            except Exception as e:
                logger.error(f'Error creating table for resource\
                             {resource_id}: {e}')
                pass

    def check_connection(self) -> HandlerStatusResponse:
        try:
            self.connect()
            return HandlerStatusResponse(success=True)
        except Exception as e:
            return HandlerStatusResponse(success=False, error_message=str(e))

    def native_query(self, query: str = None) -> HandlerResponse:
        method, params = self.parse_native_query(query)
        df = self.call_ckan_api(method, params)
        return HandlerResponse(RESPONSE_TYPE.TABLE, df)

    def call_ckan_api(self, method_name: str, params: dict) -> pd.DataFrame:
        """
        Call a CKAN API method with the given parameters

        :param method_name: Name of the CKAN API method
        :param params: Parameters to pass to the method
        :return: DataFrame with the results
        """
        connection = self.connect()
        method = getattr(connection.action, method_name)

        try:
            result = method(**params)
            if 'records' in result:
                df = pd.DataFrame(result['records'])
                if 'fields' in result:
                    df.fields = result['fields']
                return df
            else:
                return pd.DataFrame(result)
        except Exception as e:
            logger.error(f'Error calling CKAN API: {e}')
            # raise RuntimeError(f'Failed to call CKAN API: {e}')

    @staticmethod
    def parse_native_query(query: str):
        """
        Parses the native query string of format method
        (arg1=val1, arg2=val2, ...) and returns the method name and arguments.
        """

        parts = query.split(':')
        method = parts[0].strip()
        params = {}
        if len(parts) > 1:
            param_pairs = parts[1].split(',')
            for pair in param_pairs:
                key, value = pair.split('=')
                params[key.strip()] = value.strip()
        return method, params