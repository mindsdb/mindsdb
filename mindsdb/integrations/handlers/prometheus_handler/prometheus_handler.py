import os
import requests

from prometheus_api_client import utils
from prometheus_pandas import query as prometheus_pandas

from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

class PrometheusTable(APITable):
    def select(self, query: ast.Select) -> Response:
        conditions = extract_comparison_conditions(query.where)
        params = {
            "query": None,
            "start_time": utils.parse_datetime("5m"),
            "end_time": utils.parse_datetime("now"),
            "step": "1m"
        }

        for operation, key, value in conditions:
            if operation == 'or':
                raise NotImplementedError(f'OR is not supported')
            if key == 'start_time':
                params['start_time'] = utils.parse_datetime(value)
            elif key == 'end_time':
                params['end_time'] = utils.parse_datetime(value)
            elif key == 'step':
                params[key] = value
            elif key == 'query':
                if operation == '=':
                    params[key] = value
                else:
                    NotImplementedError(f'Unknown op: {operation}')

        if params.query is None:
            ValueError("Query must be provided")

        return self.handler.call_prometheus_api(params)

class PrometheusHandler(APIHandler):
    """A class for handling connections and interactions with the Prometheus API.

    Attributes:
        api (prometheus_pandas.Prometheus): The `prometheus_pandas.Prometheus` object for interacting with the Prometheus API.

    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get('connection_data', {})
        handler_config = Config().get('prometheus_handler', {})

        for k in ['prometheus_host', 'disable_ssl']:
            if k in args:
                self.connection_args[k] = args[k]
            elif f'PROMETHEUS_{k.upper()}' in os.environ:
                self.connection_args[k] = os.environ[f'PROMETHEUS_{k.upper()}']
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        prometheus = PrometheusTable(self)
        self._register_table('prometheus', prometheus)
    
    def create_connection(self):
        return prometheus_pandas.Prometheus(
            self.connection_args['prometheus_host'],
        )

    def connect(self):
        """Authenticate with the Prometheus API."""

        if self.is_connected is True:
            return self.api

        self.api = self.create_connection()

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            api = self.connect()
            api.query(query="vector(1)")
            response.success = True
        except requests.exceptions.RequestException as e:
            response.error_message = f'Error connecting to Prometheus api: {e}.'
            log.logger.error(response.error_message)
            
        if response.success is False and self.is_connected is True:
            self.is_connected = False

        return response

    def call_prometheus_api(self, params: dict = None):
        api = self.connect()
        return api.query_range(
            params.get("query"), 
            params.get("start_time"), 
            params.get("end_time"),
            params.get("step")
        )
