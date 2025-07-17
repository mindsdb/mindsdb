from typing import Optional

import json
import time
import requests
import pandas as pd

from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.libs.base import DatabaseHandler
from sqlalchemy_dremio.base import DremioDialect

from mindsdb_sql_parser.ast.base import ASTNode

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class DremioHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Dremio statements.
    """

    name = 'dremio'

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
        self.dialect = 'dremio'

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.base_url = f"http://{self.connection_data['host']}:{self.connection_data['port']}"

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> dict:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        headers = {
            'Content-Type': 'application/json',
        }

        data = '{' + f'"userName": "{self.connection_data["username"]}","password": "{self.connection_data["password"]}"' + '}'

        response = requests.post(self.base_url + '/apiv2/login', headers=headers, data=data)

        return {
            'Authorization': '_dremio' + response.json()['token'],
            'Content-Type': 'application/json',
        }

    def disconnect(self):
        """
        Close any existing connections.
        """

        self.is_connected = False
        return

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
            logger.error(f'Error connecting to Dremio, {e}!')
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

        query = query.replace('"', '\\"').replace('\n', ' ')

        need_to_close = self.is_connected is False

        auth_headers = self.connect()
        data = '{' + f'"sql": "{query}"' + '}'

        try:
            sql_result = requests.post(self.base_url + '/api/v3/sql', headers=auth_headers, data=data)

            job_id = sql_result.json()['id']

            if sql_result.status_code == 200:
                logger.info('Job creation successful. Job id is: ' + job_id)
            else:
                logger.info('Job creation failed.')

            logger.info('Waiting for the job to complete...')

            job_status = requests.request("GET", self.base_url + "/api/v3/job/" + job_id, headers=auth_headers).json()[
                'jobState']

            while job_status != 'COMPLETED':
                if job_status == 'FAILED':
                    logger.error('Job failed!')
                    break

                time.sleep(2)
                job_status = requests.request("GET", self.base_url + "/api/v3/job/" + job_id, headers=auth_headers).json()[
                    'jobState']

            job_result = json.loads(requests.request("GET", self.base_url + "/api/v3/job/" + job_id + "/results", headers=auth_headers).text)

            if 'errorMessage' not in job_result:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.DataFrame(
                        job_result['rows']
                    )
                )
            else:
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=str(job_result['errorMessage'])
                )

        except Exception as e:
            logger.error(f'Error running query: {query} on Dremio!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

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

        renderer = SqlalchemyRender(DremioDialect)
        query_str = renderer.get_string(query, with_failback=True)
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        query = """
            SELECT
                TABLE_NAME,
                TABLE_SCHEMA,
                CASE
                    WHEN TABLE_TYPE = 'TABLE' THEN 'BASE TABLE'
                    ELSE TABLE_TYPE
                END AS TABLE_TYPE
            FROM INFORMATION_SCHEMA."TABLES"
            WHERE TABLE_TYPE <> 'SYSTEM_TABLE';
        """
        return self.native_query(query)

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        query = f"DESCRIBE {table_name}"
        result = self.native_query(query)
        df = result.data_frame
        result.data_frame = df.rename(columns={'COLUMN_NAME': 'Field', 'DATA_TYPE': 'Type'})
        return result
