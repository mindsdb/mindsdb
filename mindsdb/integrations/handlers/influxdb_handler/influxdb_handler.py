from mindsdb.integrations.handlers.influxdb_handler.influxdb_tables import InfluxDBTables
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

import requests
import pandas as pd
import io

logger = log.getLogger(__name__)

class InfluxDBHandler(APIHandler):
    """InfluxDB handler implementation"""

    def __init__(self, name=None, **kwargs):
        """Initialize the InfluxDB handler.
        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})

        self.parser = parse_sql
        self.dialect = 'influxdb'
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.connection = None
        self.is_connected = False

        influxdb_tables_data = InfluxDBTables(self)
        self._register_table("tables", influxdb_tables_data)

    def connect(self):
        """Set up the connection required by the handler.
        Returns
        -------
        None

        Raises Expection if ping check fails
        """
        if self.is_connected is True:
            return self.connection

        url = f"{self.connection_data['influxdb_url']}/ping"
        response = requests.request("GET",url)

        if response.status_code == 204:
            self.connection = response
        else:
            raise Expection
        

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to InfluxDB API: {e}!")
            response.error_message = e

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

    def call_influxdb_tables(self):
        """Pulls all the records from the given  InfluxDB table and returns it select()
    
        Returns
        -------
        pd.DataFrame of all the records of the particular InfluxDB 
        """
        url = f"{self.connection_data['influxdb_url']}/query"

        params = {
            ('db',f"{self.connection_data['influxdb_db_name']}"),
            ('q','SELECT * FROM '+ f"{self.connection_data['influxdb_table_name']}" )
        }
        headers = {
            "Authorization": f"Token {self.connection_data['influxdb_token']}",
            "Accept": "application/csv",
        }

        response = requests.request("GET",url,params=params,headers=headers)
        influxdb_df = pd.read_csv(io.StringIO(response.text))

        return influxdb_df
