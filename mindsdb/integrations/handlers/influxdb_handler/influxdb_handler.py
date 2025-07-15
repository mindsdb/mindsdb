from influxdb_client_3 import InfluxDBClient3
from mindsdb.integrations.handlers.influxdb_handler.influxdb_tables import InfluxDBTables
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql


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

        self.connection = InfluxDBClient3(host=self.connection_data['influxdb_url'], token=self.connection_data['influxdb_token'], org=self.connection_data.get('org'))

        self.is_connected = True

        return self.connection

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.
        Returns
        -------
        StatusResponse
            Status confirmation
        """
        response = StatusResponse(False)

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
        ast = parse_sql(query)
        return self.query(ast)

    def call_influxdb_tables(self, query):
        """Pulls all the records from the given  InfluxDB table and returns it select()

        Returns
        -------
        pd.DataFrame of all the records of the particular InfluxDB
        """
        influx_connection = self.connect()
        if query is None:
            query = 'SELECT * FROM ' + f"{self.connection_data['influxdb_table_name']}"

        table = influx_connection.query(query=query, database=self.connection_data['influxdb_db_name'], language='sql')
        return table.to_pandas()
