from mindsdb.integrations.handlers.scylla_handler import Handler as ScyllaHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
import pandas as pd


class CassandraHandler(ScyllaHandler):
    """
    This handler handles connection and execution of the Cassandra statements.
    """

    name = 'cassandra'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.connection_data = kwargs.get('connection_data', {})
        self.keyspace = self.connection_data.get('keyspace')

    def get_tables(self) -> Response:
        """
        Get the list of tables in the connected Cassandra database.

        :return: List of table names.
        """
        tables = self.session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s'" % self.keyspace)
        response = Response(RESPONSE_TYPE.TABLE,
                            pd.DataFrame([dict(table_name=row.table_name) for row in tables]))
        return response
