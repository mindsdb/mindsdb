from mindsdb.integrations.handlers.scylla_handler import Handler as ScyllaHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response)
import pandas as pd


class CassandraHandler(ScyllaHandler):
    """
    This handler handles connection and execution of the Cassandra statements.
    """

    name = 'cassandra'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    def get_tables(self) -> Response:
        """
        Get the list of tables in the connected Cassandra database.

        :return: List of table names.
        """
        sql = "DESCRIBE TABLES"
        result = self.native_query(sql)
        df = result.data_frame
        table_data = pd.DataFrame(
            {'table_name': df['name'],
             'keyspace_name': df['keyspace_name'],
             'type': df['type']})
        result.data_frame = table_data
        return result
