from mindsdb.integrations.handlers.scylla_handler import Handler as ScyllaHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response)


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
        df = df.rename(columns={'name': 'table_name'})
        result.data_frame = df
        return result
