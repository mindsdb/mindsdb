import pandas as pd
import numpy as np

from questdb.ingress import Sender

from mindsdb.integrations.handlers.postgres_handler import Handler as PostgresHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class QuestDBHandler(PostgresHandler):
    """
    This handler handles connection and execution of the QuestDB statements.
    TODO: check the dialect for questdb
    """
    name = 'questdb'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

    def get_tables(self):
        """
        List all tabels in QuestDB
        """
        query = "SHOW TABLES"
        response = super().native_query(query)
        return response

    def get_columns(self, table_name):
        """
        List information about the table
        """
        query = f"SELECT * FROM tables() WHERE name='{table_name}';"
        response = super().native_query(query)
        return response

    def qdb_connect(self):
        args = self.connection_args
        conf = f"http::addr={args['host']}:9000;username={args['user']};password={args['password']};"
        return Sender.from_conf(conf)

    def insert(self, table_name: str, df: pd.DataFrame):

        with self.qdb_connect() as sender:
            try:
                # find datetime column
                at_col = None
                for col, dtype in df.dtypes.items():
                    if np.issubdtype(dtype, np.datetime64):
                        at_col = col
                if at_col is None:
                    raise Exception(f'Unable to find datetime column: {df.dtypes}')

                sender.dataframe(df, table_name=table_name, at=at_col)
                response = Response(RESPONSE_TYPE.OK)

            except Exception as e:
                logger.error(f'Error running insert to {table_name} on {self.database}, {e}!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )

        return response
