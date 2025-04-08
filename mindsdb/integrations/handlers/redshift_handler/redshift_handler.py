import os
import numpy as np
import pandas as pd

from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler

logger = log.getLogger(__name__)
os.environ["PGCLIENTENCODING"] = "utf-8"


class RedshiftHandler(PostgresHandler):
    """
    This handler handles connection and execution of the Redshift statements.
    """

    name = 'redshift'

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance.
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name, **kwargs)

    def insert(self, table_name: str, df: pd.DataFrame):
        """
        Handles the execution of INSERT statements.

        Args:
            table_name (str): name of the table to insert the data into.
            df (pd.DataFrame): data to be inserted into the table.
        """
        need_to_close = not self.is_connected

        connection = self.connect()

        # Replace NaN values with None
        df = df.replace({np.nan: None})

        # Build the query to insert the data
        columns = ', '.join([f'"{col}"' if ' ' in col else col for col in df.columns])
        values = ', '.join(['%s' for _ in range(len(df.columns))])
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({values})'

        with connection.cursor() as cur:
            try:
                cur.executemany(query, df.values.tolist())
                response = Response(RESPONSE_TYPE.OK, affected_rows=cur.rowcount)

                connection.commit()
            except Exception as e:
                logger.error(f"Error inserting data into {table_name}, {e}!")
                connection.rollback()
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_code=0,
                    error_message=str(e)
                )

        if need_to_close:
            self.disconnect()

        return response
