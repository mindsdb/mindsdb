from typing import List, Optional

import pandas as pd

from mindsdb.interfaces.storage import db
from mindsdb.utilities import log


logger = log.getLogger("mindsdb")


class DataCatalogLoader:
    """
    This class is responsible for loading the metadata from a data source to the data catalog.
    """

    def __init__(self, database_name: str, table_names: Optional[List[str]] = None):
        """
        Initialize the DataCatalogLoader.

        Args:
            database_name (str): The data source to load metadata from.
            table_names (Optional[List[str]]): The list of table names to load metadata for. If None, all tables will be loaded.
        """
        from mindsdb.api.executor.controllers.session_controller import SessionController
        session = SessionController()
        
        self.data_handler = session.integration_controller.get_data_handler(database_name)
        self.integration_id = session.integration_controller.get(database_name)['id']
        self.table_names = table_names

    def load_metadata(self):
        tables = self._load_table_metadata()
        
        columns = self._load_column_metadata(tables)
        
        self._load_column_statistics(tables, columns)
    
    def _load_table_metadata(self):
        """
        Load the metadata for the specified tables.
        """
        logger.debug(f"Loading table metadata for {self.table_names}")
        response = self.data_handler.get_table_metadata(self.table_names)
        df = response.data_frame

        return self._add_table_metadata(df)
    
    def _add_table_metadata(self, df: pd.DataFrame):
        """
        Add the metadata for the specified tables to the database.
        """
        tables = []
        for row in df.to_dict(orient='records'):
            record =db.MetaTables(
                integration_id=self.integration_id,
                name=row.get('table_name') or row.get('name'),
                schema=row.get('schema'),
                description=row.get('description'),
                row_count=row.get('row_count')
            )
            tables.append(record)

        db.session.add_all(tables)
        db.session.commit()
        return tables

    def _load_column_metadata(self, tables: db.MetaTables):
        """
        Load the metadata for the specified columns.
        """
        logger.debug(f"Loading column metadata for {self.table_names}")
        response = self.data_handler.get_column_metadata(self.table_names)
        df = response.data_frame

        return self._add_column_metadata(df, tables)
    
    def _add_column_metadata(self, df: pd.DataFrame, tables: db.MetaTables):
        """
        Add the metadata for the specified columns to the database.
        """
        columns = []
        for row in df.to_dict(orient='records'):
            record =db.MetaColumns(
                table_id=next(
                    (table.id for table in tables if table.name == row.get('table_name'))
                ),
                name=row.get('column_name'),
                data_type=row.get('data_type'),
                description=row.get('description'),
                is_nullable=row.get('is_nullable')
            )
            columns.append(record)
            
        db.session.add_all(columns)
        db.session.commit()
        return columns

    def _load_column_statistics(self, tables: db.MetaTables, columns: db.MetaColumns):
        """
        Load the statistics for the specified columns.
        """
        logger.debug(f"Loading column statistics for {self.table_names}")
        response = self.data_handler.get_column_statistics(self.table_names)
        df = response.data_frame

        return self._add_column_statistics(df, tables, columns)

    def _add_column_statistics(self, df: pd.DataFrame, tables: db.MetaTables, columns: db.MetaColumns):
        """
        Add the statistics for the specified columns to the database.
        """
        column_statistics = []
        for row in df.to_dict(orient='records'):
            table_id = next(
                (table.id for table in tables if table.name == row.get('table_name'))
            )
            column_id = next(
                (column.id for column in columns if column.name == row.get('column_name') and column.table_id == table_id)
            )
            
            record = db.MetaColumnStatistics(
                column_id=column_id,
                most_common_values=row.get('most_common_values'),
                most_common_frequencies=str(row.get('most_common_frequencies')) if row.get('most_common_frequencies') else None,
                null_percentage=row.get('null_percentage'),
                distinct_values_count=row.get('distinct_values_count')
            )
            column_statistics.append(record)

        db.session.add_all(column_statistics)
        db.session.commit()
