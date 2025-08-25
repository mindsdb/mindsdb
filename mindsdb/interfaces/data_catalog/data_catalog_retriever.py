from typing import List
import pandas as pd

from mindsdb.interfaces.data_catalog.base_data_catalog import BaseDataCatalog
from mindsdb.integrations.libs.response import RESPONSE_TYPE


class DataCatalogRetriever(BaseDataCatalog):
    """
    This class is responsible for retrieving (data catalog) metadata directly from the data source via the handler.
    This is different from the DataCatalogReader, which relies on the fact that the metadata is already stored in the database.
    """

    def retrieve_table_metadata(self) -> pd.DataFrame:
        """
        Retrieve the table metadata from the handler.
        """
        self.logger.info(f"Retrieving {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_tables(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to retrieve tables for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No tables found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame
    
    def retrieve_column_metadata(self) -> pd.DataFrame:
        """
        Retrieve the column metadata from the handler.
        """
        self.logger.info(f"Retrieving columns for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_columns(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to retrieve columns for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No columns found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame
