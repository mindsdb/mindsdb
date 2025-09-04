from typing import List, Optional, Union
import pandas as pd

from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.utilities import log


logger = log.getLogger("mindsdb")


class DataCatalogRetriever:
    """
    This class is responsible for retrieving (data catalog) metadata directly from the data source via the handler.
    """

    def __init__(self, database_name: str, table_names: Optional[List[str]] = None) -> None:
        """
        Initialize the DataCatalogRetriever.

        Args:
            database_name (str): The data source to read/write metadata from.
            table_names (Optional[List[str]]): The list of table names to read or write metadata for. If None, all tables will be read or written.
        """
        from mindsdb.api.executor.controllers.session_controller import (
            SessionController,
        )

        session = SessionController()

        self.database_name = database_name
        self.data_handler: Union[MetaDatabaseHandler, MetaAPIHandler] = session.integration_controller.get_data_handler(
            database_name
        )
        integration = session.integration_controller.get(database_name)
        self.integration_id = integration["id"]
        self.integration_engine = integration["engine"]
        # TODO: Handle situations where a schema is provided along with the database name, e.g., 'schema.table'.
        # TODO: Handle situations where a file path is provided with integrations like S3, e.g., 'dir/file.csv'.
        self.table_names = table_names

    def retrieve_tables(self) -> pd.DataFrame:
        """
        Retrieve the table metadata from the handler.
        """
        logger.info(f"Retrieving {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_tables(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            logger.error(f"Failed to retrieve tables for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            logger.error(f"No tables found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame

    def retrieve_columns(self) -> pd.DataFrame:
        """
        Retrieve the column metadata from the handler.
        """
        logger.info(f"Retrieving columns for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_columns(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            logger.error(f"Failed to retrieve columns for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            logger.error(f"No columns found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame

    def retrieve_column_statistics(self) -> pd.DataFrame:
        """
        Retrieve the column statistics from the handler.
        """
        logger.info(f"Retrieving column statistics for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_column_statistics(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            logger.error(f"Failed to retrieve column statistics for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            logger.error(f"No column statistics found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame

    def retrieve_primary_keys(self) -> pd.DataFrame:
        """
        Retrieve the primary keys from the handler.
        """
        logger.info(f"Retrieving primary keys for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_primary_keys(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            logger.error(f"Failed to retrieve primary keys for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            logger.error(f"No primary keys found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame

    def retrieve_foreign_keys(self) -> pd.DataFrame:
        """
        Retrieve the foreign keys from the handler.
        """
        logger.info(f"Retrieving foreign keys for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}")
        response = self.data_handler.meta_get_foreign_keys(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            logger.error(f"Failed to retrieve foreign keys for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            logger.error(f"No foreign keys found for {self.database_name} in the data source.")
            return pd.DataFrame()
        
        return response.data_frame
    
    def retrieve_handler_info(self) -> str:
        """
        Retrieve the handler info from the handler.
        """
        logger.info(f"Retrieving handler info for {self.database_name}")
        return self.data_handler.meta_get_handler_info()