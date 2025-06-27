from typing import List, Optional, Union

from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.utilities import log


logger = log.getLogger("mindsdb")


class BaseDataCatalog:
    """
    This is the base class for the Data Catalog interface.
    """

    def __init__(self, database_name: str, table_names: Optional[List[str]] = None) -> None:
        """
        Initialize the DataCatalogReader.

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

        self.logger = logger

    def is_data_catalog_supported(self) -> bool:
        """
        Check if the data catalog is supported for the given database.

        Returns:
            bool: True if the data catalog is supported, False otherwise.
        """
        if not isinstance(self.data_handler, (MetaDatabaseHandler, MetaAPIHandler)):
            self.logger.warning(f"Data catalog is not supported for the '{self.integration_engine}' integration'. ")
            return False

        return True
