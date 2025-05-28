from typing import List, Optional, Union

from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.base import MetaDatabaseHandler


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
        self.integration_id = session.integration_controller.get(database_name)["id"]
        self.table_names = table_names
