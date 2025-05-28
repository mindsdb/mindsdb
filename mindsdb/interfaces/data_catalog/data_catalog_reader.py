from typing import List, Optional

from mindsdb.integrations.libs.base import MetaDatabaseHandler
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log


logger = log.getLogger("mindsdb")


class DataCatalogReader:
    """
    This class is responsible for reading the metadata from the data catalog and providing it in a structured format.
    """

    def __init__(
        self, database_name: str, table_names: Optional[List[str]] = None
    ) -> None:
        """
        Initialize the DataCatalogReader.

        Args:
            database_name (str): The data source to read metadata from.
            table_names (Optional[List[str]]): The list of table names to read metadata for. If None, all tables will be read.
        """
        from mindsdb.api.executor.controllers.session_controller import (
            SessionController,
        )

        session = SessionController()

        self.database_name = database_name
        self.data_handler: MetaDatabaseHandler = (
            session.integration_controller.get_data_handler(database_name)
        )
        self.integration_id = session.integration_controller.get(database_name)["id"]
        self.table_names = table_names

    def read_metadata_as_string(self) -> str:
        """
        Read the metadata from the data catalog and return it as a string.
        """
        tables = self._read_metadata()

        metadata_str = "Data Catalog: \n"
        for table in tables:
            metadata_str += table.as_string() + "\n\n"

        return metadata_str

    def _read_metadata(self) -> None:
        """
        Read the metadata from the data catalog and return it in a structured format.
        """
        query = db.session.query(db.MetaTables).filter_by(
            integration_id=self.integration_id
        )
        if self.table_names:
            query = query.filter(db.MetaTables.name.in_(self.table_names))

        tables = query.all()
        return tables
