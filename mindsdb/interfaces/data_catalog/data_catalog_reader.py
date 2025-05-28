from typing import List, Optional

from mindsdb.interfaces.data_catalog.base_data_catalog import BaseDataCatalog
from mindsdb.interfaces.storage import db
from mindsdb.utilities import log


logger = log.getLogger("mindsdb")


class DataCatalogReader(BaseDataCatalog):
    """
    This class is responsible for reading the metadata from the data catalog and providing it in a structured format.
    """

    def read_metadata_as_string(self) -> str:
        """
        Read the metadata from the data catalog and return it as a string.
        """
        tables = self._read_metadata()

        if not tables:
            logger.warning(f"No metadata found for database '{self.database_name}'")
            return f"No metadata found for database '{self.database_name}'"

        metadata_str = "Data Catalog: \n"
        for table in tables:
            metadata_str += table.as_string() + "\n\n"

        return metadata_str

    def _read_metadata(self) -> None:
        """
        Read the metadata from the data catalog and return it in a structured format.
        """
        query = db.session.query(db.MetaTables).filter_by(integration_id=self.integration_id)
        if self.table_names:
            query = query.filter(db.MetaTables.name.in_(self.table_names))

        tables = query.all()
        return tables
