from mindsdb.interfaces.data_catalog.base_data_catalog import BaseDataCatalog
from mindsdb.interfaces.storage import db


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
            self.logger.warning(f"No metadata found for database '{self.database_name}'")
            return f"No metadata found for database '{self.database_name}'"

        metadata_str = "Data Catalog: \n"
        if hasattr(self.data_handler, "meta_get_handler_info"):
            info = self.data_handler.meta_get_handler_info()
            if info:
                metadata_str += info + "\n\n"

        for table in tables:
            metadata_str += table.as_string() + "\n\n"
        return metadata_str

    def read_metadata_as_records(self) -> list:
        """
        Read the metadata from the data catalog and return it as a list of database records.
        """
        tables = self._read_metadata()
        if not tables:
            self.logger.warning(f"No metadata found for database '{self.database_name}'")
            return []
        return tables

    def get_handler_info(self) -> str:
        """
        Get the handler info for the database.
        """
        return self.data_handler.meta_get_handler_info()

    def _read_metadata(self) -> list:
        """
        Read the metadata from the data catalog and return it in a structured format.
        """
        if not self.is_data_catalog_supported():
            return f"Data catalog is not supported for database '{self.database_name}'."

        query = db.session.query(db.MetaTables).filter_by(integration_id=self.integration_id)
        if self.table_names:
            cleaned_table_names = [name.strip("`").split(".")[-1] for name in self.table_names]
            query = query.filter(db.MetaTables.name.in_(cleaned_table_names))
        tables = query.all()
        return tables
