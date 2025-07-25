from typing import List, Union
import pandas as pd
import json
import datetime
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.interfaces.data_catalog.base_data_catalog import BaseDataCatalog
from mindsdb.interfaces.storage import db


class DataCatalogLoader(BaseDataCatalog):
    """
    This class is responsible for loading the metadata from a data source (via the handler) and storing it in the data catalog.
    """

    def load_metadata(self) -> None:
        """
        Load the metadata from the handler and store it in the database.
        """
        if not self.is_data_catalog_supported():
            return

        loaded_table_names = self._get_loaded_table_names()

        tables = self._load_table_metadata(loaded_table_names)

        if tables:
            columns = self._load_column_metadata(tables)

            self._load_column_statistics(tables, columns)

            self._load_primary_keys(tables, columns)

            self._load_foreign_keys(tables, columns)

        self.logger.info(f"Metadata loading completed for {self.database_name}.")

    def _get_loaded_table_names(self) -> List[str]:
        """
        Retrieve the names of tables that are already present in the data catalog for the current integration.
        If table_names are provided, only those tables will be checked.

        Returns:
            List[str]: Names of tables already loaded in the data catalog.
        """
        query = db.session.query(db.MetaTables).filter_by(integration_id=self.integration_id)
        if self.table_names:
            query = query.filter(db.MetaTables.name.in_(self.table_names))

        tables = query.all()
        table_names = [table.name for table in tables]

        if table_names:
            self.logger.info(f"Tables already loaded in the data catalog: {', '.join(table_names)}.")

        return table_names

    def _load_table_metadata(self, loaded_table_names: List[str] = None) -> List[Union[db.MetaTables, None]]:
        """
        Load the table metadata from the handler.
        """
        self.logger.info(f"Loading tables for {self.database_name}")
        response = self.data_handler.meta_get_tables(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to load tables for {self.database_name}: {response.error_message}")
            return []
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No tables found for {self.database_name}.")
            return []

        df = response.data_frame
        if df.empty:
            self.logger.info(f"No tables to add for {self.database_name}.")
            return []

        df.columns = df.columns.str.lower()

        # Filter out tables that are already loaded in the data catalog
        if loaded_table_names:
            df = df[~df["table_name"].isin(loaded_table_names)]

        if df.empty:
            self.logger.info(f"No new tables to load for {self.database_name}.")
            return []

        tables = self._add_table_metadata(df)
        self.logger.info(f"Tables loaded for {self.database_name}.")
        return tables

    def _add_table_metadata(self, df: pd.DataFrame) -> List[db.MetaTables]:
        """
        Add the table metadata to the database.
        """
        tables = []
        try:
            for row in df.to_dict(orient="records"):
                # Convert the distinct_values_count to an integer if it is not NaN, otherwise set it to None.
                val = row.get("row_count")
                row_count = int(val) if pd.notna(val) else None

                record = db.MetaTables(
                    integration_id=self.integration_id,
                    name=row.get("table_name") or row.get("name"),
                    schema=row.get("table_schema"),
                    description=row.get("table_description"),
                    type=row.get("table_type"),
                    row_count=row_count,
                )
                tables.append(record)

            db.session.add_all(tables)
            db.session.commit()
        except Exception as e:
            self.logger.error(f"Failed to add tables: {e}")
            db.session.rollback()
            raise
        return tables

    def _load_column_metadata(self, tables: db.MetaTables) -> List[db.MetaColumns]:
        """
        Load the column metadata from the handler.
        """
        self.logger.info(f"Loading columns for {self.database_name}")
        response = self.data_handler.meta_get_columns([table.name for table in tables])
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to load columns for {self.database_name}: {response.error_message}")
            return []
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No columns found for {self.database_name}.")
            return []

        df = response.data_frame
        if df.empty:
            self.logger.info(f"No columns to load for {self.database_name}.")
            return []

        df.columns = df.columns.str.lower()
        columns = self._add_column_metadata(df, tables)
        self.logger.info(f"Columns loaded for {self.database_name}.")
        return columns

    def _add_column_metadata(self, df: pd.DataFrame, tables: db.MetaTables) -> List[db.MetaColumns]:
        """
        Add the column metadata to the database.
        """
        columns = []
        try:
            for row in df.to_dict(orient="records"):
                record = db.MetaColumns(
                    table_id=next((table.id for table in tables if table.name == row.get("table_name"))),
                    name=row.get("column_name"),
                    data_type=row.get("data_type"),
                    default_value=row.get("column_default"),
                    description=row.get("description"),
                    is_nullable=row.get("is_nullable"),
                )
                columns.append(record)

            db.session.add_all(columns)
            db.session.commit()
        except Exception as e:
            self.logger.error(f"Failed to add columns: {e}")
            db.session.rollback()
            raise
        return columns

    def _load_column_statistics(self, tables: db.MetaTables, columns: db.MetaColumns) -> None:
        """
        Load the column statistics metadata from the handler.
        """
        self.logger.info(f"Loading column statistics for {self.database_name}")
        response = self.data_handler.meta_get_column_statistics([table.name for table in tables])
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to load column statistics for {self.database_name}: {response.error_message}")
            return
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No column statistics found for {self.database_name}.")
            return

        df = response.data_frame
        if df.empty:
            self.logger.info(f"No column statistics to load for {self.database_name}.")
            return

        df.columns = df.columns.str.lower()
        self._add_column_statistics(df, tables, columns)
        self.logger.info(f"Column statistics loaded for {self.database_name}.")

    def _add_column_statistics(self, df: pd.DataFrame, tables: db.MetaTables, columns: db.MetaColumns) -> None:
        """
        Add the column statistics metadata to the database.
        """
        column_statistics = []
        try:
            for row in df.to_dict(orient="records"):
                table_id = next((table.id for table in tables if table.name == row.get("table_name")))
                column_id = next(
                    (
                        column.id
                        for column in columns
                        if column.name == row.get("column_name") and column.table_id == table_id
                    )
                )

                # Convert the distinct_values_count to an integer if it is not NaN, otherwise set it to None.
                val = row.get("distinct_values_count")
                distinct_values_count = int(val) if pd.notna(val) else None
                min_val = row.get("minimum_value")
                max_val = row.get("maximum_value")

                # Convert the most_common_frequencies to a list of strings.
                most_common_frequencies = [str(val) for val in row.get("most_common_frequencies") or []]

                record = db.MetaColumnStatistics(
                    column_id=column_id,
                    most_common_values=row.get("most_common_values"),
                    most_common_frequencies=most_common_frequencies,
                    null_percentage=row.get("null_percentage"),
                    distinct_values_count=distinct_values_count,
                    minimum_value=self.to_str(min_val),
                    maximum_value=self.to_str(max_val),
                )
                column_statistics.append(record)

            db.session.add_all(column_statistics)
            db.session.commit()
        except Exception as e:
            self.logger.error(f"Failed to add column statistics: {e}")
            db.session.rollback()
            raise

    def _load_primary_keys(self, tables: db.MetaTables, columns: db.MetaColumns) -> None:
        """
        Load the primary keys metadata from the handler.
        """
        self.logger.info(f"Loading primary keys for {self.database_name}")
        response = self.data_handler.meta_get_primary_keys([table.name for table in tables])
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to load primary keys for {self.database_name}: {response.error_message}")
            return
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No primary keys found for {self.database_name}.")
            return

        df = response.data_frame
        if df.empty:
            self.logger.info(f"No primary keys to load for {self.database_name}.")
            return

        df.columns = df.columns.str.lower()
        self._add_primary_keys(df, tables, columns)
        self.logger.info(f"Primary keys loaded for {self.database_name}.")

    def _add_primary_keys(self, df: pd.DataFrame, tables: db.MetaTables, columns: db.MetaColumns) -> None:
        """
        Add the primary keys metadata to the database.
        """
        primary_keys = []
        try:
            for row in df.to_dict(orient="records"):
                table_id = next((table.id for table in tables if table.name == row.get("table_name")))
                column_id = next(
                    (
                        column.id
                        for column in columns
                        if column.name == row.get("column_name") and column.table_id == table_id
                    )
                )

                record = db.MetaPrimaryKeys(
                    table_id=table_id,
                    column_id=column_id,
                    constraint_name=row.get("constraint_name"),
                )
                primary_keys.append(record)

            db.session.add_all(primary_keys)
            db.session.commit()
        except Exception as e:
            self.logger.error(f"Failed to add primary keys: {e}")
            db.session.rollback()
            raise

    def _load_foreign_keys(self, tables: db.MetaTables, columns: db.MetaColumns) -> None:
        """
        Load the foreign keys metadata from the handler.
        """
        self.logger.info(f"Loading foreign keys for {self.database_name}")
        response = self.data_handler.meta_get_foreign_keys([table.name for table in tables])
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to foreign keys for {self.database_name}: {response.error_message}")
            return
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No foreign keys found for {self.database_name}.")
            return

        df = response.data_frame
        if df.empty:
            self.logger.info(f"No foreign keys to load for {self.database_name}.")
            return

        df.columns = df.columns.str.lower()
        self._add_foreign_keys(df, tables, columns)
        self.logger.info(f"Foreign keys loaded for {self.database_name}.")

    def _add_foreign_keys(self, df: pd.DataFrame, tables: db.MetaTables, columns: db.MetaColumns) -> None:
        """
        Add the foreign keys metadata to the database.
        """
        foreign_keys = []
        try:
            for row in df.to_dict(orient="records"):
                try:
                    parent_table_id = next((table.id for table in tables if table.name == row.get("parent_table_name")))
                    parent_column_id = next(
                        (
                            column.id
                            for column in columns
                            if column.name == row.get("parent_column_name") and column.table_id == parent_table_id
                        )
                    )
                    child_table_id = next((table.id for table in tables if table.name == row.get("child_table_name")))
                    child_column_id = next(
                        (
                            column.id
                            for column in columns
                            if column.name == row.get("child_column_name") and column.table_id == child_table_id
                        )
                    )
                except StopIteration:
                    self.logger.warning(
                        f"The foreign key relationship for {row.get('parent_table_name')} -> {row.get('child_table_name')} "
                        f"could not be established. One or more tables or columns may not exist in the metadata."
                    )
                    continue

                record = db.MetaForeignKeys(
                    parent_table_id=parent_table_id,
                    parent_column_id=parent_column_id,
                    child_table_id=child_table_id,
                    child_column_id=child_column_id,
                    constraint_name=row.get("constraint_name"),
                )
                foreign_keys.append(record)

            db.session.add_all(foreign_keys)
            db.session.commit()
        except Exception as e:
            self.logger.error(f"Failed to add foreign keys: {e}")
            db.session.rollback()
            raise

    def unload_metadata(self) -> None:
        """
        Remove the metadata for the specified database from the data catalog.
        """
        if not self.is_data_catalog_supported():
            return

        meta_tables = db.session.query(db.MetaTables).filter_by(integration_id=self.integration_id).all()

        if not meta_tables:
            self.logger.info(f"No metadata found for {self.database_name}. Nothing to remove.")
            return

        for table in meta_tables:
            db.session.query(db.MetaPrimaryKeys).filter_by(table_id=table.id).delete()
            db.session.query(db.MetaForeignKeys).filter(
                (db.MetaForeignKeys.parent_table_id == table.id) | (db.MetaForeignKeys.child_table_id == table.id)
            ).delete()
            meta_columns = db.session.query(db.MetaColumns).filter_by(table_id=table.id).all()
            for col in meta_columns:
                db.session.query(db.MetaColumnStatistics).filter_by(column_id=col.id).delete()
                db.session.delete(col)

            db.session.delete(table)
        db.session.commit()
        self.logger.info(f"Metadata for {self.database_name} removed successfully.")

    def to_str(self, val) -> str:
        """
        Convert a value to a string.
        """
        if val is None:
            return None
        if isinstance(val, (datetime.datetime, datetime.date)):
            return val.isoformat()
        if isinstance(val, (list, dict, set, tuple)):
            return json.dumps(val, default=str)
        return str(val)
