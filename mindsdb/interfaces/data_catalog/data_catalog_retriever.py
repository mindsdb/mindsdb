import ast
import pandas as pd
from typing import List, Optional, Union

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
            database_name (str): The data source to retrieve metadata from.
            table_names (Optional[List[str]]): The list of table names to retrieve metadata for. If None, all tables will be read.
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

    def retrieve_metadata_as_string(self) -> str:
        """
        Retrieve the metadata as a formatted string.
        """
        tables_df = self.retrieve_tables()
        if tables_df.empty:
            return f"No metadata found for database '{self.database_name}'"

        metadata_str = "Data Catalog: \n"
        handler_info = self.retrieve_handler_info()
        if handler_info:
            metadata_str += handler_info + "\n\n"

        columns_df = self.retrieve_columns()
        column_stats_df = self.retrieve_column_statistics()
        primary_keys_df = self.retrieve_primary_keys()
        foreign_keys_df = self.retrieve_foreign_keys()

        metadata_str += self._construct_metadata_string_for_tables(
            tables_df,
            columns_df,
            column_stats_df,
            primary_keys_df,
            foreign_keys_df,
        )
        return metadata_str

    def _construct_metadata_string_for_tables(
        self,
        tables_df: pd.DataFrame,
        columns_df: pd.DataFrame,
        column_stats_df: pd.DataFrame,
        primary_keys_df: pd.DataFrame,
        foreign_keys_df: pd.DataFrame,
    ) -> str:
        """
        Construct a formatted string representation of the metadata for the given tables.
        """
        tables_metadata_str = ""

        # Convert all DataFrame column names to uppercase for consistency.
        tables_df.columns = tables_df.columns.str.upper()
        columns_df.columns = columns_df.columns.str.upper()
        column_stats_df.columns = column_stats_df.columns.str.upper()
        primary_keys_df.columns = primary_keys_df.columns.str.upper()
        foreign_keys_df.columns = foreign_keys_df.columns.str.upper()

        for _, table_row in tables_df.iterrows():
            table_columns_df = columns_df[columns_df["TABLE_NAME"] == table_row["TABLE_NAME"]]

            table_column_stats_df = pd.DataFrame()
            table_primary_keys_df = pd.DataFrame()
            table_foreign_keys_df = pd.DataFrame()
            # If no columns are found for the table,
            # looking for column stats, primary keys, and foreign keys is redundant.
            if not table_columns_df.empty:
                if not column_stats_df.empty:
                    table_column_stats_df = column_stats_df[column_stats_df["TABLE_NAME"] == table_row["TABLE_NAME"]]
                if not primary_keys_df.empty:
                    table_primary_keys_df = primary_keys_df[primary_keys_df["TABLE_NAME"] == table_row["TABLE_NAME"]]
                if not foreign_keys_df.empty:
                    table_foreign_keys_df = foreign_keys_df[foreign_keys_df["TABLE_NAME"] == table_row["TABLE_NAME"]]

            tables_metadata_str += self._construct_metadata_string_for_table(
                table_row,
                table_columns_df,
                table_column_stats_df,
                table_primary_keys_df,
                table_foreign_keys_df,
            )
        return tables_metadata_str

    def _construct_metadata_string_for_table(
        self,
        table_row: pd.Series,
        columns_df: pd.DataFrame,
        column_stats_df: pd.DataFrame,
        primary_keys_df: pd.DataFrame,
        foreign_keys_df: pd.DataFrame,
    ) -> str:
        """
        Construct a formatted string representation of the metadata for a single table.
        """
        table_metadata_str = f"`{self.database_name}`.`{table_row['TABLE_NAME']}`"

        if "TABLE_TYPE" in table_row and pd.notna(table_row["TABLE_TYPE"]):
            table_metadata_str += f" ({table_row['TABLE_TYPE']})"
        if "TABLE_DESCRIPTION" in table_row and pd.notna(table_row["TABLE_DESCRIPTION"]):
            table_metadata_str += f": {table_row['TABLE_DESCRIPTION']}"
        if "TABLE_SCHEMA" in table_row and pd.notna(table_row["TABLE_SCHEMA"]):
            table_metadata_str += f"\nSchema: {table_row['TABLE_SCHEMA']}"
        if "ROW_COUNT" in table_row and pd.notna(table_row["ROW_COUNT"]) and table_row["ROW_COUNT"] > 0:
            table_metadata_str += f"\nEstimated Row Count: {int(table_row['ROW_COUNT'])}"

        if not primary_keys_df.empty:
            table_metadata_str += self._construct_metadata_string_for_primary_keys(primary_keys_df)

        if not columns_df.empty:
            table_metadata_str += self._construct_metadata_string_for_columns(columns_df, column_stats_df)

        if not foreign_keys_df.empty:
            table_metadata_str += self._construct_metadata_string_for_foreign_keys(
                foreign_keys_df,
                table_row["TABLE_NAME"],
            )

        return table_metadata_str

    def _construct_metadata_string_for_primary_keys(
        self,
        primary_keys_df: pd.DataFrame,
    ) -> str:
        """
        Construct a formatted string representation of the primary keys for a single table.
        """
        primary_keys_str = "\nPrimary Keys (in defined order): "
        if "ORDINAL_POSITION" in primary_keys_df.columns:
            primary_keys_df.sort_values(by="ORDINAL_POSITION", inplace=True)
        primary_keys = primary_keys_df["COLUMN_NAME"].tolist()
        primary_keys_str += ", ".join([f"`{pk}`" for pk in primary_keys])
        return primary_keys_str

    def _construct_metadata_string_for_columns(
        self,
        columns_df: pd.DataFrame,
        column_stats_df: pd.DataFrame,
    ) -> str:
        """
        Construct a formatted string representation of the columns for a single table.
        """
        columns_str = "\n\nColumns:\n"
        for _, column_row in columns_df.iterrows():
            # Ideally, there should be only one stats row per column.
            stats_row = column_stats_df[column_stats_df["COLUMN_NAME"] == column_row["COLUMN_NAME"]]
            if len(stats_row) == 0:
                stats_row = pd.Series()
            else:
                stats_row = stats_row.iloc[0]
            columns_str += self._construct_metadata_string_for_column(
                column_row,
                stats_row,
            )
        return columns_str

    def _construct_metadata_string_for_column(
        self,
        column_row: pd.Series,
        column_stats_row: pd.Series,
    ) -> str:
        """
        Construct a formatted string representation of a single column.
        """
        pad = " " * 4
        column_str = f"{column_row['COLUMN_NAME']} ({column_row['DATA_TYPE']}):"

        if "COLUMN_DESCRIPTION" in column_row and pd.notna(column_row["COLUMN_DESCRIPTION"]):
            column_str += f": {column_row['COLUMN_DESCRIPTION']}"
        if "IS_NULLABLE" in column_row and pd.notna(column_row["IS_NULLABLE"]):
            column_str += f"\n{pad}- Nullable: {column_row['IS_NULLABLE']}"
        if "COLUMN_DEFAULT" in column_row and pd.notna(column_row["COLUMN_DEFAULT"]):
            column_str += f"\n{pad}- Default Value: {column_row['COLUMN_DEFAULT']}"

        if not column_stats_row.empty:
            column_str += self._construct_metadata_string_for_column_statistics(column_stats_row, pad)

        column_str += "\n\n"

        return column_str

    def _construct_metadata_string_for_column_statistics(
        self,
        stats_row: pd.Series,
        pad: str,
    ) -> str:
        """
        Construct a formatted string representation of the column statistics for a single column.
        """
        inner_pad = pad + " " * 4
        inner_inner_pad = inner_pad + " " * 4
        stats_str = f"\n{pad}- Column Statistics:"

        # Most common values is expected to be a list, hence the check for non-null.
        if "MOST_COMMON_VALUES" in stats_row and stats_row["MOST_COMMON_VALUES"]:
            most_common_values = stats_row["MOST_COMMON_VALUES"]
            # Handle case where most_common_values is a string representation of a list or other formats.
            if isinstance(most_common_values, str):
                most_common_values = self._parse_list_from_string(most_common_values)
            elif isinstance(most_common_values, (list, tuple, pd.Series)):
                most_common_values = list(most_common_values)
            else:
                most_common_values = [most_common_values]

            if most_common_values and pd.notna(most_common_values).any():
                stats_str += f"\n{inner_pad}- Top 10 Most Common Values and Frequencies:"

                # Most common frequencies is also expected to be a list.
                most_common_frequencies = []
                if "MOST_COMMON_FREQUENCIES" in stats_row and stats_row["MOST_COMMON_FREQUENCIES"]:
                    most_common_frequencies = stats_row["MOST_COMMON_FREQUENCIES"]
                    if isinstance(most_common_frequencies, str):
                        most_common_frequencies = self._parse_list_from_string(most_common_frequencies)
                    elif isinstance(most_common_frequencies, (list, tuple, pd.Series)):
                        most_common_frequencies = list(most_common_frequencies)
                    else:
                        most_common_frequencies = [most_common_frequencies]

                for i in range(min(10, len(most_common_values))):
                    if (
                        most_common_frequencies
                        and pd.notna(most_common_frequencies).any()
                        and i < len(most_common_frequencies)
                    ):
                        freq = most_common_frequencies[i]
                        try:
                            percent = float(freq) * 100
                            freq_str = f"{percent:.2f}%"
                        except (ValueError, TypeError):
                            freq_str = str(freq)
                    else:
                        freq_str = ""

                    stats_str += f"\n{inner_inner_pad}- {most_common_values[i]}" + (f": {freq_str}" if freq_str else "")
                stats_str += "\n"

        if "NULL_PERCENTAGE" in stats_row and pd.notna(stats_row["NULL_PERCENTAGE"]):
            stats_str += f"\n{inner_pad}- Null Percentage: {stats_row['NULL_PERCENTAGE']}"
        if "DISTINCT_VALUES_COUNT" in stats_row and pd.notna(stats_row["DISTINCT_VALUES_COUNT"]):
            stats_str += f"\n{inner_pad}- No. of Distinct Values: {stats_row['DISTINCT_VALUES_COUNT']}"
        if "MINIMUM_VALUE" in stats_row and pd.notna(stats_row["MINIMUM_VALUE"]):
            stats_str += f"\n{inner_pad}- Minimum Value: {stats_row['MINIMUM_VALUE']}"
        if "MAXIMUM_VALUE" in stats_row and pd.notna(stats_row["MAXIMUM_VALUE"]):
            stats_str += f"\n{inner_pad}- Maximum Value: {stats_row['MAXIMUM_VALUE']}"

        return stats_str

    def _parse_list_from_string(self, list_str: str) -> List[str]:
        """
        Safely parse a string representation of a list into an actual list.
        This is used to handle most common values and frequencies stored as strings.
        """
        # Try to safely parse python-like list strings: "['a','b']" or '["a","b"]'.
        try:
            parsed = ast.literal_eval(list_str)
            if isinstance(parsed, (list, tuple)):
                lst = [x for x in parsed if not pd.isna(x)]
            else:
                # fallback: treat parsed as scalar
                if not pd.isna(parsed):
                    lst = [parsed]
        except (ValueError, SyntaxError):
            # fallback to splitting on comma for simpler string formats
            s = list_str.strip("[]")
            lst = [v.strip() for v in s.split(",") if v.strip()]
        return lst

    def _construct_metadata_string_for_foreign_keys(
        self,
        foreign_keys_df: pd.DataFrame,
        table_name: str,
    ) -> str:
        """
        Construct a formatted string representation of the foreign keys for a single table.
        """
        pad = " " * 4
        foreign_keys_str = "\n\nKey Relationships:"
        for _, fk_row in foreign_keys_df.iterrows():
            # Avoid relationships where the current table is the child table to prevent redundancy.
            if fk_row["CHILD_TABLE_NAME"] == table_name:
                continue
            foreign_keys_str += f"{pad}-{fk_row['CHILD_COLUMN_NAME']} in `{fk_row['CHILD_TABLE_NAME']}` references {fk_row['PARENT_COLUMN_NAME']} in `{fk_row['PARENT_TABLE_NAME']}`\n"
        return foreign_keys_str

    def retrieve_tables(self) -> pd.DataFrame:
        """
        Retrieve the table metadata from the handler.
        """
        self.logger.info(
            f"Retrieving {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}"
        )
        response = self.data_handler.meta_get_tables(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to retrieve tables for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No tables found for {self.database_name} in the data source.")
            return pd.DataFrame()

        return response.data_frame

    def retrieve_columns(self) -> pd.DataFrame:
        """
        Retrieve the column metadata from the handler.
        """
        self.logger.info(
            f"Retrieving columns for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}"
        )
        response = self.data_handler.meta_get_columns(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to retrieve columns for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No columns found for {self.database_name} in the data source.")
            return pd.DataFrame()

        return response.data_frame

    def retrieve_column_statistics(self) -> pd.DataFrame:
        """
        Retrieve the column statistics from the handler.
        """
        self.logger.info(
            f"Retrieving column statistics for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}"
        )
        response = self.data_handler.meta_get_column_statistics(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(
                f"Failed to retrieve column statistics for {self.database_name}: {response.error_message}"
            )
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No column statistics found for {self.database_name} in the data source.")
            return pd.DataFrame()

        return response.data_frame

    def retrieve_primary_keys(self) -> pd.DataFrame:
        """
        Retrieve the primary keys from the handler.
        """
        self.logger.info(
            f"Retrieving primary keys for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}"
        )
        response = self.data_handler.meta_get_primary_keys(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to retrieve primary keys for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No primary keys found for {self.database_name} in the data source.")
            return pd.DataFrame()

        return response.data_frame

    def retrieve_foreign_keys(self) -> pd.DataFrame:
        """
        Retrieve the foreign keys from the handler.
        """
        self.logger.info(
            f"Retrieving foreign keys for {', '.join(self.table_names) if self.table_names else 'all'} tables for {self.database_name}"
        )
        response = self.data_handler.meta_get_foreign_keys(self.table_names)
        if response.resp_type == RESPONSE_TYPE.ERROR:
            self.logger.error(f"Failed to retrieve foreign keys for {self.database_name}: {response.error_message}")
            return pd.DataFrame()
        elif response.resp_type == RESPONSE_TYPE.OK:
            self.logger.error(f"No foreign keys found for {self.database_name} in the data source.")
            return pd.DataFrame()

        return response.data_frame

    def retrieve_handler_info(self) -> str:
        """
        Retrieve the handler info from the handler.
        """
        self.logger.info(f"Retrieving handler info for {self.database_name}")
        return self.data_handler.meta_get_handler_info()
