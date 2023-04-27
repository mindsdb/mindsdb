"""
    Wrapper around MindsDB's executor and integration controller following the implementation of the original
    langchain.sql_database.SQLDatabase class to partly replicate its behavior.
"""
import warnings
from typing import Any, Iterable, List, Optional
from langchain.sql_database import SQLDatabase


def _format_index(index: dict) -> str:
    return (
        f'Name: {index["name"]}, Unique: {index["unique"]},'
        f' Columns: {str(index["column_names"])}'
    )


class MindsDBSQL(SQLDatabase):
    """ Class has to be named to replace LangChain's one, as it does a class name check with Pydantic."""
    def __init__(
        self,
        engine,
        schema: Optional[str] = None,
        metadata: Optional = None,
        ignore_tables: Optional[List[str]] = None,  # TODO: use this
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        view_support: Optional[bool] = True,  # TODO: use this
    ):
        # Some args above are not used in this class, but are kept for compatibility
        self._engine = engine   # executor instance
        self._schema = schema  # unused (TODO check if it's better to pass integration controller here instead)
        self._metadata = metadata  # instance of the IntegrationController through which metadata can be obtained
        self._all_tables = set(include_tables)  # TODO: set according to integration controller
        self._sample_rows_in_table_info = int(sample_rows_in_table_info)

        # ###### TODO: temporal ref, delete later ########
        # implement additional tool with integrations_controller to get: table.columns, table.data_types, table.rows
        # args['integrations'].get_handler('files').get_tables().data_frame  # returns DF with TABLE_NAME, TABLE_ROWS, TABLE_TYPE  # noqa
        # args['integrations'].get_handler('files').get_columns('diamonds').data_frame  # returns DF with Field, Type
        # ## end TODO: temporal ref, delete later ########

    # TODO: double check and remove if superflous
    # @classmethod
    # def from_uri(cls, database_uri: str, engine_args: Optional[dict] = None, **kwargs: Any):
    #     pass

    @property
    def dialect(self) -> str:
        return 'mindsdb'

    def get_usable_table_names(self) -> Iterable[str]:
        controller = self._metadata
        usable_tables = []
        for integration in controller.get_handlers():  # TODO: check that get_handlers exist!
            int_df = controller.get_handler(integration).get_tables().data_frame  # also contains TABLE_ROWS, TABLE_TYPE
            usable_tables.extend([f'{integration}.{t}' for t in int_df['TABLE_NAME'].to_list()])
        return self._usable_tables

    def get_table_names(self) -> Iterable[str]:
        warnings.warn("This method is deprecated - please use `get_usable_table_names`.")
        return self.get_usable_table_names()

    @property
    def table_info(self) -> str:
        """Information about all tables in the database."""
        return self.get_table_info()

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        """ Get information about specified tables.
        Follows best practices as specified in: Rajkumar et al, 2022 (https://arxiv.org/abs/2204.00498)
        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as demonstrated in the paper.
        """
        all_table_names = self.get_usable_table_names()
        if table_names is not None:
            missing_tables = set(table_names).difference(all_table_names)
            if missing_tables:
                raise ValueError(f"table_names {missing_tables} not found in database")
            all_table_names = table_names

        tables = []
        for table in all_table_names:
            table_info = self._get_single_table_info(table)
            if self._sample_rows_in_table_info:
                table_info += "\n\n/*"
                table_info += f"\n{self._get_sample_rows(table)}\n"
                table_info += "*/"
            tables.append(table_info)

        final_str = "\n\n".join(all_table_names)
        return final_str

    def _get_single_table_info(self, table_str: str) -> str:
        controller = self._metadata
        integration, table_name = table_str.split('.')

        tbl_name, n_rows, tbl_type = controller.get_handler(integration).get_tables().data_frame.iloc[0].to_list()
        cols_df = controller.get_handler(integration).get_columns(table_name).data_frame
        fields = cols_df['Field'].to_list()
        dtypes = cols_df['Types'].to_list()

        # get basic info
        info = f'Table named `{tbl_name}, type `{tbl_type}`, row count: {n_rows}.\n'

        # TODO: add sample rows to tool!
        info += f'Here is a sample with {self._sample_rows_in_table_info} rows:\n'

        # TODO: implement
        # add sample rows
        # sample_rows_df = self._get_sample_rows()
        info += "\t".join([f'`{col_name}` (data type: {col_dtype})' for col_name, col_dtype in zip(fields, dtypes)])

        return info

    # TODO: ensure _get_table_indexes() is not needed, implement otherwise

    def _get_sample_rows(self, table: str) -> str:
        command = f"select * from {table} limit {self._sample_rows_in_table_info};"

        # TODO: replace with some other mechanism! buggy right now
        columns_str = ""  # "\t".join([col.name for col in table])

        try:
            result = self._engine.execute_command(command)
            sample_rows = result.data
            sample_rows = list(
                map(lambda ls: [str(i)[:100] for i in ls], sample_rows)
            )
            # save the sample rows in string format
            sample_rows_str = "\n".join(["\t".join(row) for row in sample_rows])

        except Exception:
            sample_rows_str = ""

        return (
            f"{self._sample_rows_in_table_info} rows from {table} table:\n"
            f"{columns_str}\n"
            f"{sample_rows_str}"
        )

    def run(self, command: str, fetch: str = "all") -> str:
        """Execute a SQL command and return a string representing the results.
        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """
        result = self._engine.execute_command(command)
        if fetch == "all":
            result = result.fetchall()  # TODO fix
        elif fetch == "one":
            result = result.fetchone()[0]  # TODO fix
        else:
            raise ValueError("Fetch parameter must be either 'one' or 'all'")
        return str(result)

    def get_table_info_no_throw(self, table_names: Optional[List[str]] = None) -> str:
        try:
            return self.get_table_info(table_names)
        except Exception as e:
            return f"Error: {e}"

    def run_no_throw(self, command: str, fetch: str = "all") -> str:
        try:
            return self.run(command, fetch)
        except Exception as e:
            return f"Error: {e}"
