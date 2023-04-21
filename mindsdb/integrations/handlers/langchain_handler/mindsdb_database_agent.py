"""
    Wrapper around MindsDB's executor following the implementation of the original
    langchain.sql_database.SQLDatabase class to replicate its behavior.
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
        engine,  # ref to the mindsdb executor
        schema: Optional[str] = None,
        metadata: Optional = None,
        ignore_tables: Optional[List[str]] = None,  # TODO: use this
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        view_support: Optional[bool] = True,  # TODO: true by default?
    ):
        # Some args above are not used in this class, but are kept for compatibility
        # TODO: call this from within the handler, two modes, either as tool or as DB.
        self._engine = engine   # TODO add executor here, `engine` should be the reference to the executor
        self._schema = schema  # TODO what is this?
        self._metadata = metadata  # TODO Can we avoid this?
        self._all_tables = self._usable_tables = set(include_tables)  # TODO: set according to select data query/using
        self._sample_rows_in_table_info = int(sample_rows_in_table_info)
        self._indexes_in_table_info = indexes_in_table_info
        self._custom_table_info = custom_table_info

        # TODO: should we support this?
        if self._custom_table_info:
            if not isinstance(self._custom_table_info, dict):
                raise TypeError(
                    "table_info must be a dictionary with table names as keys and the "
                    "desired table info as values"
                )
            # only keep the tables that are also present in the database
            intersection = set(self._custom_table_info).intersection(self._all_tables)
            self._custom_table_info = dict(
                (table, self._custom_table_info[table])
                for table in self._custom_table_info
                if table in intersection
            )

    @classmethod
    def from_uri(cls, database_uri: str, engine_args: Optional[dict] = None, **kwargs: Any):
        pass  # TODO: implement?

    @property
    def dialect(self) -> str:
        return 'mindsdb'

    def get_usable_table_names(self) -> Iterable[str]:
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
        # #################### #
        # TODO: implement this right
        # #################### #
        all_table_names = self.get_usable_table_names()
        if table_names is not None:
            missing_tables = set(table_names).difference(all_table_names)
            if missing_tables:
                raise ValueError(f"table_names {missing_tables} not found in database")
            all_table_names = table_names

        tables = []
        for table in all_table_names:
            if self._custom_table_info and table in self._custom_table_info:
                tables.append(self._custom_table_info[table])
                continue

            table_info = f"{table.rstrip()}"  # TODO: how to pass the table's column, data types, etc?
            has_extra_info = (self._indexes_in_table_info or self._sample_rows_in_table_info)
            if has_extra_info:
                table_info += "\n\n/*"
            if self._sample_rows_in_table_info:
                table_info += f"\n{self._get_sample_rows(table)}\n"
            if has_extra_info:
                table_info += "*/"
            tables.append(table_info)

        final_str = "\n\n".join(all_table_names)
        return final_str

    # TODO: double check we don't need the _get_table_indexes() method, else, implement

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
        # TODO:
        # self._engine should be the mindsdb executor
        # pass str sql command to mindsdb executor
        # if fetch all, return all rows
        # if fetch one, return one row
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
