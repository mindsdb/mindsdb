"""
    Wrapper around MindsDB's executor and integration controller following the implementation of the original
    langchain.sql_database.SQLDatabase class to partly replicate its behavior.
"""
import warnings
from typing import Iterable, List, Optional
from mindsdb_sql import parse_sql
from langchain.sql_database import SQLDatabase

from mindsdb.utilities import log

logger = log.getLogger(__name__)


class MindsDBSQL(SQLDatabase):
    """ Can't modify signature, as LangChain does a Pydantic check."""
    def __init__(
        self,
        engine,
        schema: Optional[str] = None,
        metadata: Optional = None,
        ignore_tables: Optional[List[str]] = None,
        include_tables: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        indexes_in_table_info: bool = False,
        custom_table_info: Optional[dict] = None,
        view_support: Optional[bool] = True,
    ):
        # Some args above are not used in this class, but are kept for compatibility
        self._engine = engine   # executor instance
        self._metadata = metadata  # integrations controller instance
        self._sample_rows_in_table_info = int(sample_rows_in_table_info)
        
        self._tables_to_include = include_tables
        self._tables_to_ignore = []
        if not self._tables_to_include:
            # ignore_tables and include_tables should not be used together.
            # include_tables takes priority if it's set.
            self._tables_to_ignore = ignore_tables

    @property
    def dialect(self) -> str:
        return 'mindsdb'

    def _call_engine(self, queries: List[str]):
        for query in queries:
            self._engine.is_executed = False
            ast_query = parse_sql(query.strip('`'), dialect='mindsdb')
            ret = self._engine.execute_command(ast_query)
        return ret

    def get_usable_table_names(self) -> Iterable[str]:
        if self._tables_to_include:
            return self._tables_to_include
        
        original_db = self._engine.session.database
        ret = self._call_engine(['show databases;'])
        dbs = [lst[0] for lst in ret.data if lst[0] != 'information_schema']
        usable_tables = []
        for db in dbs:
            if db != 'mindsdb':
                try:
                    ret = self._call_engine([f'use `{db}`;', 'show tables;'])
                    tables = [lst[0] for lst in ret.data if lst[0] != 'information_schema']
                    for table in tables:
                        table_name = f'{db}.{table}'
                        if table_name not in self._tables_to_ignore:
                            usable_tables.append(table_name)
                except Exception as e:
                    logger.warning('Unable to get tables for %s: %s', db, str(e))
                finally:
                    _ = self._call_engine([f'use {original_db};'])
        return usable_tables

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
            tables.append(table_info)

        final_str = "\n\n".join(tables)
        return final_str

    def _get_single_table_info(self, table_str: str) -> str:
        controller = self._metadata
        integration, table_name = table_str.split('.')
        cols_df = controller.get_handler(integration).get_columns(table_name).data_frame
        fields = cols_df['Field'].to_list()
        dtypes = cols_df['Type'].to_list()

        info = f'Table named `{table_name}`\n'
        info += f"\n/* Sample with first {self._sample_rows_in_table_info} rows from table `{table_str}`:\n"
        info += "\t".join([field for field in fields])
        info += self._get_sample_rows(table_str, fields) + "\n*/"
        info += '\nColumn data types: ' + ",\t".join([f'`{field}` : `{dtype}`' for field, dtype in zip(fields, dtypes)]) + '\n'  # noqa
        return info

    def _get_sample_rows(self, table: str, fields: List[str]) -> str:
        command = f"select {','.join(fields)} from {table} limit {self._sample_rows_in_table_info};"
        try:
            ret = self._call_engine([command])
            sample_rows = ret.data
            sample_rows = list(map(lambda ls: [str(i) if len(str(i)) < 100 else str[:100]+'...' for i in ls], sample_rows))
            sample_rows_str = "\n" + "\n".join(["\t".join(row) for row in sample_rows])
        except Exception:
            sample_rows_str = "\n" + "\t [error] Couldn't retrieve sample rows!"

        return sample_rows_str

    def run(self, command: str, fetch: str = "all") -> str:
        """Execute a SQL command and return a string representing the results.
        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """
        def _tidy(result: List) -> str:
            return '\n'.join(['\t'.join([str(value) for value in row]) for row in result])
        ret = self._call_engine([command])
        if fetch == "all":
            result = _tidy(ret.data)
        elif fetch == "one":
            result = _tidy(ret.data[0])
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
