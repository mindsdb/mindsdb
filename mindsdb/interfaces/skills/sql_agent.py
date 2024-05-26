from typing import Iterable, List, Optional

import pandas as pd
from mindsdb_sql import parse_sql, Identifier
from mindsdb_sql.planner.utils import query_traversal


from mindsdb.utilities import log

logger = log.getLogger(__name__)


class SQLAgent:

    def __init__(
            self,
            command_executor,
            database: str,
            include_tables: Optional[List[str]] = None,
            ignore_tables: Optional[List[str]] = None,
            sample_rows_in_table_info: int = 3,
    ):
        self._database = database
        self._command_executor = command_executor
        self._integration_controller = command_executor.session.integration_controller

        self._sample_rows_in_table_info = int(sample_rows_in_table_info)

        self._tables_to_include = include_tables
        self._tables_to_ignore = []
        self._database = database
        if not self._tables_to_include:
            # ignore_tables and include_tables should not be used together.
            # include_tables takes priority if it's set.
            self._tables_to_ignore = ignore_tables or []

    def _call_engine(self, query: str, database=None):
        # switch database

        ast_query = parse_sql(query.strip('`'))
        self._check_tables(ast_query)

        if database is None:
            database = self._database

        ret = self._command_executor.execute_command(
            ast_query,
            database_name=database
        )
        return ret

    def _check_tables(self, ast_query):

        def _check_f(node, is_table=None, **kwargs):
            if is_table and isinstance(node, Identifier):
                table = node.parts[-1]
                if table not in self._tables_to_include:
                    ValueError(f"Table {table} not found. Available tables: {', '.join(self._tables_to_include)}")

        query_traversal(ast_query, _check_f)

    def get_usable_table_names(self) -> Iterable[str]:
        if self._tables_to_include:
            return self._tables_to_include

        ret = self._call_engine('show databases;')
        dbs = [lst[0] for lst in ret.data if lst[0] != 'information_schema']
        usable_tables = []
        for db in dbs:
            if db != 'mindsdb' and db == self._database:
                try:
                    ret = self._call_engine('show tables', database=db)
                    tables = [lst[0] for lst in ret.data if lst[0] != 'information_schema']
                    for table in tables:
                        # By default, include all tables in a database unless expilcitly ignored.
                        table_name = f'{db}.{table}'
                        if table_name not in self._tables_to_ignore:
                            usable_tables.append(table_name)
                except Exception as e:
                    logger.warning('Unable to get tables for %s: %s', db, str(e))

        return usable_tables

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

    def get_table_columns(self, table_name: str) -> List[str]:
        controller = self._integration_controller
        cols_df = controller.get_data_handler(self._database).get_columns(table_name).data_frame
        return cols_df['Field'].to_list()

    def _get_single_table_info(self, table_str: str) -> str:
        controller = self._integration_controller
        integration, table_name = table_str.split('.')
        cols_df = controller.get_data_handler(integration).get_columns(table_name).data_frame
        fields = cols_df['Field'].to_list()
        dtypes = cols_df['Type'].to_list()

        info = f'Table named `{table_name}`\n'
        info += f"\n/* Sample with first {self._sample_rows_in_table_info} rows from table `{table_str}`:\n"
        info += "\t".join([field for field in fields])
        info += self._get_sample_rows(table_str, fields) + "\n*/"
        info += '\nColumn data types: ' + ",\t".join(
            [f'`{field}` : `{dtype}`' for field, dtype in zip(fields, dtypes)]) + '\n'  # noqa
        return info

    def _get_sample_rows(self, table: str, fields: List[str]) -> str:
        command = f"select {','.join(fields)} from {table} limit {self._sample_rows_in_table_info};"
        try:
            ret = self._call_engine(command)
            sample_rows = ret.data
            sample_rows = list(
                map(lambda ls: [str(i) if len(str(i)) < 100 else str[:100] + '...' for i in ls], sample_rows))
            sample_rows_str = "\n" + "\n".join(["\t".join(row) for row in sample_rows])
        except Exception:
            sample_rows_str = "\n" + "\t [error] Couldn't retrieve sample rows!"

        return sample_rows_str

    def query(self, command: str, fetch: str = "all") -> str:
        """Execute a SQL command and return a string representing the results.
        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """

        def _tidy(result: List) -> str:
            return '\n'.join(['\t'.join([str(value) for value in row]) for row in result])

        def _repr_result(ret):
            limit_rows = 30

            columns_str = ', '.join([repr(col.name) for col in ret.columns])
            res = f'Output columns: {columns_str}\n'

            if len(ret.data) > limit_rows:
                df = pd.DataFrame(ret.data, columns=[col.name for col in ret.columns])

                res += f'Result has {len(ret.data)} rows. Description of data:\n'
                res += str(df.describe(include='all')) + '\n\n'
                res += f'First {limit_rows} rows:\n'

            else:
                res += 'Result:\n'

            res += _tidy(ret.data[:limit_rows])
            return res

        ret = self._call_engine(command)
        if fetch == "all":
            result = _repr_result(ret)
        elif fetch == "one":
            result = _tidy(ret.data[0])
        else:
            raise ValueError("Fetch parameter must be either 'one' or 'all'")
        return str(result)

    def get_table_info_safe(self, table_names: Optional[List[str]] = None) -> str:
        try:
            return self.get_table_info(table_names)
        except Exception as e:
            return f"Error: {e}"

    def query_safe(self, command: str, fetch: str = "all") -> str:
        try:
            return self.query(command, fetch)
        except Exception as e:
            return f"Error: {e}"
