import re
import csv
import inspect
import traceback
from io import StringIO
from typing import Iterable, List, Optional, Any

import pandas as pd
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Select, Show, Describe, Explain, Identifier

from mindsdb.utilities import log
from mindsdb.utilities.context import context as ctx
from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.integrations.libs.response import INF_SCHEMA_COLUMNS_NAMES
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.utilities.config import config
from mindsdb.interfaces.data_catalog.data_catalog_reader import DataCatalogReader

logger = log.getLogger(__name__)


def list_to_csv_str(array: List[List[Any]]) -> str:
    """Convert a 2D array into a CSV string.

    Args:
        array (List[List[Any]]): A 2D array/list of values to convert to CSV format

    Returns:
        str: The array formatted as a CSV string using Excel dialect
    """
    output = StringIO()
    writer = csv.writer(output, dialect="excel")
    str_array = [[str(item) for item in row] for row in array]
    writer.writerows(str_array)
    return output.getvalue()


def split_table_name(table_name: str) -> List[str]:
    """Split table name from llm to parts

    Args:
        table_name (str): input table name

    Returns:
        List[str]: parts of table identifier like ['database', 'schema', 'table']

    Example:
        'input': '`aaa`.`bbb.ccc`', 'output': ['aaa', 'bbb.ccc']
        'input': '`aaa`.`bbb`.`ccc`', 'output': ['aaa', 'bbb', 'ccc']
        'input': 'aaa.bbb', 'output': ['aaa', 'bbb']
        'input': '`aaa.bbb`', 'output': ['aaa.bbb']
        'input': '`aaa.bbb.ccc`', 'output': ['aaa.bbb.ccc']
        'input': 'aaa.`bbb`', 'output': ['aaa', 'bbb']
        'input': 'aaa.bbb.ccc', 'output': ['aaa', 'bbb', 'ccc']
        'input': 'aaa.`bbb.ccc`', 'output': ['aaa', 'bbb.ccc']
        'input': '`aaa`.`bbb.ccc`', 'output': ['aaa', 'bbb.ccc']
    """
    result = []
    current = ""
    in_backticks = False

    i = 0
    while i < len(table_name):
        if table_name[i] == "`":
            in_backticks = not in_backticks
        elif table_name[i] == "." and not in_backticks:
            if current:
                result.append(current.strip("`"))
                current = ""
        else:
            current += table_name[i]
        i += 1

    if current:
        result.append(current.strip("`"))

    # ensure we split the table name
    # result = [r.split(".") for r in result][0]

    return result


class SQLAgent:
    """
    SQLAgent is a class that handles SQL queries for agents.
    """

    def __init__(
        self,
        command_executor,
        databases: List[str],
        databases_struct: dict,
        knowledge_base_database: str = "mindsdb",
        include_tables: Optional[List[str]] = None,
        ignore_tables: Optional[List[str]] = None,
        include_knowledge_bases: Optional[List[str]] = None,
        ignore_knowledge_bases: Optional[List[str]] = None,
        sample_rows_in_table_info: int = 3,
        cache: Optional[dict] = None,
    ):
        """
        Initialize SQLAgent.

        Args:
            command_executor: Executor for SQL commands
            databases (List[str]): List of databases to use
            databases_struct (dict): Dictionary of database structures
            knowledge_base_database (str): Project name where knowledge bases are stored (defaults to 'mindsdb')
            include_tables (List[str]): Tables to include
            ignore_tables (List[str]): Tables to ignore
            include_knowledge_bases (List[str]): Knowledge bases to include
            ignore_knowledge_bases (List[str]): Knowledge bases to ignore
            sample_rows_in_table_info (int): Number of sample rows to include in table info
            cache (Optional[dict]): Cache for query results
        """
        self._command_executor = command_executor
        self._mindsdb_db_struct = databases_struct
        self.knowledge_base_database = knowledge_base_database  # This is a project name, not a database connection
        self._sample_rows_in_table_info = int(sample_rows_in_table_info)

        self._tables_to_include = include_tables
        self._tables_to_ignore = []
        self._knowledge_bases_to_include = include_knowledge_bases
        self._knowledge_bases_to_ignore = []
        self._databases = databases
        if not self._tables_to_include:
            # ignore_tables and include_tables should not be used together.
            # include_tables takes priority if it's set.
            self._tables_to_ignore = ignore_tables or []
        if not self._knowledge_bases_to_include:
            # ignore_knowledge_bases and include_knowledge_bases should not be used together.
            # include_knowledge_bases takes priority if it's set.
            self._knowledge_bases_to_ignore = ignore_knowledge_bases or []
        self._cache = cache

        from mindsdb.interfaces.skills.skill_tool import SkillToolController

        # Initialize the skill tool controller from MindsDB
        self.skill_tool = SkillToolController()

    def _call_engine(self, query: str, database=None):
        # switch database
        ast_query = parse_sql(query.strip("`"))
        self._check_permissions(ast_query)

        if database is None:
            # if we use tables with prefixes it should work for any database
            if self._databases is not None:
                # if we have multiple databases, we need to check which one to use
                # for now, we will just use the first one
                database = self._databases[0] if self._databases else "mindsdb"

        ret = self._command_executor.execute_command(ast_query, database_name=database)
        return ret

    def _check_permissions(self, ast_query):
        # check type of query
        if not isinstance(ast_query, (Select, Show, Describe, Explain)):
            raise ValueError(f"Query is not allowed: {ast_query.to_string()}")

        # Check tables
        if self._tables_to_include:
            tables_parts = [split_table_name(x) for x in self._tables_to_include]
            no_schema_parts = []
            for t in tables_parts:
                if len(t) == 3:
                    no_schema_parts.append([t[0], t[2]])
            tables_parts += no_schema_parts

            def _check_f(node, is_table=None, **kwargs):
                if is_table and isinstance(node, Identifier):
                    table_name = ".".join(node.parts)

                    # Get the list of available knowledge bases
                    kb_names = self.get_usable_knowledge_base_names()

                    # Check if this table is a knowledge base
                    is_kb = table_name in kb_names

                    # If it's a knowledge base and we have knowledge base restrictions
                    if is_kb and self._knowledge_bases_to_include:
                        kb_parts = [split_table_name(x) for x in self._knowledge_bases_to_include]
                        if node.parts not in kb_parts:
                            raise ValueError(
                                f"Knowledge base {table_name} not found. Available knowledge bases: {', '.join(self._knowledge_bases_to_include)}"
                            )
                    # Regular table check
                    elif not is_kb and self._tables_to_include and node.parts not in tables_parts:
                        raise ValueError(
                            f"Table {table_name} not found. Available tables: {', '.join(self._tables_to_include)}"
                        )
                    # Check if it's a restricted knowledge base
                    elif is_kb and table_name in self._knowledge_bases_to_ignore:
                        raise ValueError(f"Knowledge base {table_name} is not allowed.")
                    # Check if it's a restricted table
                    elif not is_kb and table_name in self._tables_to_ignore:
                        raise ValueError(f"Table {table_name} is not allowed.")

            query_traversal(ast_query, _check_f)

    def get_usable_table_names(self) -> Iterable[str]:
        """Get a list of tables that the agent has access to.

        Returns:
            Iterable[str]: list with table names
        """
        cache_key = f"{ctx.company_id}_{','.join(self._databases)}_tables"

        # first check cache and return if found
        if self._cache:
            cached_tables = self._cache.get(cache_key)
            if cached_tables:
                return cached_tables

        if self._tables_to_include:
            return self._tables_to_include

        result_tables = []

        for db_name in self._mindsdb_db_struct:
            handler = self._command_executor.session.integration_controller.get_data_handler(db_name)

            schemas_names = list(self._mindsdb_db_struct[db_name].keys())
            if len(schemas_names) > 1 and None in schemas_names:
                raise Exception("default schema and named schemas can not be used in same filter")

            if None in schemas_names:
                # get tables only from default schema
                response = handler.get_tables()
                tables_in_default_schema = list(response.data_frame.table_name)
                schema_tables_restrictions = self._mindsdb_db_struct[db_name][None]  # None - is default schema
                if schema_tables_restrictions is None:
                    for table_name in tables_in_default_schema:
                        result_tables.append([db_name, table_name])
                else:
                    for table_name in schema_tables_restrictions:
                        if table_name in tables_in_default_schema:
                            result_tables.append([db_name, table_name])
            else:
                if "all" in inspect.signature(handler.get_tables).parameters:
                    response = handler.get_tables(all=True)
                else:
                    response = handler.get_tables()
                response_schema_names = list(response.data_frame.table_schema.unique())
                schemas_intersection = set(schemas_names) & set(response_schema_names)
                if len(schemas_intersection) == 0:
                    raise Exception("There are no allowed schemas in ds")

                for schema_name in schemas_intersection:
                    schema_sub_df = response.data_frame[response.data_frame["table_schema"] == schema_name]
                    if self._mindsdb_db_struct[db_name][schema_name] is None:
                        # all tables from schema allowed
                        for row in schema_sub_df:
                            result_tables.append([db_name, schema_name, row["table_name"]])
                    else:
                        for table_name in self._mindsdb_db_struct[db_name][schema_name]:
                            if table_name in schema_sub_df["table_name"].values:
                                result_tables.append([db_name, schema_name, table_name])

        result_tables = [".".join(x) for x in result_tables]
        if self._cache:
            self._cache.set(cache_key, set(result_tables))
        return result_tables

    def get_usable_knowledge_base_names(self) -> Iterable[str]:
        """Get a list of knowledge bases that the agent has access to.

        Returns:
            Iterable[str]: list with knowledge base names
        """
        cache_key = f"{ctx.company_id}_{self.knowledge_base_database}_knowledge_bases"

        # todo we need to fix the cache, file cache can potentially store out of data information
        # # first check cache and return if found
        # if self._cache:
        #    cached_kbs = self._cache.get(cache_key)
        #     if cached_kbs:
        #        return cached_kbs

        if self._knowledge_bases_to_include:
            return self._knowledge_bases_to_include

        try:
            # Query to get all knowledge bases
            query = f"SHOW KNOWLEDGE_BASES FROM {self.knowledge_base_database};"
            try:
                result = self._call_engine(query, database=self.knowledge_base_database)
            except Exception as e:
                # If the direct query fails, try a different approach
                # This handles the case where knowledge_base_database is not a valid integration
                logger.warning(f"Error querying knowledge bases from {self.knowledge_base_database}: {str(e)}")
                # Try to get knowledge bases directly from the project database
                try:
                    # Get knowledge bases from the project database
                    kb_controller = self._command_executor.session.kb_controller
                    kb_names = [kb["name"] for kb in kb_controller.list()]

                    # Filter knowledge bases based on include list
                    if self._knowledge_bases_to_include:
                        kb_names = [kb_name for kb_name in kb_names if kb_name in self._knowledge_bases_to_include]
                        if not kb_names:
                            logger.warning(
                                f"No knowledge bases found in the include list: {self._knowledge_bases_to_include}"
                            )
                            return []

                        return kb_names

                    # Filter knowledge bases based on ignore list
                    kb_names = [kb_name for kb_name in kb_names if kb_name not in self._knowledge_bases_to_ignore]

                    if self._cache:
                        self._cache.set(cache_key, set(kb_names))

                    return kb_names
                except Exception as inner_e:
                    logger.error(f"Error getting knowledge bases from kb_controller: {str(inner_e)}")
                    return []

            if not result:
                return []

            # Filter knowledge bases based on ignore list
            kb_names = []
            for row in result:
                kb_name = row["name"]
                if kb_name not in self._knowledge_bases_to_ignore:
                    kb_names.append(kb_name)

            if self._cache:
                self._cache.set(cache_key, set(kb_names))

            return kb_names
        except Exception as e:
            # If there's an error, log it and return an empty list
            logger.error(f"Error in get_usable_knowledge_base_names: {str(e)}")
            return []

    def _resolve_table_names(self, table_names: List[str], all_tables: List[Identifier]) -> List[Identifier]:
        """
        Tries to find table (which comes directly from an LLM) by its name
        Handles backticks (`) and tables without databases
        """

        # index to lookup table
        tables_idx = {}
        for table in all_tables:
            # by name
            if len(table.parts) == 3:
                tables_idx[tuple(table.parts[1:])] = table
            else:
                tables_idx[(table.parts[-1],)] = table
            # by path
            tables_idx[tuple(table.parts)] = table

        tables = []
        for table_name in table_names:
            if not table_name.strip():
                continue

            # Some LLMs (e.g. gpt-4o) may include backticks or quotes when invoking tools.
            table_parts = split_table_name(table_name)
            if len(table_parts) == 1:
                # most likely LLM enclosed all table name in backticks `database.table`
                table_parts = split_table_name(table_name)

            # resolved table
            table_identifier = tables_idx.get(tuple(table_parts))

            if table_identifier is None:
                raise ValueError(f"Table {table} not found in the database")
            tables.append(table_identifier)

        return tables

    def get_knowledge_base_info(self, kb_names: Optional[List[str]] = None) -> str:
        """Get information about specified knowledge bases.
        Follows best practices as specified in: Rajkumar et al, 2022 (https://arxiv.org/abs/2204.00498)
        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as demonstrated in the paper.
        """

        kbs_info = []
        for kb in kb_names:
            key = f"{ctx.company_id}_{kb}_info"
            kb_info = self._cache.get(key) if self._cache else None
            if True or kb_info is None:
                kb_info = self.get_kb_sample_rows(kb)
                if self._cache:
                    self._cache.set(key, kb_info)

            kbs_info.append(kb_info)

        return "\n\n".join(kbs_info)

    def get_table_info(self, table_names: Optional[List[str]] = None) -> str:
        """Get information about specified tables.
        Follows best practices as specified in: Rajkumar et al, 2022 (https://arxiv.org/abs/2204.00498)
        If `sample_rows_in_table_info`, the specified number of sample rows will be
        appended to each table description. This can increase performance as demonstrated in the paper.
        """
        if config.get("data_catalog", {}).get("enabled", False):
            database_table_map = {}
            for name in table_names or self.get_usable_table_names():
                name = name.replace("`", "")

                parts = name.split(".", 1)
                # TODO: Will there be situations where parts has more than 2 elements? Like a schema?
                # This is unlikely given that we default to a single schema per database.
                if len(parts) == 1:
                    raise ValueError(f"Invalid table name: {name}. Expected format is 'database.table'.")

                database_table_map[parts[0]] = database_table_map.get(parts[0], []) + [parts[1]]

            data_catalog_str = ""
            for database_name, table_names in database_table_map.items():
                data_catalog_reader = DataCatalogReader(database_name=database_name, table_names=table_names)

                data_catalog_str += data_catalog_reader.read_metadata_as_string()

            return data_catalog_str

        else:
            # TODO: Improve old logic without data catalog
            all_tables = []
            for name in self.get_usable_table_names():
                # remove backticks
                name = name.replace("`", "")

                split = name.split(".")
                if len(split) > 1:
                    all_tables.append(Identifier(parts=[split[0], split[1]]))
                else:
                    all_tables.append(Identifier(name))

            if table_names is not None:
                all_tables = self._resolve_table_names(table_names, all_tables)

            tables_info = []
            for table in all_tables:
                key = f"{ctx.company_id}_{table}_info"
                table_info = self._cache.get(key) if self._cache else None
                if True or table_info is None:
                    table_info = self._get_single_table_info(table)
                    if self._cache:
                        self._cache.set(key, table_info)

                tables_info.append(table_info)

            return "\n\n".join(tables_info)

    def get_kb_sample_rows(self, kb_name: str) -> str:
        """Get sample rows from a knowledge base.

        Args:
            kb_name (str): The name of the knowledge base.

        Returns:
            str: A string containing the sample rows from the knowledge base.
        """
        logger.info(f"_get_sample_rows: knowledge base={kb_name}")
        command = f"select * from {kb_name} limit 10;"
        try:
            ret = self._call_engine(command)
            sample_rows = ret.data.to_lists()

            def truncate_value(val):
                str_val = str(val)
                return str_val if len(str_val) < 100 else (str_val[:100] + "...")

            sample_rows = list(map(lambda row: [truncate_value(value) for value in row], sample_rows))
            sample_rows_str = "\n" + f"{kb_name}:" + list_to_csv_str(sample_rows)
        except Exception as e:
            logger.info(f"_get_sample_rows error: {e}")
            sample_rows_str = "\n" + "\t [error] Couldn't retrieve sample rows!"

        return sample_rows_str

    def _get_single_table_info(self, table: Identifier) -> str:
        if len(table.parts) < 2:
            raise ValueError(f"Database is required for table: {table}")
        if len(table.parts) == 3:
            integration, schema_name, table_name = table.parts[-3:]
        else:
            schema_name = None
            integration, table_name = table.parts[-2:]

        table_str = str(table)

        dn = self._command_executor.session.datahub.get(integration)

        fields, dtypes = [], []
        try:
            df = dn.get_table_columns_df(table_name, schema_name)
            if not isinstance(df, pd.DataFrame) or df.empty:
                logger.warning(f"Received empty or invalid DataFrame for table columns of {table_str}")
                return f"Table named `{table_str}`:\n [No column information available]"

            fields = df[INF_SCHEMA_COLUMNS_NAMES.COLUMN_NAME].to_list()
            dtypes = [
                mysql_data_type.value if isinstance(mysql_data_type, MYSQL_DATA_TYPE) else (data_type or "UNKNOWN")
                for mysql_data_type, data_type in zip(
                    df[INF_SCHEMA_COLUMNS_NAMES.MYSQL_DATA_TYPE], df[INF_SCHEMA_COLUMNS_NAMES.DATA_TYPE]
                )
            ]
        except Exception as e:
            logger.error(f"Failed processing column info for {table_str}: {e}", exc_info=True)
            raise ValueError(f"Failed to process column info for {table_str}") from e

        if not fields:
            logger.error(f"Could not extract column fields for {table_str}.")
            return f"Table named `{table_str}`:\n [Could not extract column information]"

        try:
            sample_rows_info = self._get_sample_rows(table_str, fields)
        except Exception as e:
            logger.warning(f"Could not get sample rows for {table_str}: {e}")
            sample_rows_info = "\n\t [error] Couldn't retrieve sample rows!"

        info = f"Table named `{table_str}`:\n"
        info += f"\nSample with first {self._sample_rows_in_table_info} rows from table {table_str} in CSV format (dialect is 'excel'):\n"
        info += sample_rows_info + "\n"
        info += (
            "\nColumn data types: "
            + ",\t".join([f"\n`{field}` : `{dtype}`" for field, dtype in zip(fields, dtypes)])
            + "\n"
        )
        return info

    def _get_sample_rows(self, table: str, fields: List[str]) -> str:
        logger.info(f"_get_sample_rows: table={table} fields={fields}")
        command = f"select {', '.join(fields)} from {table} limit {self._sample_rows_in_table_info};"
        try:
            ret = self._call_engine(command)
            sample_rows = ret.data.to_lists()

            def truncate_value(val):
                str_val = str(val)
                return str_val if len(str_val) < 100 else (str_val[:100] + "...")

            sample_rows = list(map(lambda row: [truncate_value(value) for value in row], sample_rows))
            sample_rows_str = "\n" + list_to_csv_str([fields] + sample_rows)
        except Exception as e:
            logger.info(f"_get_sample_rows error: {e}")
            sample_rows_str = "\n" + "\t [error] Couldn't retrieve sample rows!"

        return sample_rows_str

    def _clean_query(self, query: str) -> str:
        # Sometimes LLM can input markdown into query tools.
        cmd = re.sub(r"```(sql)?", "", query)
        return cmd

    def query(self, command: str, fetch: str = "all") -> str:
        """Execute a SQL command and return a string representing the results.
        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """

        def _repr_result(ret):
            limit_rows = 30

            columns_str = ", ".join([repr(col.name) for col in ret.columns])
            res = f"Output columns: {columns_str}\n"

            data = ret.to_lists()
            if len(data) > limit_rows:
                df = pd.DataFrame(data, columns=[col.name for col in ret.columns])

                res += f"Result has {len(data)} rows. Description of data:\n"
                res += str(df.describe(include="all")) + "\n\n"
                res += f"First {limit_rows} rows:\n"

            else:
                res += "Result in CSV format (dialect is 'excel'):\n"
            res += list_to_csv_str([[col.name for col in ret.columns]] + data[:limit_rows])
            return res

        ret = self._call_engine(self._clean_query(command))
        if fetch == "all":
            result = _repr_result(ret.data)
        elif fetch == "one":
            result = "Result in CSV format (dialect is 'excel'):\n"
            result += list_to_csv_str([[col.name for col in ret.data.columns]] + [ret.data.to_lists()[0]])
        else:
            raise ValueError("Fetch parameter must be either 'one' or 'all'")
        return str(result)

    def get_table_info_safe(self, table_names: Optional[List[str]] = None) -> str:
        try:
            logger.info(f"get_table_info_safe: {table_names}")
            return self.get_table_info(table_names)
        except Exception as e:
            logger.info(f"get_table_info_safe error: {e}")
            return f"Error: {e}"

    def query_safe(self, command: str, fetch: str = "all") -> str:
        try:
            logger.info(f"query_safe (fetch={fetch}): {command}")
            return self.query(command, fetch)
        except Exception as e:
            logger.error(f"Error in query_safe: {str(e)}\n{traceback.format_exc()}")
            logger.info(f"query_safe error: {e}")
            msg = f"Error: {e}"
            if "does not exist" in msg and " relation " in msg:
                msg += "\nAvailable tables: " + ", ".join(self.get_usable_table_names())
            return msg
