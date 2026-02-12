import copy
import csv
import inspect
from io import StringIO
from typing import List, Any, Tuple
from collections import defaultdict
import fnmatch
import re

import pandas as pd
from mindsdb_sql_parser.ast import Identifier, ASTNode

from mindsdb.utilities import log
from mindsdb.utilities.config import config
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.cache import get_cache
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser import Constant, Select, Show, Describe, Explain, Union, Intersect, Except
from mindsdb.integrations.utilities.query_traversal import query_traversal


logger = log.getLogger(__name__)

_MAX_CACHE_SIZE = 100


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

    return result


def _escape_identifiers(node: ASTNode, **kwargs) -> None:
    """Escape identifier and alias if possible

    Args:
        node: The AST node to escape
        **kwargs: Additional keyword arguments

    Returns:
        None
    """
    if isinstance(getattr(node, 'alias'), Identifier):
        node.alias.is_quoted = [True] * len(node.alias.parts)
    if not isinstance(node, Identifier):
        return
    node.is_quoted = [True] * len(node.parts)


class TablesCollection:
    """
    Collection of identifiers.
    Supports wildcard in tables name.
    """

    def __init__(self, items: List[Identifier | str] = None, default_db=None):
        self._dbs = defaultdict(set)
        self._schemas = defaultdict(dict)
        self._no_db_tables = set()
        self.has_wildcard = False
        self.databases = set()
        self._default_db = default_db

        if items is None:
            items = []
        self.items = []
        for name in items:
            if not isinstance(name, Identifier):
                name = Identifier(name)
            self.items.append(name)
            db, schema, tbl = self._get_paths(name)
            if db is None:
                db = self._default_db
            if db is None:
                self._no_db_tables.add(tbl)
            elif schema is None:
                self._dbs[db].add(tbl)
            else:
                if schema not in self._schemas[db]:
                    self._schemas[db][schema] = set()
                self._schemas[db][schema].add(tbl)

            if "*" in tbl:
                self.has_wildcard = True
            self.databases.add(db)

    def _get_paths(self, table: Identifier) -> Tuple:
        # split identifier to db, schema, table name
        schema = None
        db = None

        match [x.lower() for x in table.parts]:
            case [tbl]:
                pass
            case [db, tbl]:
                pass
            case [db, schema, tbl]:
                pass
            case _:
                raise NotImplementedError
        return db, schema, tbl.lower()

    def match(self, table: Identifier) -> bool:
        # Check if input table matches to tables in collection

        db, schema, tbl = self._get_paths(table)
        if db is None:
            if tbl in self._no_db_tables:
                return True
            if self._default_db is not None:
                return self.match(Identifier(parts=[self._default_db, tbl]))

        if schema is not None:
            if any([fnmatch.fnmatch(tbl, pattern) for pattern in self._schemas[db].get(schema, [])]):
                return True

        # table might be specified without schema
        return any([fnmatch.fnmatch(tbl, pattern) for pattern in self._dbs[db]])

    def __bool__(self):
        return len(self.items) > 0

    def __repr__(self):
        return f"Tables({self.items})"


class MindsDBQuery:
    def __init__(self, tables=None, knowledge_bases=None):
        self.tables = TablesCollection(tables or [])
        self.knowledge_bases = TablesCollection(knowledge_bases or [], default_db=config.get("default_project"))
        self.command_executor = self.get_command_executor()
        self._cache = get_cache("agent", max_size=_MAX_CACHE_SIZE)

    def get_command_executor(self):
        from mindsdb.api.executor.command_executor import ExecuteCommands
        from mindsdb.api.executor.controllers import (
            SessionController,
        )  # Top-level import produces circular import in some cases TODO: figure out a fix without losing runtime improvements (context: see #9304)  # noqa

        sql_session = SessionController()
        sql_session.database = config.get("default_project")

        return ExecuteCommands(sql_session)

    def _clean_query(self, query: str) -> str:
        # Sometimes LLM can input markdown into query tools.
        cmd = re.sub(r"```(sql)?", "", query)
        return cmd

    def execute_sql(self, sql: str, check_permissions=True, escape_identifiers: bool = False):
        sql = self._clean_query(sql)
        ast_query = parse_sql(sql.strip("`"))
        if escape_identifiers:
            query_traversal(ast_query, _escape_identifiers)
        if check_permissions:
            self._check_permissions(ast_query)

        return self.execute(ast_query)

    def execute(self, query):
        """Execute a SQL command and return a string representing the results.
        If the statement returns rows, a string of the results is returned.
        If the statement returns no rows, an empty string is returned.
        """

        if isinstance(query, Select) and query.limit is None:
            query.limit = Constant(100)
        query = copy.deepcopy(query)
        ret = self.command_executor.execute_command(query)
        if ret.error_code is not None:
            raise RuntimeError(ret.error_message)
        if ret.data is not None:
            return ret.data.to_df()

        return pd.DataFrame([])

    def _check_permissions(self, ast_query):
        # check type of query
        if not isinstance(ast_query, (Select, Show, Describe, Explain, Union, Intersect, Except)):
            raise ValueError(f"Query is not allowed: {ast_query.to_string()}")

        cte_names = []
        if isinstance(ast_query, Select) and ast_query.cte is not None:
            cte_names = [expr.name.parts[-1] for expr in ast_query.cte]

        def _check_f(node, is_table=None, **kwargs):
            if not (is_table and isinstance(node, Identifier)):
                return

            if self.knowledge_bases.match(node):
                return

            if self.tables.match(node):
                return

            if len(node.parts) == 1:
                if node.parts[0] in cte_names:
                    return

            if "." in node.parts[0]:
                # extract quoted parts (with dots) to sub-parts
                parts = []
                for i, item in enumerate(node.parts):
                    if node.is_quoted[i] and "." in item:
                        parts.extend(Identifier(item).parts)
                    else:
                        parts.append(item)
                node2 = Identifier(parts=parts)

                if self.tables.match(node2):
                    return node2

                if self.knowledge_bases.match(node2):
                    return node2

            # no success: show exception
            error = f"{str(node)} not found."
            if self.tables:
                error += f" Available tables: {self.tables.items}"

            if self.knowledge_bases:
                error += f" Available knowledge bases: {self.knowledge_bases.items}"

            elif not self.tables:
                # no KBs and not tables
                error += "You don't have access to any tables or knowledge bases."

            raise ValueError(error)

        query_traversal(ast_query, _check_f)

    def get_usable_table_names(self):
        if not self.tables:
            # no tables allowed
            return []
        if not self.tables.has_wildcard:
            return self.tables.items

        result_tables = []

        for db_name in self.tables.databases:
            cache_key = f"{ctx.company_id}_db_tables_{db_name}"

            # first check cache and return if found
            list_tables = None
            if self._cache:
                list_tables = self._cache.get(cache_key)

            if list_tables is None:
                # get from handler
                list_tables = []
                handler = self.command_executor.session.integration_controller.get_data_handler(db_name)

                if "all" in inspect.signature(handler.get_tables).parameters:
                    response = handler.get_tables(all=True)
                else:
                    response = handler.get_tables()
                df = response.data_frame
                col_name = "table_name"
                if col_name not in df.columns:
                    # get first column if not found
                    col_name = df.columns[0]
                for _, row in df.iterrows():
                    list_tables.append({"schema": row.get("table_schema"), "name": row[col_name]})

                if self._cache:
                    self._cache.set(cache_key, list_tables)

            result_tables = []
            for row in list_tables:
                if row.get("schema") is not None:
                    parts = [db_name, row["schema"], row["name"]]
                else:
                    parts = [db_name, row["name"]]

                if self.tables.match(Identifier(parts=parts)):
                    result_tables.append(Identifier(parts=parts))

        return result_tables

    def get_usable_knowledge_base_names(self):
        if not self.knowledge_bases:
            # no tables allowed
            return []
        if not self.knowledge_bases.has_wildcard:
            return self.knowledge_bases.items

        try:
            # Query to get all knowledge bases
            ast_query = Show(category="Knowledge Bases")
            result = self.command_executor.execute_command(ast_query)

            kb_names = []
            for row in result.data.records:
                kb = Identifier(parts=[row["PROJECT"], row["NAME"]])

                # Filter knowledge bases
                if self.knowledge_bases.match(kb):
                    kb_names.append(kb)

            return kb_names
        except Exception:
            # If there's an error, log it and return an empty list
            logger.exception("Error in get_usable_knowledge_base_names")
            return []
