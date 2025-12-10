import csv
from io import StringIO
from typing import List, Any, Tuple
from collections import defaultdict
import fnmatch

import pandas as pd
from mindsdb_sql_parser.ast import Identifier

from mindsdb.utilities import log
from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.mysql.mysql_proxy.mysql_proxy import SQLAnswer
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.api.executor.exceptions import ExecutorException, UnknownError
from mindsdb.utilities.exception import QueryError
from pydantic import BaseModel, Field



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

    return result


class TablesCollection:
    """
    Collection of identifiers.
    Supports wildcard in tables name.
    """

    def __init__(self, items: List[Identifier | str] = None, default_db=None):
        if items is None:
            items = []

        self.items = items
        self._dbs = defaultdict(set)
        self._schemas = defaultdict(dict)
        self._no_db_tables = set()
        self.has_wildcard = False
        self.databases = set()
        self._default_db = default_db

        for name in items:
            if not isinstance(name, Identifier):
                name = Identifier(name)
            db, schema, tbl = self._get_paths(name)
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



class QueryType:
    FINAL = "final_query"  # this is the final query
    EXPLORATORY = "exploratory_query"  # this is a query to explore and collect info to solve the challenge (e.g., distinct values of a categorical column, schema inference, etc.)

class Plan(BaseModel):
    plan: str = Field(..., description="A step-by-step plan for solving the question, identifying data sources and steps needed")
    estimated_steps: int = Field(..., description="Estimated number of steps needed to solve the question")

class SQLQuery(BaseModel):
    sql_query: str = Field(..., description="The SQL query to run")
    short_description: str = Field(..., description="A short summary or description of the SQL query's purpose")
    query_type: str = Field(QueryType.FINAL, description="Type of query: 'final_query' for the main query if we can solve the question we were asked, 'exploratory_query' for queries used to explore or collect info to solve the question, if we need to interrogagte the database a bit more")

class MindsDBQuery:
    def __init__(self, context=None):
        self.context = context or {}

    def execute(self, query: str):
        mysql_proxy = FakeMysqlProxy()
        mysql_proxy.set_context(self.context)
        query_response = None
        try:
            result: SQLAnswer = mysql_proxy.process_query(query)
            
            # Check for errors
            if result.type == SQL_RESPONSE_TYPE.ERROR:
                error_text = result.error_message or "Unknown error"
                
                raise QueryError(
                    db_error_msg=error_text,
                    failed_query=query,
                    is_external=False,
                    is_expected=True
                )
            
            # Check if result has data (TABLE response)
            if result.type != SQL_RESPONSE_TYPE.TABLE or result.result_set is None:
                # Query executed successfully but returned no data (e.g., INSERT, UPDATE, DELETE)
                return pd.DataFrame()
            
            query_response = result.result_set.to_df()
            return query_response

        except ExecutorException as e:
            # classified error
            error_text = str(e)
            
            raise QueryError(
                db_error_msg=error_text,
                failed_query=query,
                is_external=False,
                is_expected=True
            )
        except QueryError:
            raise
        except UnknownError as e:
            # unclassified error
            error_text = str(e)
            
            raise QueryError(
                db_error_msg=error_text,
                failed_query=query,
                is_external=False,
                is_expected=False
            )
        except Exception as e:
            # unexpected error
            error_text = str(e)
            
            raise QueryError(
                db_error_msg=error_text,
                failed_query=query,
                is_external=False,
                is_expected=False
            )


