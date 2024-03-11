from typing import List
from collections import OrderedDict

from sqlalchemy import Boolean
import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Select, BinaryOperation, Identifier, Constant, Star
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.utilities.functions import resolve_table_identifier
from mindsdb.api.executor.utilities.sql import get_query_tables
from mindsdb.utilities.exception import EntityNotExistsError
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.context import context as ctx


class LogTable:
    def __init__(self, name: str) -> None:
        self.name = name
        self.deletable = False
        self.kind = 'table'
        self.visible = True


class LogDBController:
    def __init__(self):
        self._tables = OrderedDict()
        self._tables['llm_log'] = LogTable('llm_log')

    def get_list(self) -> List[LogTable]:
        return list(self._tables.values())

    def get(self, name: str = None) -> LogTable:
        try:
            return self._tables[name]
        except KeyError:
            raise EntityNotExistsError(f'Table log.{name} does not exists')

    def get_tables(self) -> OrderedDict:
        return self._tables

    def query(self, query: Select = None, native_query: str = None, session=None):
        if native_query is not None:
            if query is not None:
                raise Exception("'query' and 'native_query' arguments can not be used together")
            query = parse_sql(native_query)

        if type(query) is not Select:
            raise Exception("Only 'SELECT' is allowed for tables in log database")
        tables = get_query_tables(query)
        if len(tables) != 1:
            raise Exception("Only one table may be in query to log database")
        table = tables[0]
        if table[0] is not None and table[0].lower() != 'log':
            raise Exception("This is not a query to the log database")
        if table[1].lower() not in self._tables.keys():
            raise Exception(f"There is no table '{table[1]}' in the log database")

        # region inject company_id
        company_id_op = BinaryOperation(
            op='is' if ctx.company_id is None else '=',
            args=(
                Identifier('company_id'),
                Constant(ctx.company_id),
            )
        )
        if query.where is None:
            query.where = company_id_op
        else:
            query.where = BinaryOperation(
                op='and',
                args=(
                    query.where,
                    company_id_op
                )
            )
        # endregion

        # region check that only allowed identifiers are used in the query
        available_columns_names = [column.name.lower() for column in db.LLMLog.__table__.columns]
        available_columns_names.remove('id')

        def check_columns(node, is_table, **kwargs):
            # region replace * to available columns
            if type(node) is Select:
                new_targets = []
                for target in node.targets:
                    if type(target) is Star:
                        new_targets += [Identifier(name) for name in available_columns_names]
                    else:
                        new_targets.append(target)
                node.targets = new_targets
            # endregion

            if type(node) is Identifier and is_table is False:
                parts = resolve_table_identifier(node)
                if parts[0] is not None and parts[0].lower() != 'llm_log':
                    raise Exception(f"Table '{parts[0]}' can not be used in query")
                if parts[1].lower() not in available_columns_names:
                    raise Exception(f"Column '{parts[1]}' can not be used in query")

        query_traversal(query, check_columns)
        # endregion

        render_engine = db.engine.name
        if render_engine == "postgresql":
            'postgres'
        render = SqlalchemyRender(render_engine)
        query_str = render.get_string(query, with_failback=False)
        df = pd.read_sql_query(query_str, db.engine)

        # region fix boolean column for sqllite (otherwise vals will be integer)
        if render_engine == 'sqlite':
            for column_name in df.columns:
                model_columns_types = {
                    x.name.lower(): type(x.type)
                    for x in db.LLMLog.__table__.columns._all_columns
                }
                if model_columns_types[column_name.lower()] is Boolean:
                    df['success'] = df['success'].astype(bool)
        # endregion

        columns_info = [{
            'name': k,
            'type': v
        } for k, v in df.dtypes.items()]

        return df.to_dict(orient='split')['data'], columns_info
