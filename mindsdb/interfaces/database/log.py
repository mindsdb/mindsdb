from typing import List
from collections import OrderedDict

from sqlalchemy import text
import pandas as pd

from mindsdb_sql.parser.ast import Select, BinaryOperation, Identifier, Constant
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.utilities.functions import resolve_table_identifier
from mindsdb.api.executor.utilities.sql import query_df, get_query_tables
from mindsdb.utilities.exception import EntityNotExistsError
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.context import context as ctx


class MindsdbDBTable:
    pass


class SystemTable(MindsdbDBTable):
    pass


class LogTable(SystemTable):
    def __init__(self, name: str) -> None:
        self.name = name
        self.deletable = False
        self.kind = 'table'
        self.visible = True


class LogDBController:
    def __init__(self):
        self._tables = {
            'llm_log': LogTable('llm_log')
        }

    def get_list(self) -> List[LogTable]:
        return list(self._tables.values())

    def get(self, name: str = None) -> LogTable:
        try:
            return self._tables[name]
        except KeyError:
            raise EntityNotExistsError(f'Table log.{name} does not exists')

    def get_tables(self) -> OrderedDict:
        data = OrderedDict()
        data['llm_log'] = LogTable('llm_log')
        return data

    def query(self, query: Select, session):
        if type(query) is not Select:
            raise Exception()
        tables = get_query_tables(query)
        if len(tables) != 1:
            raise Exception()
        table = tables[0]
        if table[0] is not None and table[0].lower() != 'log':
            raise Exception()
        if table[1].lower() not in ('llm_log',):
            raise Exception()

        render_engine = db.engine.name
        if render_engine == "postgresql":
            'postgres'
        render = SqlalchemyRender(render_engine)
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
        query_str = render.get_string(query, with_failback=False)
        df = pd.read_sql_query(query_str, db.engine)

        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in df.dtypes.items()
        ]

        # TODO filter columns

        return df.to_dict(orient='records'), columns_info
