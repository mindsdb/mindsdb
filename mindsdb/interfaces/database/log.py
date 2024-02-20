from abc import ABC, abstractmethod
from typing import List
from collections import OrderedDict

import pandas as pd

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Select, Identifier, Star
# from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.utilities.functions import resolve_table_identifier
from mindsdb.api.executor.utilities.sql import get_query_tables
from mindsdb.utilities.exception import EntityNotExistsError
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.context import context as ctx


class LogTable(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self.deletable = False
        self.kind = 'table'
        self.visible = True

    @abstractmethod
    def _get_base(self):
        pass


class LLMLogTable(LogTable):
    columns = [
        'API_KEY', 'MODEL_NAME', 'INPUT', 'OUTPUT', 'START_TIME', 'END_TIME',
        'PROMPT_TOKENS', 'COMPLETION_TOKENS', 'TOTAL_TOKENS', 'SUCCESS'
    ]
    types_map = {
        'SUCCESS': 'boolean',
        'START_TIME': 'datetime64[ns]',
        'END_TIME': 'datetime64[ns]'
    }

    def _get_base(self):
        query = db.session.query(
            db.LLMLog, db.Predictor
        ).filter_by(
            company_id=ctx.company_id
        ).outerjoin(
            db.Predictor, db.Predictor.id == db.LLMLog.model_id
        ).with_entities(
            db.LLMLog.api_key.label('api_key'),
            db.Predictor.name.label('model_name'),
            db.LLMLog.input.label('input'),
            db.LLMLog.output.label('output'),
            db.LLMLog.start_time.label('start_time'),
            db.LLMLog.end_time.label('end_time'),
            db.LLMLog.prompt_tokens.label('prompt_tokens'),
            db.LLMLog.completion_tokens.label('completion_tokens'),
            db.LLMLog.total_tokens.label('total_tokens'),
            db.LLMLog.success.label('success')
        )
        return query


class JobsHistoryTable(LogTable):
    columns = ['NAME', 'PROJECT', 'RUN_START', 'RUN_END', 'ERROR', 'QUERY']
    types_map = {
        'RUN_START': 'datetime64[ns]',
        'RUN_END': 'datetime64[ns]'
    }

    def _get_base(self):
        query = db.session.query(
            db.JobsHistory, db.Jobs
        ).filter_by(
            company_id=ctx.company_id,
        ).outerjoin(
            db.Jobs, db.Jobs.id == db.JobsHistory.job_id
        ).outerjoin(
            db.Project, db.Project.id == db.Jobs.project_id
        ).with_entities(
            db.Jobs.name.label('name'),
            db.Project.name.label('project'),
            db.JobsHistory.start_at.label('run_start'),
            db.JobsHistory.end_at.label('run_end'),
            db.JobsHistory.error.label('error'),
            db.JobsHistory.query_str.label('query')
        )

        return query


class LogDBController:
    def __init__(self):
        self._tables = OrderedDict()
        self._tables['llm_log'] = LLMLogTable('llm_log')
        self._tables['jobs_history'] = JobsHistoryTable('jobs_history')

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

        log_table = self._tables[table[1].lower()]

        # region check that only allowed identifiers are used in the query
        available_columns_names = [column.lower() for column in log_table.columns]

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

        # region TEMP
        alias = query.from_table.alias or Identifier(query.from_table.parts[-1])
        query.from_table = f'({log_table._get_base()}) as {alias}'
        # query.from_table = NativeQuery(query=str(jht._get_base()), integration=Identifier('log'), alias=alias)

        # render_engine = db.engine.name
        # if render_engine == "postgresql":
        #     'postgres'
        # render = SqlalchemyRender(render_engine)
        # query_str = render.get_string(query, with_failback=False)
        query_str = str(query)
        df = pd.read_sql_query(query_str, db.engine)
        # endregion

        # region cast columns values to proper types
        for column_name, column_type in log_table.types_map.items():
            for df_column_name in df.columns:
                if df_column_name.lower() == column_name.lower() and df[df_column_name].dtype != column_type:
                    df[df_column_name] = df[df_column_name].astype(column_type)
        # endregion

        columns_info = [{
            'name': k,
            'type': v
        } for k, v in df.dtypes.items()]

        return df.to_dict(orient='split')['data'], columns_info
