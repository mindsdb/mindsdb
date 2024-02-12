"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""
import inspect
import re

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Identifier
from mindsdb_sql.planner.steps import (
    ApplyTimeseriesPredictorStep,
    ApplyPredictorRowStep,
    ApplyPredictorStep,
)

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner import query_planner
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.api.executor.utilities.sql import query_df
from mindsdb.interfaces.model.functions import get_model_record
from mindsdb.api.executor.exceptions import (
    UnknownError,
    LogicError,
)
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.fs import create_process_mark, delete_process_mark

from . import steps
from .result_set import ResultSet, Column
from . steps.base import BaseStepCall

superset_subquery = re.compile(r'from[\s\n]*(\(.*\))[\s\n]*as[\s\n]*virtual_table', flags=re.IGNORECASE | re.MULTILINE | re.S)


class SQLQuery:

    step_handlers = {}

    def __init__(self, sql, session, execute=True):
        self.session = session

        self.context = {
            'database': None if session.database == '' else session.database.lower(),
            'row_id': 0
        }

        self.columns_list = None
        self.steps_data = []

        self.planner = None
        self.parameters = []
        self.fetched_data = None

        self.outer_query = None

        if isinstance(sql, str):
            # region workaround for subqueries in superset
            if 'as virtual_table' in sql.lower():
                subquery = re.findall(superset_subquery, sql)
                if isinstance(subquery, list) and len(subquery) == 1:
                    subquery = subquery[0]
                    self.outer_query = sql.replace(subquery, 'dataframe')
                    sql = subquery.strip('()')
            # endregion
            self.query = parse_sql(sql, dialect='mindsdb')
            self.context['query_str'] = sql
        else:
            self.query = sql
            renderer = SqlalchemyRender('mysql')
            try:
                self.context['query_str'] = renderer.get_string(self.query, with_failback=True)
            except Exception:
                self.context['query_str'] = str(self.query)

        self.create_planner()

        if execute:
            self.prepare_query(prepare=False)
            self.execute_query()

    @classmethod
    def register_steps(cls):

        cls.step_handlers = {}
        for _, cl in inspect.getmembers(steps):
            if inspect.isclass(cl) and issubclass(cl, BaseStepCall):
                if cl.bind is not None:
                    step_name = cl.bind.__name__
                    cls.step_handlers[step_name] = cl

    @profiler.profile()
    def create_planner(self):
        databases = self.session.database_controller.get_list()

        predictor_metadata = []

        query_tables = []

        def get_all_query_tables(node, is_table, **kwargs):
            if is_table and isinstance(node, Identifier):
                table_name = node.parts[-1]
                table_version = None
                project_name = self.session.database
                if table_name.isdigit():
                    # is predictor version
                    table_version = int(table_name)
                    table_name = node.parts[-2]
                if table_name != node.parts[0]:
                    project_name = node.parts[0]
                query_tables.append((table_name, table_version, project_name))

        query_traversal(self.query, get_all_query_tables)

        for table_name, table_version, project_name in query_tables:
            args = {
                'name': table_name,
                'project_name': project_name
            }
            if table_version is not None:
                args['active'] = None
                args['version'] = table_version

            model_record = get_model_record(**args)
            if model_record is None:
                continue

            if isinstance(model_record.data, dict) is False or 'error' in model_record.data:
                continue

            ts_settings = model_record.learn_args.get('timeseries_settings', {})
            predictor = {
                'name': table_name,
                'integration_name': project_name,   # integration_name,
                'timeseries': False,
                'id': model_record.id,
                'to_predict': model_record.to_predict,
            }
            if ts_settings.get('is_timeseries') is True:
                window = ts_settings.get('window')
                order_by = ts_settings.get('order_by')
                if isinstance(order_by, list):
                    order_by = order_by[0]
                group_by = ts_settings.get('group_by')
                if isinstance(group_by, list) is False and group_by is not None:
                    group_by = [group_by]
                predictor.update({
                    'timeseries': True,
                    'window': window,
                    'horizon': ts_settings.get('horizon'),
                    'order_by_column': order_by,
                    'group_by_columns': group_by
                })

            predictor['model_types'] = model_record.data.get('dtypes', {})

            predictor_metadata.append(predictor)

        database = None if self.session.database == '' else self.session.database.lower()

        self.context['predictor_metadata'] = predictor_metadata
        self.planner = query_planner.QueryPlanner(
            self.query,
            integrations=databases,
            predictor_metadata=predictor_metadata,
            default_namespace=database,
        )

    def fetch(self, view='list'):
        data = self.fetched_data

        if view == 'dataframe':
            result = data.to_df()
        else:
            result = data.get_records_raw()

        return {
            'success': True,
            'result': result
        }

    def prepare_query(self, prepare=True):
        if prepare:
            # it is prepared statement call
            try:
                for step in self.planner.prepare_steps(self.query):
                    data = self.execute_step(step)
                    step.set_result(data)
                    self.steps_data.append(data)
            except PlanningException as e:
                raise LogicError(e)

            statement_info = self.planner.get_statement_info()

            self.columns_list = []
            for col in statement_info['columns']:
                self.columns_list.append(
                    Column(
                        database=col['ds'],
                        table_name=col['table_name'],
                        table_alias=col['table_alias'],
                        name=col['name'],
                        alias=col['alias'],
                        type=col['type']
                    )
                )

            self.parameters = [
                Column(
                    name=col['name'],
                    alias=col['alias'],
                    type=col['type']
                )
                for col in statement_info['parameters']
            ]

    def execute_query(self, params=None):
        if self.fetched_data is not None:
            # no need to execute
            return

        process_mark = None
        try:
            steps = list(self.planner.execute_steps(params))
            steps_classes = (x.__class__ for x in steps)
            predict_steps = (ApplyPredictorRowStep, ApplyPredictorStep, ApplyTimeseriesPredictorStep)
            if any(s in predict_steps for s in steps_classes):
                process_mark = create_process_mark('predict')
            for step in steps:
                with profiler.Context(f'step: {step.__class__.__name__}'):
                    data = self.execute_step(step)
                step.set_result(data)
                self.steps_data.append(data)
        except PlanningException as e:
            raise LogicError(e)
        except Exception as e:
            raise e
        finally:
            if process_mark is not None:
                delete_process_mark('predict', process_mark)

        # save updated query
        self.query = self.planner.query

        # there was no executing
        if len(self.steps_data) == 0:
            return

        try:
            if self.outer_query is not None:
                # workaround for subqueries in superset. remove it?
                # +++
                # ???

                result = self.steps_data[-1]
                df = result.to_df()

                df2 = query_df(df, self.outer_query)

                result2 = ResultSet().from_df(df2, database='', table_name='')

                self.columns_list = result2.columns
                self.fetched_data = result2

            else:
                result = self.steps_data[-1]
                self.fetched_data = result
        except Exception as e:
            raise UnknownError("error in preparing result query step") from e

        try:
            if hasattr(self, 'columns_list') is False:
                # how it becomes False?
                self.columns_list = self.fetched_data.columns

            if self.columns_list is None:
                self.columns_list = self.fetched_data.columns

            for col in self.fetched_data.find_columns('__mindsdb_row_id'):
                self.fetched_data.del_column(col)

        except Exception as e:
            raise UnknownError("error in column list step") from e

    def execute_step(self, step):
        cls_name = step.__class__.__name__
        handler = self.step_handlers.get(cls_name)
        if handler is None:
            raise UnknownError(f"Unknown step: {cls_name}")

        return handler(self).call(step)


SQLQuery.register_steps()
