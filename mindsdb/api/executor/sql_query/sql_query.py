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
from textwrap import dedent
from typing import Union, Dict

import pandas as pd
from mindsdb_sql_parser import parse_sql, ASTNode

from mindsdb.api.executor.planner.steps import (
    ApplyTimeseriesPredictorStep,
    ApplyPredictorRowStep,
    ApplyPredictorStep,
)

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.api.executor.planner import query_planner

from mindsdb.api.executor.utilities.sql import get_query_models
from mindsdb.interfaces.model.functions import get_model_record
from mindsdb.api.executor.exceptions import (
    BadTableError,
    UnknownError,
    LogicError,
)
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.fs import create_process_mark, delete_process_mark
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.interfaces.query_context.context_controller import query_context_controller
from mindsdb.utilities.context import context as ctx


from . import steps
from .result_set import ResultSet, Column
from . steps.base import BaseStepCall


class SQLQuery:

    step_handlers = {}

    def __init__(self, sql: Union[ASTNode, str], session, execute: bool = True,
                 database: str = None, query_id: int = None, stop_event=None):
        self.session = session

        self.query_id = query_id
        if self.query_id is not None:
            # get sql and database from resumed query
            run_query = query_context_controller.get_query(self.query_id)
            sql = run_query.sql
            database = run_query.database

        if database is not None:
            self.database = database
        else:
            self.database = session.database

        self.context = {
            'database': None if self.database == '' else self.database.lower(),
            'row_id': 0
        }

        self.columns_list = None
        self.steps_data: Dict[int, ResultSet] = {}

        self.planner: query_planner.QueryPlanner = None
        self.parameters = []
        self.fetched_data: ResultSet = None

        self.outer_query = None
        self.run_query = None
        self.stop_event = stop_event

        if isinstance(sql, str):
            self.query = parse_sql(sql)
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

        query_tables = get_query_models(self.query, default_database=self.database)

        for project_name, table_name, table_version in query_tables:
            args = {
                'name': table_name,
                'project_name': project_name
            }
            if table_version is not None:
                args['active'] = None
                args['version'] = table_version

            model_record = get_model_record(**args)
            if model_record is None:
                # check if it is an agent
                try:
                    agent = self.session.agents_controller.get_agent(table_name, project_name)
                except EntityNotExistsError:
                    continue
                if agent is not None:
                    predictor = {
                        'name': table_name,
                        'integration_name': project_name,  # integration_name,
                        'timeseries': False,
                        'id': agent.id,
                        'to_predict': 'answer',
                    }
                    predictor_metadata.append(predictor)

                continue

            if model_record.status == 'error':
                dot_version_str = ''
                and_version_str = ''
                if table_version is not None:
                    dot_version_str = f'.{table_version}'
                    and_version_str = f' and version = {table_version}'

                raise BadTableError(dedent(f'''\
                    The model '{table_name}{dot_version_str}' cannot be used as it is currently in 'error' status.
                    For detailed information about the error, please execute the following command:

                        select error from information_schema.models where name = '{table_name}'{and_version_str};
                '''))

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

        database = None if self.database == '' else self.database.lower()

        self.context['predictor_metadata'] = predictor_metadata
        self.planner = query_planner.QueryPlanner(
            self.query,
            integrations=databases,
            predictor_metadata=predictor_metadata,
            default_namespace=database,
        )

    def prepare_query(self):
        """it is prepared statement call
        """
        try:
            for step in self.planner.prepare_steps(self.query):
                data = self.execute_step(step)
                step.set_result(data)
                self.steps_data[step.step_num] = data
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

    def execute_query(self):
        if self.fetched_data is not None:
            # no need to execute
            return

        try:
            steps = list(self.planner.execute_steps())
        except PlanningException as e:
            raise LogicError(e)

        if self.planner.plan.is_resumable:
            # create query
            if self.query_id is not None:
                self.run_query = query_context_controller.get_query(self.query_id)
            else:
                self.run_query = query_context_controller.create_query(self.context['query_str'], database=self.database)

            if self.planner.plan.is_async and ctx.task_id is None:
                # add to task
                self.run_query.add_to_task()
                # return query info
                # columns in upper case
                rec = {k.upper(): v for k, v in self.run_query.get_info().items()}
                self.fetched_data = ResultSet.from_df(pd.DataFrame([rec]))
                self.columns_list = self.fetched_data.columns
                return
            self.run_query.mark_as_run()

            ctx.run_query_id = self.run_query.record.id

        step_result = None
        process_mark = None
        try:
            steps_classes = (x.__class__ for x in steps)
            predict_steps = (ApplyPredictorRowStep, ApplyPredictorStep, ApplyTimeseriesPredictorStep)
            if any(s in predict_steps for s in steps_classes):
                process_mark = create_process_mark('predict')
            for step in steps:
                with profiler.Context(f'step: {step.__class__.__name__}'):
                    step_result = self.execute_step(step)
                self.steps_data[step.step_num] = step_result
        except Exception as e:
            if self.run_query is not None:
                # set error and place where it stopped
                self.run_query.on_error(e, step.step_num, self.steps_data)
            raise e
        else:
            # mark running query as completed
            if self.run_query is not None:
                self.run_query.finish()
                ctx.run_query_id = None
        finally:
            if process_mark is not None:
                delete_process_mark('predict', process_mark)

        # save updated query
        self.query = self.planner.query

        # there was no executing
        if len(self.steps_data) == 0:
            return

        self.fetched_data = step_result

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

    def execute_step(self, step, steps_data=None):
        cls_name = step.__class__.__name__
        handler = self.step_handlers.get(cls_name)
        if handler is None:
            raise UnknownError(f"Unknown step: {cls_name}")

        return handler(self, steps_data=steps_data).call(step)


SQLQuery.register_steps()
