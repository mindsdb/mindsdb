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

import re
from collections import OrderedDict, defaultdict
import datetime
import time

import duckdb
import pandas as pd
import numpy as np
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    UnaryOperation,
    CreateTable,
    Identifier,
    Constant,
    Select,
    Union,
    Join,
    Star,
    Insert,
    Delete,
)
from mindsdb_sql.planner.steps import (
    ApplyTimeseriesPredictorStep,
    ApplyPredictorRowStep,
    GetPredictorColumns,
    FetchDataframeStep,
    ApplyPredictorStep,
    GetTableColumns,
    LimitOffsetStep,
    MapReduceStep,
    MultipleSteps,
    ProjectStep,
    SaveToTable,
    InsertToTable,
    FilterStep,
    UnionStep,
    JoinStep,
    GroupByStep
)

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner import query_planner
from mindsdb_sql.planner.utils import query_traversal

from mindsdb.api.mysql.mysql_proxy.classes.com_operators import operator_map
import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.utilities.functions import get_column_in_case

from mindsdb.api.mysql.mysql_proxy.utilities import (
    SqlApiException,
    ErKeyColumnDoesNotExist,
    ErNotSupportedYet,
    SqlApiUnknownError,
    ErLogicError,
    ErSqlWrongArguments
)
from mindsdb_sql.parser.ast.base import ASTNode


superset_subquery = re.compile(r'from[\s\n]*(\(.*\))[\s\n]*as[\s\n]*virtual_table', flags=re.IGNORECASE | re.MULTILINE | re.S)


def get_preditor_alias(step, mindsdb_database):
    predictor_name = '.'.join(step.predictor.parts)
    predictor_alias = '.'.join(step.predictor.alias.parts) if step.predictor.alias is not None else predictor_name
    return (mindsdb_database, predictor_name, predictor_alias)


def get_table_alias(table_obj, default_db_name):
    # (database, table, alias)
    if isinstance(table_obj, Identifier):
        if len(table_obj.parts) > 2:
            raise ErSqlWrongArguments(f'Table name must contain no more than 2 parts. Got name: {table_obj.parts}')
        elif len(table_obj.parts) == 1:
            name = (default_db_name, table_obj.parts[0])
        else:
            name = tuple(table_obj.parts)
    else:
        # it is subquery
        name = table_obj.alias.parts[0] or 't'
        name = (default_db_name, name)

    if table_obj.alias is not None:
        name = name + ('.'.join(table_obj.alias.parts),)
    else:
        name = name + (name[1],)
    return name


def get_all_tables(stmt):
    if isinstance(stmt, Union):
        left = get_all_tables(stmt.left)
        right = get_all_tables(stmt.right)
        return left + right

    if isinstance(stmt, Select):
        from_stmt = stmt.from_table
    elif isinstance(stmt, (Identifier, Join)):
        from_stmt = stmt
    elif isinstance(stmt, Insert):
        from_stmt = stmt.table
    elif isinstance(stmt, Delete):
        from_stmt = stmt.table
    else:
        # raise SqlApiException(f'Unknown type of identifier: {stmt}')
        return []

    result = []
    if isinstance(from_stmt, Identifier):
        result.append(from_stmt.parts[-1])
    elif isinstance(from_stmt, Join):
        result.extend(get_all_tables(from_stmt.left))
        result.extend(get_all_tables(from_stmt.right))
    return result


def markQueryVar(where):
    if isinstance(where, BinaryOperation):
        markQueryVar(where.args[0])
        markQueryVar(where.args[1])
    elif isinstance(where, UnaryOperation):
        markQueryVar(where.args[0])
    elif isinstance(where, Constant):
        if str(where.value).startswith('$var['):
            where.is_var = True
            where.var_name = where.value


def unmarkQueryVar(where):
    if isinstance(where, BinaryOperation):
        unmarkQueryVar(where.args[0])
        unmarkQueryVar(where.args[1])
    elif isinstance(where, UnaryOperation):
        unmarkQueryVar(where.args[0])
    elif isinstance(where, Constant):
        if hasattr(where, 'is_var') and where.is_var is True:
            where.value = where.var_name


def replaceQueryVar(where, var_value, var_name):
    if isinstance(where, BinaryOperation):
        replaceQueryVar(where.args[0], var_value, var_name)
        replaceQueryVar(where.args[1], var_value, var_name)
    elif isinstance(where, UnaryOperation):
        replaceQueryVar(where.args[0], var_value, var_name)
    elif isinstance(where, Constant):
        if hasattr(where, 'is_var') and where.is_var is True and where.value == f'$var[{var_name}]':
            where.value = var_value


def join_query_data(target, source):
    target['values'].extend(source['values'])
    target['tables'].extend(source['tables'])
    target['tables'] = list(set(target['tables']))
    for table_name in source['columns']:
        if table_name not in target['columns']:
            target['columns'][table_name] = source['columns'][table_name]
        else:
            target['columns'][table_name].extend(source['columns'][table_name])
            target['columns'][table_name] = list(set(target['columns'][table_name]))


def is_empty_prediction_row(predictor_value):
    "Define empty rows in predictor after JOIN"
    for key in predictor_value:
        if predictor_value[key] is not None and pd.notna(predictor_value[key]):
            return False
    return True


class Column:
    def __init__(self, name=None, alias=None,
                 table_name=None, table_alias=None,
                 type=None, database=None, flags=None,
                 charset=None):
        if alias is None:
            alias = name
        if table_alias is None:
            table_alias = table_name
        self.name = name
        self.alias = alias
        self.table_name = table_name
        self.table_alias = table_alias
        self.type = type
        self.database = database
        self.flags = flags
        self.charset = charset

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'


class SQLQuery():
    def __init__(self, sql, session, execute=True):
        self.session = session
        self.integration = session.integration
        self.database = None if session.database == '' else session.database.lower()
        self.datahub = session.datahub
        self.outer_query = None
        self.row_id = 0
        self.columns_list = None
        self.model_types = {}

        self.mindsdb_database_name = 'mindsdb'

        if isinstance(sql, str):
        # +++ workaround for subqueries in superset
            if 'as virtual_table' in sql.lower():
                subquery = re.findall(superset_subquery, sql)
                if isinstance(subquery, list) and len(subquery) == 1:
                    subquery = subquery[0]
                    self.outer_query = sql.replace(subquery, 'dataframe')
                    sql = subquery.strip('()')
            # ---
            self.query = parse_sql(sql, dialect='mindsdb')
            self.query_str = sql
        else:
            self.query = sql
            renderer = SqlalchemyRender('mysql')
            try:
                self.query_str = renderer.get_string(self.query, with_failback=True)
            except Exception:
                self.query_str = str(self.query)

        self.planner = None
        self.parameters = []
        self.fetched_data = None
        # self._process_query(sql)
        self.create_planner()
        if execute:
            self.prepare_query(prepare=False)
            self.execute_query()

    def create_planner(self):

        integrations_names = self.session.datahub.get_integrations_names()
        integrations_names.append('information_schema')
        integrations_names.append('files')
        integrations_names.append('views')

        predictor_metadata = {}
        predictors = db.session.query(db.Predictor).filter_by(company_id=self.session.company_id)

        query_tables = []

        def get_all_query_tables(node, is_table, **kwargs):
            if is_table and isinstance(node, Identifier):
                query_tables.append(node.parts[-1])

        query_traversal(self.query, get_all_query_tables)

        # get all predictors
        for p in predictors:
            model_name = p.name

            if model_name not in query_tables:
                # skip
                continue

            if isinstance(p.data, dict) and 'error' not in p.data:
                ts_settings = p.learn_args.get('timeseries_settings', {})
                if ts_settings.get('is_timeseries') is True:
                    window = ts_settings.get('window')
                    order_by = ts_settings.get('order_by')[0]
                    group_by = ts_settings.get('group_by')
                    if isinstance(group_by, list) is False and group_by is not None:
                        group_by = [group_by]
                    predictor_metadata[model_name] = {
                        'timeseries': True,
                        'window': window,
                        'horizon': ts_settings.get('horizon'),
                        'order_by_column': order_by,
                        'group_by_columns': group_by
                    }
                else:
                    predictor_metadata[model_name] = {
                        'timeseries': False
                    }
                self.model_types.update(p.data.get('dtypes', {}))

        mindsdb_database_name = 'mindsdb'
        database = None if self.session.database == '' else self.session.database.lower()

        self.planner = query_planner.QueryPlanner(
            self.query,
            integrations=integrations_names,
            predictor_namespace=mindsdb_database_name,
            predictor_metadata=predictor_metadata,
            default_namespace=database
        )

    def fetch(self, datahub, view='list'):
        data = self.fetched_data

        if view == 'list':
            self.result = self._make_list_result_view(data)

        # this is not used
        # elif view == 'dict':
        #     self.result = self._make_dict_result_view(data)
        # else:
        #     raise ErNotSupportedYet('Only "list" and "dict" views supported atm')

        return {
            'success': True,
            'result': self.result
        }

    def _fetch_dataframe_step(self, step):
        dn = self.datahub.get(step.integration)
        query = step.query

        table_alias = get_table_alias(step.query.from_table, self.database)
        # TODO for information_schema we have 'database' = 'mindsdb'

        data, columns_info = dn.query(
            query=query
        )

        # if this is query: execute it
        if isinstance(data, ASTNode):
            subquery = SQLQuery(data, session=self.session)
            return subquery.fetched_data

        columns = [(column['name'], column['name']) for column in columns_info]
        columns.append(('__mindsdb_row_id', '__mindsdb_row_id'))

        for i, row in enumerate(data):
            row['__mindsdb_row_id'] = self.row_id + i
        self.row_id = self.row_id + len(data)

        data = [{(key, key): value for key, value in row.items()} for row in data]
        data = [{table_alias: x} for x in data]

        col_types = {
            column['name']: column['type']
            for column in columns_info
        }
        data = {
            'values': data,
            'columns': {table_alias: columns},
            'tables': [table_alias],
            'types': {table_alias: col_types}
        }
        return data

    def _multiple_steps(self, step):
        data = {
            'values': [],
            'columns': {},
            'tables': []
        }
        for substep in step.steps:
            sub_data = self._fetch_dataframe_step(substep)
            join_query_data(data, sub_data)
        return data

    def _multiple_steps_reduce(self, step, vars):
        if step.reduce != 'union':
            raise ErLogicError(f'Unknown MultipleSteps type: {step.reduce}')

        data = {
            'values': [],
            'columns': {},
            'tables': []
        }

        for var_group in vars:
            for substep in step.steps:
                if isinstance(substep, FetchDataframeStep) is False:
                    raise ErLogicError(f'Wrong step type for MultipleSteps: {step}')
                markQueryVar(substep.query.where)
            for name, value in var_group.items():
                for substep in step.steps:
                    replaceQueryVar(substep.query.where, value, name)
            sub_data = self._multiple_steps(step)
            join_query_data(data, sub_data)

        return data


    def prepare_query(self, prepare=True):
        mindsdb_sql_struct = self.query

        if isinstance(mindsdb_sql_struct, Select):
            # is it query to 'predictors'?
            if (
                isinstance(mindsdb_sql_struct.from_table, Identifier)
                and mindsdb_sql_struct.from_table.parts[-1].lower() == 'predictors'
                and (
                    self.database == 'mindsdb'
                    or mindsdb_sql_struct.from_table.parts[0].lower() == 'mindsdb'
                )
            ):
                dn = self.datahub.get(self.mindsdb_database_name)
                data, columns = dn.get_predictors(mindsdb_sql_struct)
                table_name = ('mindsdb', 'predictors', 'predictors')
                data = [
                    {
                        (key, key): value
                        for key, value in row.items()
                    }
                    for row in data
                ]
                data = [{table_name: x} for x in data]
                self.columns_list = [
                    Column(database='mindsdb',
                           table_name='predictors',
                           name=column_name)
                    for column_name in columns
                ]

                columns = [(column_name, column_name) for column_name in columns]

                self.fetched_data = {
                    'values': data,
                    'columns': {table_name: columns},
                    'tables': [table_name]
                }
                return

            # is it query to 'datasources'?
            if (
                isinstance(mindsdb_sql_struct.from_table, Identifier)
                and mindsdb_sql_struct.from_table.parts[-1].lower() in ('datasources', 'databases')
                and (
                    self.database == 'mindsdb'
                    or mindsdb_sql_struct.from_table.parts[0].lower() == 'mindsdb'
                )
            ):
                dn = self.datahub.get(self.mindsdb_database_name)
                data, columns = dn.get_integrations(mindsdb_sql_struct)
                table_name = ('mindsdb', 'datasources', 'datasources')
                data = [
                    {
                        (key, key): value
                        for key, value in row.items()
                    }
                    for row in data
                ]

                data = [{table_name: x} for x in data]

                self.columns_list = [
                    Column(database='mindsdb',
                           table_name='datasources',
                           name=column_name)
                    for column_name in columns
                ]

                columns = [(column_name, column_name) for column_name in columns]

                self.fetched_data = {
                    'values': data,
                    'columns': {table_name: columns},
                    'tables': [table_name]
                }
                return

        if prepare:
            # it is prepared statement call
            steps_data = []
            try:
                for step in self.planner.prepare_steps(self.query):
                    data = self.execute_step(step, steps_data)
                    step.set_result(data)
                    steps_data.append(data)
            except PlanningException as e:
                raise ErLogicError(e)

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

        steps_data = []
        try:
            for step in self.planner.execute_steps(params):
                data = self.execute_step(step, steps_data)
                step.set_result(data)
                steps_data.append(data)
        except PlanningException as e:
            raise ErLogicError(e)

        # save updated query
        self.query = self.planner.query

        # there was no executing
        if len(steps_data) == 0:
            return

        try:
            if self.outer_query is not None:
                data = []
                # +++
                # ???
                result = []
                for row in steps_data[-1]:
                    data_row = {}
                    for column_record in self.columns_list:
                        table_name = (column_record.database, column_record.table_name, column_record.table_alias)
                        column_name = column_record.name
                        data_row[column_record.alias or column_record.name] = row[table_name][column_name]
                    result.append(data_row)
                # ---
                data = self._make_list_result_view(result)
                df = pd.DataFrame(data)
                result = query_df(df, self.outer_query)

                try:
                    self.columns_list = [
                        Column(database='',
                               table_name='',
                               name=x)
                        for x in result.columns
                    ]
                except Exception:
                    self.columns_list = [
                        Column(database='',
                               table_name='',
                               name=result.name)
                    ]

                # +++ make list result view
                new_result = []
                for row in result.to_dict(orient='records'):
                    data_row = []
                    for column_record in self.columns_list:
                        column_name = column_record.alias or column_record.name
                        data_row.append(row.get(column_name))
                    new_result.append(data_row)
                result = new_result
                # ---

                self.fetched_data = result
            else:
                self.fetched_data = steps_data[-1]
        except Exception as e:
            raise SqlApiUnknownError("error in preparing result quiery step") from e

        try:
            if hasattr(self, 'columns_list') is False:
                self.columns_list = []
                keys = []
                for row in self.fetched_data:
                    for table_key in row:
                        for column_name in row[table_key]:
                            key = (table_key + (column_name, column_name))
                            if key not in keys:
                                keys.append(key)
                                self.columns_list.append(
                                    Column(database=table_key[0],
                                           table_name=table_key[1],
                                           table_alias=table_key[2],
                                           name=column_name)
                                )

            # if there was no 'ProjectStep', then get columns list from last step:
            if self.columns_list is None:
                self.columns_list = []
                if self.fetched_data is not None:
                    for table_name in self.fetched_data['columns']:
                        col_types = self.fetched_data.get('types', {}).get(table_name, {})
                        for column in self.fetched_data['columns'][table_name]:
                            self.columns_list.append(
                                Column(
                                    database=table_name[0],
                                    table_name=table_name[1],
                                    table_alias=table_name[2],
                                    name=column[0],
                                    alias=column[1],
                                    type=col_types.get(column[0])
                                )
                            )

            self.columns_list = [x for x in self.columns_list if x.name != '__mindsdb_row_id']
        except Exception as e:
            raise SqlApiUnknownError("error in column list step") from e

    def execute_step(self, step, steps_data):
        if type(step) == GetPredictorColumns:
            predictor_name = step.predictor.parts[-1]
            dn = self.datahub.get(self.mindsdb_database_name)
            columns = dn.get_table_columns(predictor_name)
            columns = [
                (column_name, column_name) for column_name in columns
            ]
            data = {
                'values': [],
                'columns': {
                    (self.mindsdb_database_name, predictor_name, predictor_name): columns
                },
                'tables': [(self.mindsdb_database_name, predictor_name, predictor_name)]
            }
        elif type(step) == GetTableColumns:
            table = step.table
            dn = self.datahub.get(step.namespace)
            ds_query = Select(from_table=Identifier(table), targets=[Star()])

            data, columns_info = dn.query(ds_query)

            table_alias = (self.database, table, table)

            data = {
                'values': [],
                'columns': {
                    table_alias: columns_info
                },
                'tables': [table_alias]
            }
        elif type(step) == FetchDataframeStep:
            data = self._fetch_dataframe_step(step)
        elif type(step) == UnionStep:
            raise ErNotSupportedYet('Union step is not implemented')
            # TODO add union support
            # left_data = steps_data[step.left.step_num]
            # right_data = steps_data[step.right.step_num]
            # data = left_data + right_data
        elif type(step) == MapReduceStep:
            try:
                if step.reduce != 'union':
                    raise ErLogicError(f'Unknown MapReduceStep type: {step.reduce}')

                step_data = steps_data[step.values.step_num]
                vars = []
                step_data_values = step_data['values']
                for row in step_data_values:
                    var_group = {}
                    vars.append(var_group)
                    for row_data in row.values():
                        for name, value in row_data.items():
                            if name[0] != '__mindsdb_row_id':
                                var_group[name[1] or name[0]] = value

                data = {
                    'values': [],
                    'columns': {},
                    'tables': []
                }
                substep = step.step
                if type(substep) == FetchDataframeStep:
                    query = substep.query
                    for var_group in vars:
                        markQueryVar(query.where)
                        for name, value in var_group.items():
                            replaceQueryVar(query.where, value, name)
                        sub_data = self._fetch_dataframe_step(substep)
                        if len(data['columns']) == 0:
                            data['columns'] = sub_data['columns']
                        if len(data['tables']) == 0:
                            data['tables'] = sub_data['tables']
                        data['values'].extend(sub_data['values'])
                        unmarkQueryVar(query.where)
                elif type(substep) == MultipleSteps:
                    data = self._multiple_steps_reduce(substep, vars)
                else:
                    raise ErLogicError(f'Unknown step type: {step.step}')
            except Exception as e:
                raise SqlApiUnknownError(f'error in map reduce step: {e}') from e
        elif type(step) == MultipleSteps:
            if step.reduce != 'union':
                raise ErNotSupportedYet(f"Only MultipleSteps with type = 'union' is supported. Got '{step.type}'")
            data = None
            for substep in step.steps:
                subdata = self.execute_step(substep, steps_data)
                if data is None:
                    data = subdata
                else:
                    data['values'].extend(subdata['values'])
        elif type(step) == ApplyPredictorRowStep:
            try:
                predictor = '.'.join(step.predictor.parts)
                dn = self.datahub.get(self.mindsdb_database_name)
                where_data = step.row_dict

                data = dn.query(
                    table=predictor,
                    columns=None,
                    where_data=where_data,
                    integration_name=self.session.integration,
                    integration_type=self.session.integration_type
                )

                data = [{(key, key): value for key, value in row.items()} for row in data]

                table_name = get_preditor_alias(step, self.database)
                values = [{table_name: x} for x in data]
                columns = {table_name: []}
                if len(data) > 0:
                    row = data[0]
                    columns[table_name] = list(row.keys())
                # TODO else

                data = {
                    'values': values,
                    'columns': columns,
                    'tables': [table_name]
                }
            except Exception as e:
                if isinstance(e, SqlApiException):
                    raise e
                else:
                    raise SqlApiUnknownError(f'error in apply predictor row step: {e}') from e
        elif type(step) in (ApplyPredictorStep, ApplyTimeseriesPredictorStep):
            try:
                dn = self.datahub.get(self.mindsdb_database_name)
                predictor = '.'.join(step.predictor.parts)
                where_data = []
                for row in steps_data[step.dataframe.step_num]['values']:
                    new_row = {}
                    for table_name in row:
                        keys_intersection = set(new_row) & set(row[table_name])
                        if len(keys_intersection) > 0:
                            raise ErLogicError(
                                f'The predictor got two identical keys from different datasources: {keys_intersection}'
                            )
                        new_row.update(row[table_name])
                    where_data.append(new_row)

                where_data = [{key[1]: value for key, value in row.items()} for row in where_data]

                is_timeseries = self.planner.predictor_metadata[predictor]['timeseries']
                _mdb_forecast_offset = None
                if is_timeseries:
                    if '> LATEST' in self.query_str:
                        # stream mode -- if > LATEST, forecast starts on inferred next timestamp
                        _mdb_forecast_offset = 1
                    elif '= LATEST' in self.query_str:
                        # override: when = LATEST, forecast starts on last provided timestamp instead of inferred next time
                        _mdb_forecast_offset = 0
                    else:
                        # normal mode -- emit a forecast ($HORIZON data points on each) for each provided timestamp
                        _mdb_forecast_offset = None
                    for row in where_data:
                        if '__mdb_forecast_offset' not in row:
                            row['__mdb_forecast_offset'] = _mdb_forecast_offset

                for row in where_data:
                    for key in row:
                        if isinstance(row[key], datetime.date):
                            row[key] = str(row[key])

                table_name = get_preditor_alias(step, self.database)
                columns = {table_name: []}
                if len(where_data) == 0:
                    # no data, don't run predictor
                    cols = dn.get_table_columns(predictor) + ['__mindsdb_row_id']
                    columns[table_name] = [(c, c) for c in cols]
                    values = []
                else:
                    data = dn.query(
                        table=predictor,
                        columns=None,
                        where_data=where_data,
                        integration_name=self.session.integration,
                        integration_type=self.session.integration_type
                    )

                    data = [{(key, key): value for key, value in row.items()} for row in data]

                    values = [{table_name: x} for x in data]

                    if len(data) > 0:
                        row = data[0]
                        columns[table_name] = list(row.keys())
                    # TODO else

                data = {
                    'values': values,
                    'columns': columns,
                    'tables': [table_name],
                    'types': {table_name: self.model_types}
                }
            except Exception as e:
                raise SqlApiUnknownError(f'error in apply predictor step: {e}') from e
        elif type(step) == JoinStep:
            try:
                left_data = steps_data[step.left.step_num]
                right_data = steps_data[step.right.step_num]

                # FIXME https://github.com/mindsdb/mindsdb_sql/issues/136
                # is_timeseries = False
                # if True in [type(step) == ApplyTimeseriesPredictorStep for step in plan.steps]:
                #     right_data = steps_data[step.left.step_num]
                #     left_data = steps_data[step.right.step_num]
                #     is_timeseries = True

                if step.query.condition is not None:
                    raise ErNotSupportedYet('At this moment supported only JOIN without condition')
                if step.query.join_type.upper() not in ('LEFT JOIN', 'JOIN'):
                    raise ErNotSupportedYet('At this moment supported only JOIN and LEFT JOIN')
                if (
                        len(left_data['tables']) != 1 or len(right_data['tables']) != 1
                        or left_data['tables'][0] == right_data['tables'][0]
                ):
                    raise ErNotSupportedYet('At this moment supported only JOIN of two different tables')

                data = {
                    'values': [],
                    'columns': {},
                    'tables': list(set(left_data['tables'] + right_data['tables'])),
                    'types': {}
                }

                for data_part in [left_data, right_data]:
                    for table_name in data_part['columns']:
                        if table_name not in data['columns']:
                            data['columns'][table_name] = data_part['columns'][table_name]
                            # keep types
                            data['types'][table_name] = data_part.get('types', {}).get(table_name, {}).copy()
                        else:
                            data['columns'][table_name].extend(data_part['columns'][table_name])
                            # keep types
                            data['types'][table_name].update(data_part.get('types', {}).get(table_name, {}))
                for table_name in data['columns']:
                    data['columns'][table_name] = list(set(data['columns'][table_name]))

                left_key = left_data['tables'][0]
                right_key = right_data['tables'][0]

                left_columns_map = OrderedDict()
                left_columns_map_reverse = OrderedDict()
                for i, column_name in enumerate(left_data['columns'][left_key]):
                    left_columns_map[f'a{i}'] = column_name
                    left_columns_map_reverse[column_name] = f'a{i}'

                right_columns_map = {}
                right_columns_map_reverse = {}
                for i, column_name in enumerate(right_data['columns'][right_key]):
                    right_columns_map[f'b{i}'] = column_name
                    right_columns_map_reverse[column_name] = f'b{i}'

                left_df_data = []
                for row in left_data['values']:
                    row = row[left_key]
                    left_df_data.append({left_columns_map_reverse[key]: value for key, value in row.items()})

                right_df_data = []
                for row in right_data['values']:
                    row = row[right_key]
                    right_df_data.append({right_columns_map_reverse[key]: value for key, value in row.items()})

                df_a = pd.DataFrame(left_df_data, columns=left_columns_map.keys())
                df_b = pd.DataFrame(right_df_data, columns=right_columns_map.keys())

                a_name = f'a{round(time.time() * 1000)}'
                b_name = f'b{round(time.time() * 1000)}'
                con = duckdb.connect(database=':memory:')
                con.register(a_name, df_a)
                con.register(b_name, df_b)
                resp_df = con.execute(f"""
                    SELECT * FROM {a_name} as ta full join {b_name} as tb
                    ON ta.{left_columns_map_reverse[('__mindsdb_row_id', '__mindsdb_row_id')]}
                     = tb.{right_columns_map_reverse[('__mindsdb_row_id', '__mindsdb_row_id')]}
                """).fetchdf()
                con.unregister(a_name)
                con.unregister(b_name)
                con.close()

                resp_df = resp_df.replace({np.nan: None})
                resp_dict = resp_df.to_dict(orient='records')

                for row in resp_dict:
                    new_row = {left_key: {}, right_key: {}}
                    for key, value in row.items():
                        if key.startswith('a'):
                            new_row[left_key][left_columns_map[key]] = value
                        else:
                            new_row[right_key][right_columns_map[key]] = value
                    data['values'].append(new_row)

                # remove all records with empty data from predictor from join result
                # otherwise there are emtpy records in the final result:
                # +------------+------------+-------+-----------+----------+
                # | time       | time       | state | pnew_case | new_case |
                # +------------+------------+-------+-----------+----------+
                # | 2020-10-21 | 2020-10-24 | CA    | 0.0       | 5945.0   |
                # | 2020-10-22 | 2020-10-23 | CA    | 0.0       | 6141.0   |
                # | 2020-10-23 | 2020-10-22 | CA    | 0.0       | 2940.0   |
                # | 2020-10-24 | 2020-10-21 | CA    | 0.0       | 3707.0   |
                # | NULL       | 2020-10-20 | NULL  | nan       | nan      |
                # | NULL       | 2020-10-19 | NULL  | nan       | nan      |
                # | NULL       | 2020-10-18 | NULL  | nan       | nan      |
                # | NULL       | 2020-10-17 | NULL  | nan       | nan      |
                # | NULL       | 2020-10-16 | NULL  | nan       | nan      |
                # +------------+------------+-------+-----------+----------+
                # 9 rows in set (2.07 sec)

                # if is_timeseries:
                #     data_values = []
                #     for row in data['values']:
                #         for key in row:
                #             if 'mindsdb' in key:
                #                 if not is_empty_prediction_row(row[key]):
                #                     data_values.append(row)
                #                     break
                #     data['values'] = data_values
            except Exception as e:
                raise SqlApiUnknownError(f'error in join step: {e}') from e

        elif type(step) == FilterStep:
            step_data = steps_data[step.dataframe.step_num]

            # dicts to look up column and table
            column_idx = {}
            tables_idx = {}
            col_table_idx = {}

            # prepare columns for dataframe. column name contains table name
            cols = set()
            for table, col_list in step_data['columns'].items():
                _, t_name, t_alias = table

                tables_idx[t_name] = t_name
                tables_idx[t_alias] = t_name
                for column in col_list:
                    # table_column
                    c_name, c_alias = column

                    col_name = f'{t_name}^{c_name}'
                    cols.add(col_name)

                    col_table_idx[col_name] = (table, column)
                    column_idx[c_name] = t_name

            # prepare dict for dataframe
            result = []
            for row in step_data['values']:
                data_row = {}
                for table, col_list in step_data['columns'].items():
                    for col in col_list:
                        col_name = f'{table[1]}^{col[0]}'
                        data_row[col_name] = row[table][col]
                result.append(data_row)

            df = pd.DataFrame(result, columns=list(cols))

            # analyze condition and change name of columns
            def check_fields(node, is_table=None, **kwargs):
                if is_table:
                    raise ErNotSupportedYet('Subqueries is not supported in WHERE')
                if isinstance(node, Identifier):
                    # only column name
                    col_name = node.parts[-1]

                    if len(node.parts) == 1:
                        if col_name not in column_idx:
                            raise ErKeyColumnDoesNotExist(f'Table not found for column: {col_name}')
                        table_name = column_idx[col_name]
                    else:
                        table_name = node.parts[-2]

                        # maybe it is alias
                        table_name = tables_idx[table_name]

                    new_name = f'{table_name}^{col_name}'
                    return Identifier(parts=[new_name])

            where_query = step.query
            query_traversal(where_query, check_fields)

            query = Select(targets=[Star()], from_table=Identifier('df'), where=where_query)

            res = query_df(df, query)

            resp_dict = res.to_dict(orient='records')

            # convert result to dict-dict structure
            values = []
            for row in resp_dict:
                value = {}
                for key, v in row.items():
                    # find original table and col
                    table, column = col_table_idx[key]
                    if table not in value:
                        value[table] = {}
                    value[table][column] = v

                values.append(value)

            data = {
                'tables': step_data['tables'],
                'columns': step_data['columns'],
                'values': values,
                'types': step_data.get('types', {})
            }

        elif type(step) == LimitOffsetStep:
            try:
                step_data = steps_data[step.dataframe.step_num]
                data = {
                    'values': step_data['values'].copy(),
                    'columns': step_data['columns'].copy(),
                    'tables': step_data['tables'].copy(),
                    'types': step_data.get('types', {}).copy(),
                }
                if isinstance(step.offset, Constant) and isinstance(step.offset.value, int):
                    data['values'] = data['values'][step.offset.value:]
                if isinstance(step.limit, Constant) and isinstance(step.limit.value, int):
                    data['values'] = data['values'][:step.limit.value]
            except Exception as e:
                raise SqlApiUnknownError(f'error in limit offset step: {e}') from e
        elif type(step) == ProjectStep:
            try:
                step_data = steps_data[step.dataframe.step_num]

                columns = defaultdict(list)
                for column_identifier in step.columns:

                    if type(column_identifier) == Star:
                        for table_name, table_columns_list in step_data['columns'].items():
                            for column in table_columns_list:

                                columns[table_name].append(column)

                    elif type(column_identifier) == Identifier:
                        appropriate_table = None
                        columns_to_copy = None

                        column_name_parts = column_identifier.parts
                        column_alias = column_identifier.parts[-1] if column_identifier.alias is None else '.'.join(
                            column_identifier.alias.parts)
                        if len(column_name_parts) > 2:
                            raise ErSqlWrongArguments(
                                f'Column name must contain no more than 2 parts. Got name: {column_identifier}')
                        elif len(column_name_parts) == 1:
                            column_name = column_name_parts[0]

                            for table_name, table_columns in step_data['columns'].items():
                                table_col_idx = {}
                                for x in table_columns:
                                    name = x[1] or x[0]
                                    table_col_idx[name] = x

                                column_exists = get_column_in_case(list(table_col_idx.keys()), column_name)
                                if column_exists:
                                    if appropriate_table is not None and not step.ignore_doubles:
                                        raise ErLogicError(
                                            f'Found multiple appropriate tables for column {column_name}')
                                    else:
                                        appropriate_table = table_name
                                        new_col = (column_name, column_alias)
                                        cur_col = table_col_idx[column_exists]

                                        columns[appropriate_table].append(new_col)
                                        if cur_col != new_col:
                                            columns_to_copy = cur_col, new_col
                                        break

                            if appropriate_table is None:
                                raise SqlApiException(f'Can not find appropriate table for column {column_name}')

                        elif len(column_name_parts) == 2:
                            table_name_or_alias = column_name_parts[0]
                            column_name = column_name_parts[1]

                            for table_name, table_columns in step_data['columns'].items():
                                checking_table_name_or_alias = table_name[2] or table_name[1]
                                if table_name_or_alias.lower() == checking_table_name_or_alias.lower():
                                    # support select table.*
                                    if isinstance(column_name, Star):
                                        # add all by table
                                        appropriate_table = table_name
                                        for column in step_data['columns'][appropriate_table]:
                                            columns[appropriate_table].append(column)
                                        break

                                    table_col_idx = {}
                                    for x in table_columns:
                                        name = x[1] or x[0]
                                        table_col_idx[name] = x

                                    column_exists = get_column_in_case(list(table_col_idx.keys()), column_name)
                                    if column_exists:
                                        appropriate_table = table_name
                                        new_col = (column_name, column_alias)
                                        cur_col = table_col_idx[column_exists]

                                        columns[appropriate_table].append(new_col)
                                        if cur_col != new_col:
                                            columns_to_copy = cur_col, new_col

                                        break
                                    else:
                                        raise ErLogicError(f'Can not find column "{column_name}" in table "{table_name}"')
                            if appropriate_table is None:
                                raise ErLogicError(f'Can not find appropriate table for column {column_name}')
                        else:
                            raise ErSqlWrongArguments('Undefined column name')

                        if columns_to_copy is not None:
                            col_from, col_to = columns_to_copy
                            for row in step_data['values']:
                                row[appropriate_table][col_to] = row[appropriate_table][col_from]
                            # TODO copy types?

                    else:
                        raise ErKeyColumnDoesNotExist(f'Unknown column type: {column_identifier}')

                data = {
                    'tables': step_data['tables'],
                    'columns': columns,
                    'values': step_data['values'],  # TODO keep values only for columns
                    'types': step_data.get('types', {})
                }
            except Exception as e:
                if isinstance(e, SqlApiException):
                    raise e
                raise SqlApiUnknownError(f'error on project step: {e} ') from e
        elif type(step) == GroupByStep:
            step_data = steps_data[step.dataframe.step_num]

            result = []
            cols = set()
            for _, col_list in step_data['columns'].items():
                for col in col_list:
                    cols.add(col[0])

            for row in step_data['values']:
                data_row = {}
                for table, col_list in step_data['columns'].items():
                    for col in col_list:
                        data_row[col[0]] = row[table][col]
                result.append(data_row)

            df = pd.DataFrame(result, columns=list(cols))

            query = Select(targets=step.targets, from_table='df', group_by=step.columns).to_string()
            res = query_df(df, query)

            resp_dict = res.to_dict(orient='records')

            # stick all columns to first table
            appropriate_table = step_data['tables'][0]

            columns_list = []
            columns = []
            for key, dtyp in res.dtypes.items():
                columns_list.append(
                    Column(database=appropriate_table[0],
                           table_name=appropriate_table[1],
                           table_alias=appropriate_table[2],
                           name=key,
                           type=dtyp,
                           alias=key)
                )
                columns.append((key, key))

            values = []
            for row in resp_dict:
                row2 = {
                    (key, key): value
                    for key, value in row.items()
                }

                values.append(
                    {
                        appropriate_table: row2
                    }
                )

            # columns are changed
            self.columns_list = columns_list
            types = step_data.get('types', {})
            data = {
                'tables': [appropriate_table],
                'columns': {appropriate_table: columns},
                'values': values,
                'types': types  # copy
            }

        elif type(step) == SaveToTable or type(step) == InsertToTable:
            is_replace = False
            is_create = False

            if type(step) == SaveToTable:
                is_create = True

                if step.is_replace:
                    is_replace = True

            step_data = step.dataframe.result_data
            integration_name = step.table.parts[0]
            table_name_parts = step.table.parts[1:]

            dn = self.datahub.get(integration_name)

            if hasattr(dn, 'create_table') is False:
                raise ErNotSupportedYet(f"Creating table in '{integration_name}' is not supporting")

            # region del 'service' columns
            for table in step_data['columns']:
                new_table_columns = []
                for column in step_data['columns'][table]:
                    if column[-1] not in ('__mindsdb_row_id', '__mdb_forecast_offset'):
                        new_table_columns.append(column)
                step_data['columns'][table] = new_table_columns
            # endregion

            # region del columns filtered at projection step
            if self.columns_list is not None:
                filtered_column_names = [x.name for x in self.columns_list]
                for table in step_data['columns']:
                    new_table_columns = []
                    for column in step_data['columns'][table]:
                        if column[0].startswith('predictor.'):
                            new_table_columns.append(column)
                        elif column[0] in filtered_column_names:
                            new_table_columns.append(column)
                    step_data['columns'][table] = new_table_columns
            # endregion

            # drop double names
            col_names = set()
            if len(step_data['tables']) > 1:
                # set prefixes for column if it doubled

                for table in step_data['tables']:
                    table_name = table[1]
                    col_map = []
                    col_list = []
                    for column in step_data['columns'][table]:
                        alias = column[1]
                        if alias not in col_names:
                            col_names.add(alias)
                            col_list.append(column)
                        else:
                            column_new = (f'{table_name}.{column[0]}', f'{table_name}.{column[1]}')
                            col_list.append(column_new)
                            col_map.append([column, column_new])

                    # replace columns
                    step_data['columns'][table] = col_list

                    # replace in values
                    for row in step_data['values']:
                        table_row = row[table]

                        for column, column_new in col_map:
                            table_row[column_new] = table_row.pop(column)

            dn.create_table(
                table_name_parts=table_name_parts,
                columns=step_data['columns'],
                data=step_data['values'],
                is_replace=is_replace,
                is_create=is_create
            )
            data = None
        else:
            raise ErLogicError(F'Unknown planner step: {step}')
        return data

    # Is not used
    # def _apply_where_filter(self, row, where):
    #     if isinstance(where, Identifier):
    #         return row[where.value]
    #     elif isinstance(where, Constant):
    #         return where.value
    #     elif not isinstance(where, (UnaryOperation, BinaryOperation)):
    #         raise SqlApiException(f'Unknown operation type: {where}')
    #
    #     op_fn = operator_map.get(where.op)
    #     if op_fn is None:
    #         raise SqlApiException(f'unknown operator {where.op}')
    #
    #     args = [self._apply_where_filter(row, arg) for arg in where.args]
    #     result = op_fn(*args)
    #     return result

    def _make_list_result_view(self, data):
        if self.outer_query is not None:
            return data['values']
        result = []
        for row in data['values']:
            data_row = []
            for column_record in self.columns_list:
                table_name = (column_record.database, column_record.table_name, column_record.table_alias)
                column_name = (column_record.name, column_record.alias)
                if table_name in row is False:
                    # try without alias
                    table_name = (table_name[0], table_name[1], None)

                data_row.append(row[table_name][column_name])
            result.append(data_row)
        return result

    def _make_dict_result_view(self, data):
        result = []
        for row in data['values']:
            data_row = {}
            for table_name in row:
                data_row.update(row[table_name])
            result.append(data_row)
        return result
