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
import hashlib
import datetime as dt

import dateinfer
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
    Update,
    Delete,
    Latest,
    BetweenOperation,
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
    UpdateToTable,
    FilterStep,
    UnionStep,
    JoinStep,
    GroupByStep,
    SubSelectStep,
)

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner import query_planner
from mindsdb_sql.planner.utils import query_traversal

import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.api.mysql.mysql_proxy.utilities.functions import get_column_in_case
from mindsdb.interfaces.model.functions import (
    get_model_records,
    get_predictor_integration
)
from mindsdb.api.mysql.mysql_proxy.utilities import (
    SqlApiException,
    ErKeyColumnDoesNotExist,
    ErNotSupportedYet,
    SqlApiUnknownError,
    ErLogicError,
    ErSqlWrongArguments
)
from mindsdb.utilities.cache import get_cache, json_checksum

from mindsdb_sql.parser.ast.base import ASTNode

superset_subquery = re.compile(r'from[\s\n]*(\(.*\))[\s\n]*as[\s\n]*virtual_table', flags=re.IGNORECASE | re.MULTILINE | re.S)

predictor_cache = get_cache('predict')


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
    elif isinstance(table_obj, Select):
        # it is subquery
        if table_obj.alias is None:
            name = 't'
        else:
            name = table_obj.alias.parts[0]
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


class ResultSet:
    def __init__(self):
        self.columns = []
        # records is list of lists with the same length as columns
        self.records = []

    def from_step_data(self, step_data):

        for table, col_list in step_data['columns'].items():
            for col in col_list:
                type = None
                if 'types' in step_data:
                    type = step_data['types'].get(table, {}).get(col[0])
                self.columns.append(Column(
                    name=col[0],
                    alias=col[1],
                    type=type,
                    table_name=table[1],
                    table_alias=table[2],
                    database=table[0]
                ))

        for row in step_data['values']:
            data_row = []
            for col in self.columns:
                col_key = (col.name, col.alias)
                table_key = (col.database, col.table_name, col.table_alias)
                val = row[table_key][col_key]

                data_row.append(val)

            self.records.append(data_row)

    def from_df(self, df, database, table_name):

        resp_dict = df.to_dict(orient='split')

        self.records = resp_dict['data']

        for col in resp_dict['columns']:
            self.columns.append(Column(
                name=col,
                table_name=table_name,
                database=database,
                type=df.dtypes[col]
            ))

    def to_df(self):
        columns = [
            col.name if col.alias is None else col.alias
            for col in self.columns
        ]

        return pd.DataFrame(self.records, columns=columns)

    def to_step_data(self):
        step_data = {
            'values': [],
            'columns': {},
            'types': {},
            'tables': []
        }

        for col in self.columns:
            col_key = (col.name, col.alias)
            table_key = (col.database, col.table_name, col.table_alias)

            if not table_key in step_data['tables']:
                step_data['tables'].append(table_key)
                step_data['columns'][table_key] = []
                step_data['types'][table_key] = {}

            step_data['columns'][table_key].append(col_key)
            if col.type is not None:
                step_data['types'][table_key][col.name] = col.type

        for rec in self.records:
            row = {}
            for table in step_data['tables']:
                row[table] = {}
            for i, col in enumerate(self.columns):
                col_key = (col.name, col.alias)
                table_key = (col.database, col.table_name, col.table_alias)

                row[table_key][col_key] = rec[i]

            step_data['values'].append(row)
        return step_data

    def clear_records(self):
        self.records = []

    def replace_records(self, records):
        self.clear_records()
        for rec in records:
            self.add_record(rec)

    def add_record(self, rec):
        if len(rec) != len(self.columns):
            raise ErSqlWrongArguments(f'Record length mismatch columns length: {len(rec)} != {len(self.columns)}')
        self.records.append(rec)


class SQLQuery():
    def __init__(self, sql, session, execute=True):
        self.session = session
        self.database = None if session.database == '' else session.database.lower()
        self.datahub = session.datahub
        self.outer_query = None
        self.row_id = 0
        self.columns_list = None
        self.model_types = {}

        self.mindsdb_database_name = 'mindsdb'

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
        integrations_meta = self.session.integration_controller.get_all()
        integrations_names = list(integrations_meta.keys())
        integrations_names.append('information_schema')
        integration_name = None

        predictor_metadata = []
        predictors_records = get_model_records(company_id=self.session.company_id)

        query_tables = []

        def get_all_query_tables(node, is_table, **kwargs):
            if is_table and isinstance(node, Identifier):
                query_tables.append(node.parts[-1])

        query_traversal(self.query, get_all_query_tables)

        for p in predictors_records:
            model_name = p.name

            if model_name not in query_tables:
                continue

            integration_name = None
            integration_record = get_predictor_integration(p)
            if integration_record is not None:
                integration_name = integration_record.name

            if isinstance(p.data, dict) and 'error' not in p.data:
                ts_settings = p.learn_args.get('timeseries_settings', {})
                predictor = {
                    'name': model_name,
                    'integration_name': integration_name,
                    'timeseries': False,
                    'id': p.id
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
                predictor_metadata.append(predictor)
                if predictor['integration_name'] == 'lightwood':
                    default_predictor = predictor.copy()
                    default_predictor['integration_name'] = 'mindsdb'
                    predictor_metadata.append(default_predictor)

                self.model_types.update(p.data.get('dtypes', {}))

        database = None if self.session.database == '' else self.session.database.lower()

        self.predictor_metadata = predictor_metadata
        self.planner = query_planner.QueryPlanner(
            self.query,
            integrations=integrations_names,
            predictor_namespace=integration_name,
            predictor_metadata=predictor_metadata,
            default_namespace=database
        )

    def fetch(self, view='list'):
        data = self.fetched_data

        if view == 'dataframe':
            result = self._make_list_result_view(data)
            col_names = [
                col.alias if col.alias is not None else col.name
                for col in self.columns_list
            ]
            result = pd.DataFrame(result, columns=col_names)
        else:
            result = self._make_list_result_view(data)

        # this is not used
        # elif view == 'dict':
        #     self.result = self._make_dict_result_view(data)
        # else:
        #     raise ErNotSupportedYet('Only "list" and "dict" views supported atm')

        return {
            'success': True,
            'result': result
        }

    def _fetch_dataframe_step(self, step):
        dn = self.datahub.get(step.integration)
        query = step.query

        if query is None:
            table_alias = (self.database, 'result', 'result')

            # fetch raw_query
            data, columns_info = dn.query(
                native_query=step.raw_query
            )
        else:

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

            if (
                isinstance(mindsdb_sql_struct.from_table, Identifier)
                and (
                    self.database == 'mindsdb'
                    or mindsdb_sql_struct.from_table.parts[0].lower() == 'mindsdb'
                )
            ):
                if mindsdb_sql_struct.from_table.parts[-1].lower() == 'predictors':
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
                        Column(
                            database='mindsdb',
                            table_name='predictors',
                            name=column_name
                        )
                        for column_name in columns
                    ]

                    columns = [(column_name, column_name) for column_name in columns]

                    self.fetched_data = {
                        'values': data,
                        'columns': {table_name: columns},
                        'tables': [table_name]
                    }
                    return
                elif mindsdb_sql_struct.from_table.parts[-1].lower() == 'predictors_versions':
                    dn = self.datahub.get(self.mindsdb_database_name)
                    data, columns = dn.get_predictors_versions(mindsdb_sql_struct)
                    table_name = ('mindsdb', 'predictors_versions', 'predictors_versions')
                    data = [
                        {
                            (key, key): value
                            for key, value in row.items()
                        }
                        for row in data
                    ]
                    data = [{table_name: x} for x in data]
                    self.columns_list = [
                        Column(
                            database='mindsdb',
                            table_name='predictors_versions',
                            name=column_name
                        )
                        for column_name in columns
                    ]

                    columns = [(column_name, column_name) for column_name in columns]

                    self.fetched_data = {
                        'values': data,
                        'columns': {table_name: columns},
                        'tables': [table_name]
                    }
                    return
                elif mindsdb_sql_struct.from_table.parts[-1].lower() in ('datasources', 'databases'):
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
                        Column(
                            database='mindsdb',
                            table_name='datasources',
                            name=column_name
                        )
                        for column_name in columns
                    ]

                    columns = [(column_name, column_name) for column_name in columns]

                    self.fetched_data = {
                        'values': data,
                        'columns': {table_name: columns},
                        'tables': [table_name]
                    }
                    return

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
            left_result = ResultSet()
            right_result = ResultSet()

            left_result.from_step_data(steps_data[step.left.step_num])
            right_result.from_step_data(steps_data[step.right.step_num])

            # count of columns have to match
            if len(left_result.columns) != len(right_result.columns):
                raise ErSqlWrongArguments(
                    f'UNION columns count mismatch: {len(left_result.columns)} != {len(right_result.columns)} ')

            # types have to match
            # TODO: return checking type later
            # for i, left_col in enumerate(left_result.columns):
            #     right_col = right_result.columns[i]
            #     type1, type2 = left_col.type, right_col.type
            #     if type1 is not None and type2 is not None:
            #         if type1 != type2:
            #             raise ErSqlWrongArguments(f'UNION types mismatch: {type1} != {type2}')

            records = []
            records_hashes = []
            for rec in left_result.records + right_result.records:
                if step.unique:
                    checksum = hashlib.sha256(str(rec).encode()).hexdigest()
                    if checksum in records_hashes:
                        continue
                    records_hashes.append(checksum)
                records.append(rec)

            left_result.replace_records(records)

            data = left_result.to_step_data()

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
                ml_handler_name = step.namespace
                predictor_name = step.predictor.parts[0]

                dn = self.datahub.get(self.mindsdb_database_name)
                where_data = step.row_dict

                data = dn.query(
                    table=predictor_name,
                    where_data=where_data,
                    ml_handler_name=ml_handler_name
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
                # set row_id
                data = steps_data[step.dataframe.step_num]
                row_id_col = ('__mindsdb_row_id', '__mindsdb_row_id')
                for table in data['columns']:
                    data['columns'][table].append(row_id_col)

                row_count = len(data['values'])

                for i, row in enumerate(data['values']):
                    for n, table_name in enumerate(row):
                        row[table_name][row_id_col] = self.row_id + i + n * row_count
                # shift counter
                self.row_id += self.row_id + row_count * len(data['tables'])

                ml_handler_name = step.namespace
                if ml_handler_name == 'mindsdb':
                    ml_handler_name = 'lightwood'
                predictor_name = step.predictor.parts[0]
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

                predictor_metadata = {}
                for pm in self.predictor_metadata:
                    if pm['name'] == predictor_name and pm['integration_name'].lower() == ml_handler_name:
                        predictor_metadata = pm
                        break
                is_timeseries = predictor_metadata['timeseries']
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

                # for row in where_data:
                #     for key in row:
                #         if isinstance(row[key], datetime.date):
                #             row[key] = str(row[key])

                table_name = get_preditor_alias(step, self.database)
                columns = {table_name: []}
                dn = self.datahub.get(self.mindsdb_database_name)
                if len(where_data) == 0:
                    cols = dn.get_table_columns(predictor_name) + ['__mindsdb_row_id']
                    columns[table_name] = [(c, c) for c in cols]
                    values = []
                else:
                    predictor_id = predictor_metadata['id']
                    key = f'{predictor_name}_{predictor_id}_{json_checksum(where_data)}'
                    data = predictor_cache.get(key)

                    if data is None:
                        data = dn.query(
                            table=predictor_name,
                            where_data=where_data,
                            ml_handler_name=ml_handler_name
                        )
                        if data is not None and isinstance(data, list):
                            predictor_cache.set(key, data)

                    if len(data) > 0:
                        row = data[0]
                        columns[table_name] = [(key, key) for key in row.keys()]

                    # apply filter
                    if is_timeseries:
                        data = self.apply_ts_filter(data, where_data, step, predictor_metadata)

                    data = [{(key, key): value for key, value in row.items()} for row in data]

                    values = [{table_name: x} for x in data]


                data = {
                    'values': values,
                    'columns': columns,
                    'tables': [table_name],
                    'types': {table_name: self.model_types},
                    'is_prediction': True  # for join step
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

                if len(left_data['tables']) == 0 or len(right_data['tables']) == 0:
                    raise ErLogicError('Table for join is not found')

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

                a_name = f'table_a'
                b_name = f'table_b'
                con = duckdb.connect(database=':memory:')
                con.register(a_name, df_a)
                con.register(b_name, df_b)

                join_type = step.query.join_type.lower()
                if join_type == 'join':
                    # join type is not specified. using join to prediction data
                    if left_data.get('is_prediction'):
                        join_type = 'left join'
                    elif right_data.get('is_prediction'):
                        join_type = 'right join'

                resp_df = con.execute(f"""
                    SELECT * FROM {a_name} as ta {join_type} {b_name} as tb
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
                    cols.add(col[1])

            for row in step_data['values']:
                data_row = {}
                for table, col_list in step_data['columns'].items():
                    for col in col_list:
                        data_row[col[1]] = row[table][col]
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

        elif type(step) == SubSelectStep:

            step_data = steps_data[step.dataframe.step_num]

            table_name = step.table_name
            if table_name is None:
                table_name = 'df_table'
            else:
                table_name = table_name

            result = ResultSet()
            result.from_step_data(step_data)

            query = step.query
            query.from_table = Identifier('df_table')

            df = result.to_df()
            res = query_df(df, query)

            result2 = ResultSet()
            # get database from first column
            database = result.columns[0].database
            result2.from_df(res, database, table_name)

            data = result2.to_step_data()


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
        elif type(step) == UpdateToTable:

            step_data = step.dataframe.result_data
            integration_name = step.table.parts[0]
            table_name_parts = step.table.parts[1:]

            dn = self.datahub.get(integration_name)

            result = ResultSet()
            result.from_step_data(step_data)

            # link nodes with parameters for fast replacing with values
            input_table_alias = step.update_command.from_select_alias.parts[0]

            params_map_index = []

            def prepare_map_index(node, is_table, **kwargs):
                if isinstance(node, Identifier) and not is_table:
                    # is input table field
                    if node.parts[0] == input_table_alias:
                        node2 = Constant(None)
                        param_name = node.parts[-1]
                        params_map_index.append([param_name, node2])
                        # replace node with constant
                        return node2
                    elif node.parts[0] == table_name_parts[0]:
                        # remove updated table alias
                        node.parts = node.parts[1:]

            # make command
            update_query = Update(
                table=Identifier(parts=table_name_parts),
                update_columns=step.update_command.update_columns,
                where=step.update_command.where
            )
            # do mapping
            query_traversal(update_query, prepare_map_index)

            # check all params is input data:
            data_header = [col.alias for col in result.columns]

            for param_name, _ in params_map_index:
                if param_name not in data_header:
                    raise ErSqlWrongArguments(f'Field {param_name} not found in input data. Input fields: {data_header}')

            # perform update
            for values in result.records:
                # run update from every row from input data
                row = dict(zip(data_header, values))

                # fill params:
                for param_name, param in params_map_index:
                    param.value = row[param_name]

                # execute
                dn.query(query=update_query)

            data = None
        else:
            raise ErLogicError(F'Unknown planner step: {step}')
        return data

    def apply_ts_filter(self, predictor_data, table_data, step, predictor_metadata):

        if step.output_time_filter is None:
            # no filter, exit
            return predictor_data

            # apply filter
        group_cols = predictor_metadata['group_by_columns']
        order_col = predictor_metadata['order_by_column']

        filter_args = step.output_time_filter.args
        filter_op = step.output_time_filter.op

        # filter field must be order column
        if not (
            isinstance(filter_args[0], Identifier)
            and filter_args[0].parts[-1] == order_col
        ):
            # exit otherwise
            return predictor_data


        def get_date_format(samples):
            # dateinfer reads sql date 2020-04-01 as yyyy-dd-mm. workaround for in
            for date_format, pattern in (
                    ('%Y-%m-%d', '[\d]{4}-[\d]{2}-[\d]{2}'),
                    # ('%Y', '[\d]{4}')
            ):
                if re.match(pattern, samples[0]):
                    # suggested format
                    for sample in samples:
                        try:
                            dt.datetime.strptime(sample, date_format)
                        except ValueError:
                            date_format = None
                            break
                    if date_format is not None:
                        return date_format

            return dateinfer.infer(samples)

        if self.model_types.get(order_col) in ('float', 'integer'):
            # convert strings to digits
            fnc = {
                'integer': int,
                'float': float
            }[self.model_types[order_col]]


            # convert predictor_data
            if len(predictor_data) > 0:
                if isinstance(predictor_data[0][order_col], str):

                    for row in predictor_data:
                        row[order_col] = fnc(row[order_col])
                elif isinstance(predictor_data[0][order_col], dt.date):
                    # convert to datetime
                    for row in predictor_data:
                        row[order_col] = fnc(row[order_col])

            # convert predictor_data
            if isinstance(table_data[0][order_col], str):

                for row in table_data:
                    row[order_col] = fnc(row[order_col])
            elif isinstance(table_data[0][order_col], dt.date):
                # convert to datetime
                for row in table_data:
                    row[order_col] = fnc(row[order_col])

            # convert args to date
            samples = [
                arg.value
                for arg in filter_args
                if isinstance(arg, Constant) and isinstance(arg.value, str)
            ]
            if len(samples) > 0:

                for arg in filter_args:
                    if isinstance(arg, Constant) and isinstance(arg.value, str):
                        arg.value = fnc(arg.value)

        if self.model_types.get(order_col) in ('date', 'datetime'):
            # convert strings to date
            # it is making side effect on original data by changing it but let it be

            # convert predictor_data
            if len(predictor_data) > 0:
                if isinstance(predictor_data[0][order_col], str):
                    samples = [row[order_col] for row in predictor_data]
                    date_format = get_date_format(samples)

                    for row in predictor_data:
                        row[order_col] = dt.datetime.strptime(row[order_col], date_format)
                elif isinstance(predictor_data[0][order_col], dt.date):
                    # convert to datetime
                    for row in predictor_data:
                        row[order_col] = dt.datetime.combine(row[order_col], dt.datetime.min.time())

            # convert predictor_data
            if isinstance(table_data[0][order_col], str):
                samples = [row[order_col] for row in table_data]
                date_format = get_date_format(samples)

                for row in table_data:
                    row[order_col] = dt.datetime.strptime(row[order_col], date_format)
            elif isinstance(table_data[0][order_col], dt.date):
                # convert to datetime
                for row in table_data:
                    row[order_col] = dt.datetime.combine(row[order_col], dt.datetime.min.time())

            # convert args to date
            samples = [
                arg.value
                for arg in filter_args
                if isinstance(arg, Constant) and isinstance(arg.value, str)
            ]
            if len(samples) > 0:
                date_format = get_date_format(samples)

                for arg in filter_args:
                    if isinstance(arg, Constant) and isinstance(arg.value, str):
                        arg.value = dt.datetime.strptime(arg.value, date_format)
            # TODO can be dt.date in args?

        # first pass: get max values for Latest in table data
        latest_vals = {}
        if Latest() in filter_args:

            for row in table_data:
                key = tuple([str(row[i]) for i in group_cols])
                val = row[order_col]
                if key not in latest_vals or latest_vals[key] < val:
                    latest_vals[key] = val

        # second pass: do filter rows
        data2 = []
        for row in predictor_data:
            val = row[order_col]

            if isinstance(step.output_time_filter, BetweenOperation):
                if val >= filter_args[1].value and val <= filter_args[2].value:
                    data2.append(row)
            elif isinstance(step.output_time_filter, BinaryOperation):
                op_map = {
                    '<': '__lt__',
                    '<=': '__le__',
                    '>': '__gt__',
                    '>=': '__ge__',
                    '=': '__eq__',
                }
                arg = filter_args[1]
                if isinstance(arg, Latest):
                    key = tuple([str(row[i]) for i in group_cols])
                    if key not in latest_vals:
                        # pass this row
                        continue
                    arg = latest_vals[key]
                elif isinstance(arg, Constant):
                    arg = arg.value

                if filter_op not in op_map:
                    # unknown operation, exit immediately
                    return predictor_data

                # check condition
                filter_op2 = op_map[filter_op]
                if getattr(val, filter_op2)(arg):
                    data2.append(row)
            else:
                # unknown operation, add anyway
                data2.append(row)

        return data2

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
