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
import copy
import re
from collections import defaultdict
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
from mindsdb_sql.parser.ast.base import ASTNode

from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df
from mindsdb.interfaces.model.functions import (
    get_model_records,
    get_predictor_project
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


superset_subquery = re.compile(r'from[\s\n]*(\(.*\))[\s\n]*as[\s\n]*virtual_table', flags=re.IGNORECASE | re.MULTILINE | re.S)


class ColumnsCollection:
    def __init__(self):
        self.__columns = []

    def add(self, table, column):
        self.__columns.append(tuple(table) + tuple(column))

    def __getitem__(self, key):
        return self.__columns[key]

    def __setitem__(self, key, value):
        self.__columns[key] = value

    def __iter__(self):
        self.__n = 0
        return self

    def __next__(self):
        n = self.__n
        if n < len(self.__columns):
            self.__n += 1
            return self.__columns[n][:3], self.__columns[n][3:]
        else:
            raise StopIteration

    def __len__(self):
        return len(self.__columns)

    def items(self):
        collection = defaultdict(list)
        for el in self.__columns:
            collection[el[:3]].append(el[3:])
        return collection.items()

    def tables(self):
        return set(
            x[:3] for x in self.__columns
        )

    def table_columns(self, table_name):
        return [x[3:] for x in self.__columns if x[:3] == table_name]

    def extend(self, columns_collection):
        for table_name, column_name in columns_collection:
            self.add(table_name, column_name)

    def group_by_table(self):
        groups = defaultdict(list)
        for x in self.__columns:
            groups[x[:3]].append(x[3:])
        return list(groups.items())

    def del_duplicate_columns(self):
        col_idx = []
        to_del = []
        for i, col in enumerate(self.__columns):
            if col not in col_idx:
                col_idx.append(col)
            else:
                to_del.append(i)

        to_del.sort(reverse=True)
        for i in to_del:
            self.__columns.pop(i)

    def del_table_columns(self, table_name):
        self.__columns = [
            x for x in self.__columns
            if x[:3] != table_name
        ]

    def from_dict(self, columns_dict):
        for table_name in columns_dict:
            for columns_name in columns_dict[table_name]:
                self.add(table_name, columns_name)

    def copy(self):
        new_obj = ColumnsCollection()
        for x in self.__columns:
            new_obj.add(x[:3], x[3:])
        return new_obj


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
    if len(target.get_columns()) == 0:
        target = source
    else:
        target.add_records(source.get_records())
    return target


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

    def get_hash_name(self, prefix):
        table_name = self.table_name if self.table_alias is None else self.table_alias
        name = self.name if self.alias is None else self.alias

        name = f'{prefix}_{table_name}_{name}'
        return name

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'


class ResultSet:
    def __init__(self, length=0):
        self._columns = []
        # records is list of lists with the same length as columns
        self._records = []
        for i in range(length):
            self._records.append([])

        self.is_prediction = False

    def __repr__(self):
        col_names = ', '.join([col.name for col in self._columns])
        data = '\n'.join([str(rec) for rec in self._records[:20]])

        if len(self._records) > 20:
            data += '\n...'

        return f'{self.__class__.__name__}({col_names}: {self.length()} rows)\n {data}'

    # --- converters ---

    def from_step_data(self, step_data):

        for table, col_list in step_data['columns'].items():
            for col in col_list:
                type = None
                if 'types' in step_data:
                    type = step_data['types'].get(table, {}).get(col[0])
                self._columns.append(Column(
                    name=col[0],
                    alias=col[1],
                    type=type,
                    table_name=table[1],
                    table_alias=table[2],
                    database=table[0]
                ))

        for row in step_data['values']:
            data_row = []
            for col in self._columns:
                col_key = (col.name, col.alias)
                table_key = (col.database, col.table_name, col.table_alias)
                val = row[table_key][col_key]

                data_row.append(val)

            self._records.append(data_row)

        self.is_prediction = step_data.get('is_prediction', False)
        return self

    def from_df(self, df, database, table_name, table_alias=None):

        resp_dict = df.to_dict(orient='split')

        self._records = resp_dict['data']

        for col in resp_dict['columns']:
            self._columns.append(Column(
                name=col,
                table_name=table_name,
                table_alias=table_alias,
                database=database,
                type=df.dtypes[col]
            ))
        return self

    def from_df_cols(self, df, col_names):

        resp_dict = df.to_dict(orient='split')

        self._records = resp_dict['data']

        for col in resp_dict['columns']:
            self._columns.append(col_names[col])
        return self

    def to_df(self):
        columns = self.get_column_names()
        return pd.DataFrame(self._records, columns=columns)

    def to_df_cols(self, prefix=''):
        # returns dataframe and dict of columns
        #   can be restored to ResultSet by from_df_cols method

        columns = []
        col_names = {}
        for col in self._columns:
            name = col.get_hash_name(prefix)
            columns.append(name)
            col_names[name] = col

        return pd.DataFrame(self._records, columns=columns), col_names

    def to_step_data(self):
        step_data = {
            'values': [],
            'columns': ColumnsCollection(),
            'types': {},
            'tables': [],
            'is_prediction': self.is_prediction
        }

        for col in self._columns:
            col_key = (col.name, col.alias)
            table_key = (col.database, col.table_name, col.table_alias)

            if table_key not in step_data['tables']:
                step_data['tables'].append(table_key)
                step_data['types'][table_key] = {}

            step_data['columns'].add(table_key, col_key)
            if col.type is not None:
                step_data['types'][table_key][col.name] = col.type

        for rec in self._records:
            row = {}
            for table in step_data['tables']:
                row[table] = {}
            for i, col in enumerate(self._columns):
                col_key = (col.name, col.alias)
                table_key = (col.database, col.table_name, col.table_alias)

                row[table_key][col_key] = rec[i]

            step_data['values'].append(row)
        return step_data

    # --- tables ---

    def get_tables(self):
        tables_idx = []
        tables = []
        cols = ['database', 'table_name', 'table_alias']
        for col in self._columns:
            table = (col.database, col.table_name, col.table_alias)
            if table not in tables_idx:
                tables_idx.append(table)
                tables.append(dict(zip(cols, table)))
        return tables

    # --- columns ---

    def _locate_column(self, col):
        col_idx = None
        for i, col0 in enumerate(self._columns):
            if col0 is col:
                col_idx = i
                break
        if col_idx is None:
            raise ErSqlWrongArguments(f'Column is not found: {col}')
        return col_idx

    def add_column(self, col, values=None):
        self._columns.append(col)

        if values is None:
            values = []
        # update records
        if len(self._records) > 0:
            for rec in self._records:
                if len(values) > 0:
                    value = values.pop(0)
                else:
                    value = None
                rec.append(value)

    def del_column(self, col):
        idx = self._locate_column(col)
        self._columns.pop(idx)
        for row in self._records:
            row.pop(idx)

    def get_columns(self):
        return self._columns

    def get_column_names(self):
        columns = [
            col.name if col.alias is None else col.alias
            for col in self._columns
        ]
        return columns

    def find_columns(self, alias=None, table_alias=None):
        col_list = []
        for col in self.get_columns():
            if alias is not None and col.alias.lower() != alias.lower():
                continue
            if table_alias is not None and col.table_alias.lower() != table_alias.lower():
                continue
            col_list.append(col)

        return col_list

    def copy_column_to(self, col, result_set2):
        # copy with values
        idx = self._locate_column(col)

        values = [row[idx] for row in self._records]

        col2 = copy.deepcopy(col)

        result_set2.add_column(col2, values)
        return col2

    # --- records ---

    def add_records(self, data):
        names = self.get_column_names()
        for rec in data:
            # if len(rec) != len(self._columns):
            #     raise ErSqlWrongArguments(f'Record length mismatch columns length: {len(rec)} != {len(self._columns)}')

            record = [
                rec[name]
                for name in names
            ]
            self._records.append(record)

    def get_records_raw(self):
        return self._records

    def add_record_raw(self, rec):
        if len(rec) != len(self._columns):
            raise ErSqlWrongArguments(f'Record length mismatch columns length: {len(rec)} != {len(self.columns)}')
        self._records.append(rec)

    def get_records(self):
        # in dicts
        names = self.get_column_names()
        records = []
        for row in self._records:
            records.append(dict(zip(names, row)))
        return records

    # def clear_records(self):
    #     self._records = []

    def length(self):
        return len(self._records)


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
        databases_names = self.session.database_controller.get_list()
        databases_names = [x['name'] for x in databases_names]

        predictor_metadata = []
        predictors_records = get_model_records()

        query_tables = []

        def get_all_query_tables(node, is_table, **kwargs):
            if is_table and isinstance(node, Identifier):
                table_name = node.parts[-1]
                if table_name.isdigit():
                    # is predictor version
                    table_name = node.parts[-2]
                query_tables.append(table_name)

        query_traversal(self.query, get_all_query_tables)

        for predictor_record in predictors_records:
            model_name = predictor_record.name

            if model_name not in query_tables:
                continue

            project_record = get_predictor_project(predictor_record)
            if project_record is None:
                continue
            project_name = project_record.name

            if isinstance(predictor_record.data, dict) and 'error' not in predictor_record.data:
                ts_settings = predictor_record.learn_args.get('timeseries_settings', {})
                predictor = {
                    'name': model_name,
                    'integration_name': project_name,   # integration_name,
                    'timeseries': False,
                    'id': predictor_record.id
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

                self.model_types.update(predictor_record.data.get('dtypes', {}))

        database = None if self.session.database == '' else self.session.database.lower()

        self.predictor_metadata = predictor_metadata
        self.planner = query_planner.QueryPlanner(
            self.query,
            integrations=databases_names,
            predictor_metadata=predictor_metadata,
            default_namespace=database
        )

    def fetch(self, view='list'):
        data = self.fetched_data

        result = self._make_list_result_view(data)
        if view == 'dataframe':
            col_names = [
                col.alias if col.alias is not None else col.name
                for col in self.columns_list
            ]
            result = pd.DataFrame(result, columns=col_names)

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

        if dn is None:
            raise SqlApiUnknownError(f'Unknown integration name: {step.integration}')

        if query is None:
            table_alias = (self.database, 'result', 'result')

            # fetch raw_query
            data, columns_info = dn.query(
                native_query=step.raw_query,
                session=self.session
            )
        else:
            table_alias = get_table_alias(step.query.from_table, self.database)
            # TODO for information_schema we have 'database' = 'mindsdb'

            data, columns_info = dn.query(
                query=query,
                session=self.session
            )

        # if this is query: execute it
        if isinstance(data, ASTNode):
            subquery = SQLQuery(data, session=self.session)
            return subquery.fetched_data

        result = ResultSet()
        for column in columns_info:
            result.add_column(Column(
                name=column['name'],
                type=column.get('type'),
                table_name=table_alias[1],
                table_alias=table_alias[2],
                database=table_alias[0]
            ))
        result.add_records(data)

        return result.to_step_data()

    def _multiple_steps(self, steps):
        data = ResultSet()
        for substep in steps:
            sub_data = self._fetch_dataframe_step(substep)
            sub_data = ResultSet().from_step_data(sub_data)
            data = join_query_data(data, sub_data)
        return data

    def _multiple_steps_reduce(self, step, vars):
        if step.reduce != 'union':
            raise ErLogicError(f'Unknown MultipleSteps type: {step.reduce}')

        data = ResultSet()

        # mark vars
        steps = []
        for substep in step.steps:
            if isinstance(substep, FetchDataframeStep) is False:
                raise ErLogicError(f'Wrong step type for MultipleSteps: {step}')
            substep = copy.deepcopy(substep)
            markQueryVar(substep.query.where)
            steps.append(substep)

        for var_group in vars:
            steps2 = copy.deepcopy(steps)
            for name, value in var_group.items():
                for substep in steps2:
                    replaceQueryVar(substep.query.where, value, name)
            sub_data = self._multiple_steps(steps2)
            data = join_query_data(data, sub_data)

        return data

    def prepare_query(self, prepare=True):
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
            if self.columns_list is None:
                self.columns_list = []
                if self.fetched_data is not None:
                    if isinstance(self.fetched_data['columns'], ColumnsCollection) is False:  # <<< del???
                        columns_collection = ColumnsCollection()
                        columns_collection.from_dict(self.fetched_data['columns'])
                        self.fetched_data['columns'] = columns_collection
                    for table_name, column in self.fetched_data['columns']:
                        col_types = self.fetched_data.get('types', {}).get(table_name, {})
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

            data = ResultSet()
            for column_name in columns:
                data.add_column(Column(
                    name=column_name,
                    table_name=predictor_name,
                    database=self.mindsdb_database_name
                ))
            data = data.to_step_data()

        elif type(step) == GetTableColumns:
            table = step.table
            dn = self.datahub.get(step.namespace)
            ds_query = Select(from_table=Identifier(table), targets=[Star()], limit=Constant(0))

            data, columns_info = dn.query(ds_query, session=self.session)

            data = ResultSet()
            for column in columns_info:
                data.add_column(Column(
                    name=column['name'],
                    type=column.get('type'),
                    table_name=table,
                    database=self.database
                ))
            data = data.to_step_data()
        elif type(step) == FetchDataframeStep:
            data = self._fetch_dataframe_step(step)
        elif type(step) == UnionStep:
            left_result = ResultSet()
            right_result = ResultSet()

            left_result.from_step_data(steps_data[step.left.step_num])
            right_result.from_step_data(steps_data[step.right.step_num])

            # count of columns have to match
            if len(left_result.get_columns()) != len(right_result.get_columns()):
                raise ErSqlWrongArguments(
                    f'UNION columns count mismatch: {len(left_result.get_columns())} != {len(right_result.get_columns())} ')

            # types have to match
            # TODO: return checking type later
            # for i, left_col in enumerate(left_result.columns):
            #     right_col = right_result.columns[i]
            #     type1, type2 = left_col.type, right_col.type
            #     if type1 is not None and type2 is not None:
            #         if type1 != type2:
            #             raise ErSqlWrongArguments(f'UNION types mismatch: {type1} != {type2}')

            result = ResultSet()
            for col in left_result.get_columns():
                result.add_column(col)

            records_hashes = []
            for row in left_result.get_records_raw() + right_result.get_records_raw():
                if step.unique:
                    checksum = hashlib.sha256(str(row).encode()).hexdigest()
                    if checksum in records_hashes:
                        continue
                    records_hashes.append(checksum)
                result.add_record_raw(row)

            data = result.to_step_data()

        elif type(step) == MapReduceStep:
            try:
                if step.reduce != 'union':
                    raise ErLogicError(f'Unknown MapReduceStep type: {step.reduce}')

                step_data = ResultSet().from_step_data(steps_data[step.values.step_num])
                vars = []
                for row in step_data.get_records():
                    var_group = {}
                    vars.append(var_group)
                    for name, value in row.items():
                        if name != '__mindsdb_row_id':
                            var_group[name] = value

                data = ResultSet()

                substep = step.step
                if type(substep) == FetchDataframeStep:
                    query = substep.query
                    for var_group in vars:
                        markQueryVar(query.where)
                        for name, value in var_group.items():
                            replaceQueryVar(query.where, value, name)
                        sub_data = self._fetch_dataframe_step(substep)
                        sub_data = ResultSet().from_step_data(sub_data)
                        if len(data.get_columns()) == 0:
                            data = sub_data
                        else:
                            data.add_records(sub_data.get_records())

                        unmarkQueryVar(query.where)
                elif type(substep) == MultipleSteps:
                    data = self._multiple_steps_reduce(substep, vars)
                else:
                    raise ErLogicError(f'Unknown step type: {step.step}')

                data = data.to_step_data()
            except Exception as e:
                raise SqlApiUnknownError(f'error in map reduce step: {e}') from e
        elif type(step) == MultipleSteps:
            if step.reduce != 'union':
                raise ErNotSupportedYet(f"Only MultipleSteps with type = 'union' is supported. Got '{step.type}'")
            data = None
            for substep in step.steps:
                subdata = self.execute_step(substep, steps_data)
                subdata = ResultSet().from_step_data(subdata)
                if data is None:
                    data = subdata
                else:
                    data.add_records(subdata.get_records())
            data = data.to_step_data()
        elif type(step) == ApplyPredictorRowStep:
            try:
                project_name = step.namespace
                predictor_name = step.predictor.parts[0]
                where_data = step.row_dict
                project_datanode = self.datahub.get(project_name)

                version = None
                if len(step.predictor.parts) > 1 and step.predictor.parts[-1].isdigit():
                    version = int(step.predictor.parts[-1])

                predictions, columns_dtypes = project_datanode.predict(
                    model_name=predictor_name,
                    data=where_data,
                    version=version,
                    params=step.params,
                )
                # update predictions with input data
                for row in predictions:
                    for k, v in where_data.items():
                        if k not in row:
                            row[k] = v

                table_name = get_preditor_alias(step, self.database)

                result = ResultSet()
                result.is_prediction = True
                if len(predictions) > 0:
                    cols = list(predictions[0].keys())
                else:
                    cols = project_datanode.get_table_columns(predictor_name)

                for col in cols:
                    result.add_column(Column(
                        name=col,
                        table_name=table_name[1],
                        table_alias=table_name[2],
                        database=table_name[0],
                        type=columns_dtypes.get(col)
                    ))
                result.add_records(predictions)

                data = result.to_step_data()
            except Exception as e:
                if isinstance(e, SqlApiException):
                    raise e
                else:
                    raise SqlApiUnknownError(f'error in apply predictor row step: {e}') from e
        elif type(step) in (ApplyPredictorStep, ApplyTimeseriesPredictorStep):
            try:
                # set row_id
                data = steps_data[step.dataframe.step_num]
                data = ResultSet().from_step_data(data)

                for table in data.get_tables():
                    row_id_col = Column(
                        name='__mindsdb_row_id',
                        database=table['database'],
                        table_name=table['table_name'],
                        table_alias=table['table_alias']
                    )

                    values = list(range(self.row_id, self.row_id + data.length()))
                    data.add_column(row_id_col, values)
                    self.row_id += data.length()

                steps_data[step.dataframe.step_num] = data.to_step_data()

                project_name = step.namespace
                predictor_name = step.predictor.parts[0]

                where_data = data.get_records()

                predictor_metadata = {}
                for pm in self.predictor_metadata:
                    if pm['name'] == predictor_name and pm['integration_name'].lower() == project_name:
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
                result = ResultSet()
                result.is_prediction = True

                project_datanode = self.datahub.get(project_name)
                if len(where_data) == 0:
                    cols = project_datanode.get_table_columns(predictor_name) + ['__mindsdb_row_id']
                    for col in cols:
                        result.add_column(Column(
                            name=col,
                            database=table_name[0],
                            table_name=table_name[1],
                            table_alias=table_name[2]
                        ))
                else:
                    predictor_id = predictor_metadata['id']
                    key = f'{predictor_name}_{predictor_id}_{json_checksum(where_data)}'
                    predictor_cache = get_cache('predict')

                    data = predictor_cache.get(key)

                    if data is None:
                        version = None
                        if len(step.predictor.parts) > 1 and step.predictor.parts[-1].isdigit():
                            version = int(step.predictor.parts[-1])
                        data, columns_dtypes = project_datanode.predict(
                            model_name=predictor_name,
                            data=where_data,
                            version=version,
                            params=step.params,
                        )
                        if data is not None and isinstance(data, list):
                            predictor_cache.set(key, data)

                    if len(data) > 0:
                        cols = list(data[0].keys())
                        for col in cols:
                            result.add_column(Column(
                                name=col,
                                table_name=table_name[1],
                                table_alias=table_name[2],
                                database=table_name[0],
                                type=columns_dtypes.get(col)
                            ))

                    # apply filter
                    if is_timeseries:
                        data = self.apply_ts_filter(data, where_data, step, predictor_metadata)

                    result.add_records(data)

                data = result.to_step_data()

            except Exception as e:
                raise SqlApiUnknownError(f'error in apply predictor step: {e}') from e
        elif type(step) == JoinStep:
            try:
                if step.query.condition is not None:
                    raise ErNotSupportedYet('At this moment supported only JOIN without condition')

                left_data = ResultSet().from_step_data(steps_data[step.left.step_num])
                right_data = ResultSet().from_step_data(steps_data[step.right.step_num])
                df_a, names_a = left_data.to_df_cols(prefix='A')
                df_b, names_b = right_data.to_df_cols(prefix='B')

                a_row_id = left_data.find_columns('__mindsdb_row_id')[0].get_hash_name(prefix='A')
                b_row_id = right_data.find_columns('__mindsdb_row_id')[0].get_hash_name(prefix='B')

                con = duckdb.connect(database=':memory:')
                con.register('table_a', df_a)
                con.register('table_b', df_b)

                join_type = step.query.join_type.lower()
                if join_type == 'join':
                    # join type is not specified. using join to prediction data
                    if left_data.is_prediction:
                        join_type = 'left join'
                    elif right_data.is_prediction:
                        join_type = 'right join'

                resp_df = con.execute(f"""
                    SELECT * FROM table_a as ta {join_type} table_b as tb
                    ON ta.{a_row_id} = tb.{b_row_id}
                """).fetchdf()
                con.unregister('table_a')
                con.unregister('table_b')
                con.close()

                resp_df = resp_df.replace({np.nan: None})

                names_a.update(names_b)
                data = ResultSet().from_df_cols(resp_df, col_names=names_a)

                data = data.to_step_data()

            except Exception as e:
                raise SqlApiUnknownError(f'error in join step: {e}') from e

        elif type(step) == FilterStep:
            # used only in join of two regular tables
            step_data = steps_data[step.dataframe.step_num]
            result_set = ResultSet().from_step_data(step_data)

            df, col_names = result_set.to_df_cols()
            col_idx = {}
            for name, col in col_names.items():
                col_idx[col.alias] = name
                col_idx[(col.table_alias, col.alias)] = name

            # analyze condition and change name of columns
            def check_fields(node, is_table=None, **kwargs):
                if is_table:
                    raise ErNotSupportedYet('Subqueries is not supported in WHERE')
                if isinstance(node, Identifier):
                    # only column name
                    col_name = node.parts[-1]

                    if len(node.parts) == 1:
                        key = col_name
                    else:
                        table_name = node.parts[-2]
                        key = (table_name, col_name)

                    if key not in col_idx:
                        raise ErKeyColumnDoesNotExist(f'Table not found for column: {key}')

                    new_name = col_idx[key]
                    return Identifier(parts=[new_name])

            where_query = step.query
            query_traversal(where_query, check_fields)

            query = Select(targets=[Star()], from_table=Identifier('df'), where=where_query)

            res = query_df(df, query)

            result_set2 = ResultSet().from_df_cols(res, col_names)

            data = result_set2.to_step_data()

        elif type(step) == LimitOffsetStep:
            try:
                step_data = steps_data[step.dataframe.step_num]
                step_data = ResultSet().from_step_data(step_data)

                step_data2 = ResultSet()
                for col in step_data.get_columns():
                    step_data2.add_column(col)

                records = step_data.get_records()

                if isinstance(step.offset, Constant) and isinstance(step.offset.value, int):
                    records = records[step.offset.value:]
                if isinstance(step.limit, Constant) and isinstance(step.limit.value, int):
                    records = records[:step.limit.value]

                step_data2.add_records(records)

                data = step_data2.to_step_data()

            except Exception as e:
                raise SqlApiUnknownError(f'error in limit offset step: {e}') from e
        elif type(step) == ProjectStep:
            try:
                step_data = steps_data[step.dataframe.step_num]
                rs_in = ResultSet().from_step_data(step_data)
                rs_out = ResultSet(length=rs_in.length())

                for column_identifier in step.columns:
                    if type(column_identifier) == Star:
                        for column in rs_in.get_columns():
                            rs_in.copy_column_to(column, rs_out)

                    elif type(column_identifier) == Identifier:

                        column_name_parts = column_identifier.parts
                        column_alias = column_identifier.parts[-1] if column_identifier.alias is None else '.'.join(
                            column_identifier.alias.parts)

                        if len(column_name_parts) > 2:
                            raise ErSqlWrongArguments(
                                f'Column name must contain no more than 2 parts. Got name: {column_identifier}')
                        elif len(column_name_parts) == 1:
                            column_name = column_name_parts[0]

                            col_list = rs_in.find_columns(column_name)
                            if len(col_list) == 0:
                                raise SqlApiException(f'Can not find appropriate table for column {column_name}')
                            elif len(col_list) > 1 and not step.ignore_doubles:
                                raise ErLogicError(f'Found multiple appropriate tables for column {column_name}')

                            col_added = rs_in.copy_column_to(col_list[0], rs_out)
                            col_added.alias = column_alias

                        elif len(column_name_parts) == 2:
                            table_name_or_alias = column_name_parts[0]
                            column_name = column_name_parts[1]

                            # support select table.*
                            if isinstance(column_name, Star):
                                # add all by table
                                for col in rs_in.find_columns(table_alias=table_name_or_alias):
                                    rs_in.copy_column_to(col, rs_out)
                            else:
                                col_list = rs_in.find_columns(column_name, table_alias=table_name_or_alias)
                                if len(col_list) == 0:
                                    raise SqlApiException(f'Can not find appropriate table for column {table_name_or_alias}.{column_name}')

                                col_added = rs_in.copy_column_to(col_list[0], rs_out)
                                col_added.alias = column_alias
                        else:
                            raise ErSqlWrongArguments('Undefined column name')

                    else:
                        raise ErKeyColumnDoesNotExist(f'Unknown column type: {column_identifier}')

                data = rs_out.to_step_data()
            except Exception as e:
                if isinstance(e, SqlApiException):
                    raise e
                raise SqlApiUnknownError(f'error on project step: {e} ') from e
        elif type(step) == GroupByStep:
            # used only in join of two regular tables
            step_data = steps_data[step.dataframe.step_num]
            step_data = ResultSet().from_step_data(step_data)

            df = step_data.to_df()

            query = Select(targets=step.targets, from_table='df', group_by=step.columns).to_string()
            res = query_df(df, query)

            # stick all columns to first table
            appropriate_table = step_data.get_tables()[0]

            data = ResultSet()

            data.from_df(res, appropriate_table['database'], appropriate_table['table_name'], appropriate_table['table_alias'])

            # columns are changed
            self.columns_list = data.get_columns()

            data = data.to_step_data()

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
            database = result.get_columns()[0].database
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
            data = ResultSet().from_step_data(step_data)
            integration_name = step.table.parts[0]
            table_name = Identifier(parts=step.table.parts[1:])

            dn = self.datahub.get(integration_name)

            if hasattr(dn, 'create_table') is False:
                raise ErNotSupportedYet(f"Creating table in '{integration_name}' is not supporting")

            #  del 'service' columns
            for col in data.find_columns('__mindsdb_row_id'):
                data.del_column(col)
            for col in data.find_columns('__mdb_forecast_offset'):
                data.del_column(col)

            # region del columns filtered at projection step
            if self.columns_list is not None:
                filtered_column_names = [x.name for x in self.columns_list]
                for col in data.get_columns():
                    if col.name.startswith('predictor.'):
                        continue
                    if col.name in filtered_column_names:
                        continue
                    data.del_column(col)
            # endregion

            # drop double names
            col_names = set()
            for col in data.get_columns():
                if col.alias in col_names:
                    data.del_column(col)
                else:
                    col_names.add(col.alias)

            dn.create_table(
                table_name=table_name,
                result_set=data,
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
            data_header = [col.alias for col in result.get_columns()]

            for param_name, _ in params_map_index:
                if param_name not in data_header:
                    raise ErSqlWrongArguments(f'Field {param_name} not found in input data. Input fields: {data_header}')

            # perform update
            for row in result.get_records():
                # run update from every row from input data

                # fill params:
                for param_name, param in params_map_index:
                    param.value = row[param_name]

                dn.query(query=update_query, session=self.session)

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
                ('%Y-%m-%d', r'[\d]{4}-[\d]{2}-[\d]{2}'),
                ('%Y-%m-%d %H:%M:%S', r'[\d]{4}-[\d]{2}-[\d]{2} [\d]{2}:[\d]{2}:[\d]{2}'),
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

            def _cast_samples(data, order_col):
                if isinstance(data[0][order_col], str):
                    samples = [row[order_col] for row in data]
                    date_format = get_date_format(samples)

                    for row in data:
                        row[order_col] = dt.datetime.strptime(row[order_col], date_format)
                elif isinstance(data[0][order_col], dt.datetime):
                    pass  # check because dt.datetime is instance of dt.date but here we don't need to add HH:MM:SS
                elif isinstance(data[0][order_col], dt.date):
                    # convert to datetime
                    for row in data:
                        row[order_col] = dt.datetime.combine(row[order_col], dt.datetime.min.time())

            # convert predictor_data
            if len(predictor_data) > 0:
                _cast_samples(predictor_data, order_col)

            # convert table data
            _cast_samples(table_data, order_col)

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
                if group_cols is None:
                    key = 0  # the same for any value
                else:
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
                    if group_cols is None:
                        key = 0  # the same for any value
                    else:
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
