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
import pandas as pd
import datetime
import time

import duckdb
from lightwood.api import dtype
from mindsdb_sql import parse_sql
from mindsdb_sql.planner import plan_query
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    UnaryOperation,
    Identifier,
    Operation,
    Constant,
    OrderBy,
    Select,
    Union,
    Join,
    Star
)
from mindsdb_sql.planner.steps import (
    ApplyTimeseriesPredictorStep,
    ApplyPredictorRowStep,
    GetPredictorColumns,
    FetchDataframeStep,
    ApplyPredictorStep,
    MapReduceStep,
    MultipleSteps,
    ProjectStep,
    FilterStep,
    UnionStep,
    JoinStep
)

from mindsdb.api.mysql.mysql_proxy.classes.com_operators import operator_map
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES, ERR
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.interfaces.ai_table.ai_table import AITableStore
import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.utilities.sql import query_df


superset_subquery = re.compile(r'from[\s\n]*(\(.*\))[\s\n]*as[\s\n]*virtual_table', flags=re.IGNORECASE | re.MULTILINE | re.S)


class NotImplementedError(Exception):
    pass


class SqlError(Exception):
    pass


def get_preditor_alias(step, mindsdb_database):
    predictor_name = '.'.join(step.predictor.parts)
    predictor_alias = '.'.join(step.predictor.alias.parts) if step.predictor.alias is not None else predictor_name
    return (mindsdb_database, predictor_name, predictor_alias)


def get_table_alias(table_obj, default_db_name):
    # (database, table, alias)
    if len(table_obj.parts) > 2:
        raise Exception(f'Table name must contain no more than 2 parts. Got name: {table_obj.parts}')
    elif len(table_obj.parts) == 1:
        name = (default_db_name, table_obj.parts[0])
    else:
        name = tuple(table_obj.parts)
    if table_obj.alias is not None:
        name = name + ('.'.join(table_obj.alias.parts),)
    else:
        name = name + (None,)
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
    else:
        raise Exception(f'Unknown type of identifier: {stmt}')

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
        if where.value == '$var':
            where.is_var = True


def replaceQueryVar(where, val):
    if isinstance(where, BinaryOperation):
        replaceQueryVar(where.args[0], val)
        replaceQueryVar(where.args[1], val)
    elif isinstance(where, UnaryOperation):
        replaceQueryVar(where.args[0], val)
    elif isinstance(where, Constant):
        if hasattr(where, 'is_var') and where.is_var is True:
            where.value = val


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


class SQLQuery():
    def __init__(self, sql, session):
        self.session = session
        self.integration = session.integration
        self.database = None if session.database == '' else session.database.lower()
        self.datahub = session.datahub
        self.ai_table = None
        self.outer_query = None
        self.row_id = 0
        self.columns_list = None

        self.mindsdb_database_name = 'mindsdb'

        # +++ workaround for subqueries in superset
        if 'as virtual_table' in sql.lower():
            subquery = re.findall(superset_subquery, sql)
            if isinstance(subquery, list) and len(subquery) == 1:
                subquery = subquery[0]
                self.outer_query = sql.replace(subquery, 'dataframe')
                sql = subquery.strip('()')
        # ---

        self.raw = sql
        self.model_types = {}
        self._parse_query(sql)

    def fetch(self, datahub, view='list'):
        data = self.fetched_data

        if view == 'list':
            self.result = self._make_list_result_view(data)
        elif view == 'dict':
            self.result = self._make_dict_result_view(data)
        else:
            raise Exception('Only "list" and "dict" views supported atm')

        return {
            'success': True,
            'result': self.result
        }

    def _fetch_dataframe_step(self, step):
        dn = self.datahub.get(step.integration)
        query = step.query

        table_alias = get_table_alias(step.query.from_table, self.database)
        # TODO for information_schema we have 'database' = 'mindsdb'

        data, column_names = dn.select(
            query=query
        )

        columns = [(column_name, column_name) for column_name in column_names]
        columns.append(('__mindsdb_row_id', '__mindsdb_row_id'))

        for i, row in enumerate(data):
            row['__mindsdb_row_id'] = self.row_id + i
        self.row_id = self.row_id + len(data)

        data = [{(key, key): value for key, value in row.items()} for row in data]
        data = [{table_alias: x} for x in data]

        data = {
            'values': data,
            'columns': {table_alias: columns},
            'tables': [table_alias]
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

    def _multiple_steps_reduce(self, step, values):
        if step.reduce != 'union':
            raise Exception(f'Unknown MultipleSteps type: {step.reduce}')

        data = {
            'values': [],
            'columns': {},
            'tables': []
        }

        for substep in step.steps:
            if isinstance(substep, FetchDataframeStep) is False:
                raise Exception(f'Wrong step type for MultipleSteps: {step}')
            markQueryVar(substep.query.where)

        for v in values:
            for substep in step.steps:
                replaceQueryVar(substep.query.where, v)
            sub_data = self._multiple_steps(step)
            join_query_data(data, sub_data)

        return data

    def _parse_query(self, sql):
        mindsdb_sql_struct = parse_sql(sql, dialect='mindsdb')

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
            data = [{(key, key): value for key, value in row.items()} for row in data]
            data = [{table_name: x} for x in data]
            self.columns_list = [
                (table_name + (column_name, column_name))
                for column_name in columns
            ]

            columns = [(column_name, column_name) for column_name in columns]

            self.fetched_data = {
                'values': data,
                'columns': {table_name: columns},
                'tables': [table_name]
            }
            return

        # is it query to 'commands'?
        if (
            isinstance(mindsdb_sql_struct.from_table, Identifier)
            and mindsdb_sql_struct.from_table.parts[-1].lower() == 'commands'
            and (
                self.database == 'mindsdb'
                or mindsdb_sql_struct.from_table.parts[0].lower() == 'mindsdb'
            )
        ):
            self.fetched_data = {
                'values': [],
                'columns': {('mindsdb', 'commands', 'commands'): [('command', 'command')]},
                'tables': [('mindsdb', 'commands', 'commands')]
            }
            self.columns_list = [('mindsdb', 'commands', 'commands', 'command', 'command')]
            return

        # is it query to 'datasources'?
        if (
            isinstance(mindsdb_sql_struct.from_table, Identifier)
            and mindsdb_sql_struct.from_table.parts[-1].lower() == 'datasources'
            and (
                self.database == 'mindsdb'
                or mindsdb_sql_struct.from_table.parts[0].lower() == 'mindsdb'
            )
        ):
            dn = self.datahub.get(self.mindsdb_database_name)
            data, columns = dn.get_datasources(mindsdb_sql_struct)
            table_name = ('mindsdb', 'datasources', 'datasources')
            data = [{(key, key): value for key, value in row.items()} for row in data]
            data = [{table_name: x} for x in data]

            self.columns_list = [
                (table_name + (column_name, column_name))
                for column_name in columns
            ]

            columns = [(column_name, column_name) for column_name in columns]

            self.fetched_data = {
                'values': data,
                'columns': {table_name: columns},
                'tables': [table_name]
            }
            return

        integrations_names = self.datahub.get_datasources_names()
        integrations_names.append('information_schema')
        integrations_names.append('file')

        all_tables = get_all_tables(mindsdb_sql_struct)

        predictor_metadata = {}
        predictors = db.session.query(db.Predictor).filter_by(company_id=self.session.company_id)
        for model_name in set(all_tables):
            for p in predictors:
                if p.name == model_name:
                    if isinstance(p.data, dict) and 'error' not in p.data:
                        ts_settings = p.learn_args.get('timeseries_settings', {})
                        if ts_settings.get('is_timeseries') is True:
                            window = ts_settings.get('window')
                            order_by = ts_settings.get('order_by')[0]
                            group_by = ts_settings.get('group_by')
                            if isinstance(group_by, list):
                                group_by = ts_settings.get('group_by')[0]
                            predictor_metadata[model_name] = {
                                'timeseries': True,
                                'window': window,
                                'nr_predictions': ts_settings.get('nr_predictions'),
                                'order_by_column': order_by,
                                'group_by_column': group_by
                            }
                        else:
                            predictor_metadata[model_name] = {
                                'timeseries': False
                            }
                        self.model_types.update(p.data.get('dtypes', {}))

        plan = plan_query(
            mindsdb_sql_struct,
            integrations=integrations_names,
            predictor_namespace=self.mindsdb_database_name,
            predictor_metadata=predictor_metadata,
            default_namespace=self.database
        )

        steps_data = []
        for step in plan.steps:
            data = []
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
            elif type(step) == FetchDataframeStep:
                data = self._fetch_dataframe_step(step)
            elif type(step) == UnionStep:
                raise Exception('Union step is not implemented')
                # TODO add union support
                # left_data = steps_data[step.left.step_num]
                # right_data = steps_data[step.right.step_num]
                # data = left_data + right_data
            elif type(step) == MapReduceStep:
                if step.reduce != 'union':
                    raise Exception(f'Unknown MapReduceStep type: {step.reduce}')

                step_data = steps_data[step.values.step_num]
                values = []
                step_data_values = step_data['values']
                for row in step_data_values:
                    for row_data in row.values():
                        for name, value in row_data.items():
                            if name[0] != '__mindsdb_row_id':
                                values.append(value)

                data = {
                    'values': [],
                    'columns': {},
                    'tables': []
                }
                substep = step.step
                if type(substep) == FetchDataframeStep:
                    query = substep.query
                    markQueryVar(query.where)
                    for value in values:
                        replaceQueryVar(query.where, value)
                        sub_data = self._fetch_dataframe_step(substep)
                        if len(data['columns']) == 0:
                            data['columns'] = sub_data['columns']
                        if len(data['tables']) == 0:
                            data['tables'] = sub_data['tables']
                        data['values'].extend(sub_data['values'])
                elif type(substep) == MultipleSteps:
                    data = self._multiple_steps_reduce(substep, values)
                else:
                    raise Exception(f'Unknown step type: {step.step}')
            elif type(step) == ApplyPredictorRowStep:
                predictor = '.'.join(step.predictor.parts)
                dn = self.datahub.get(self.mindsdb_database_name)
                where_data = step.row_dict

                data = dn.select(
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
            elif type(step) == ApplyPredictorStep or type(step) == ApplyTimeseriesPredictorStep:
                dn = self.datahub.get(self.mindsdb_database_name)
                predictor = '.'.join(step.predictor.parts)
                where_data = []
                for row in steps_data[step.dataframe.step_num]['values']:
                    new_row = {}
                    for table_name in row:
                        keys_intersection = set(new_row) & set(row[table_name])
                        if len(keys_intersection) > 0:
                            raise Exception(
                                f'The predictor got two identical keys from different datasources: {keys_intersection}'
                            )
                        new_row.update(row[table_name])
                    where_data.append(new_row)

                where_data = [{key[1]: value for key, value in row.items()} for row in where_data]

                is_timeseries = predictor_metadata[predictor]['timeseries']
                _mdb_make_predictions = None
                if is_timeseries:
                    if 'LATEST' in self.raw:
                        _mdb_make_predictions = False
                    else:
                        _mdb_make_predictions = True
                    for row in where_data:
                        if '__mdb_make_predictions' not in row:
                            row['__mdb_make_predictions'] = _mdb_make_predictions

                for row in where_data:
                    for key in row:
                        if isinstance(row[key], datetime.date):
                            row[key] = str(row[key])

                data = dn.select(
                    table=predictor,
                    columns=None,
                    where_data=where_data,
                    integration_name=self.session.integration,
                    integration_type=self.session.integration_type
                )

                # if is_timeseries:
                #     if 'LATEST' not in self.raw:
                #         # remove additional records from predictor results:
                #         # first 'window_size' and last 'nr_prediction' records
                #         # otherwise there are many unxpected rows in prediciton result:
                #         # ----------------------------------------------------------------------------------------
                #         # mysql> SELECT tb.time, tb.state, tb.pnew_case, tb.new_case from
                #         # MYSQL_LOCAL.test_data.covid AS
                #         # ta JOIN mindsdb.covid_hor3 AS tb
                #         # WHERE ta.state = "CA" AND ta.time BETWEEN "2020-10-19" AND "2020-10-20";
                #         # ----------------------------------------------------------------------------------------
                #         # +------------+-------+-----------+----------+
                #         # | time       | state | pnew_case | new_case |
                #         # +------------+-------+-----------+----------+
                #         # | 2020-10-09 | CA    | 0         | 2862     |
                #         # | 2020-10-10 | CA    | 0         | 2979     |
                #         # | 2020-10-11 | CA    | 0         | 3075     |
                #         # | 2020-10-12 | CA    | 0         | 3329     |
                #         # | 2020-10-13 | CA    | 0         | 2666     |
                #         # | 2020-10-14 | CA    | 0         | 2378     |
                #         # | 2020-10-15 | CA    | 0         | 3449     |
                #         # | 2020-10-16 | CA    | 0         | 3803     |
                #         # | 2020-10-17 | CA    | 0         | 4170     |
                #         # | 2020-10-18 | CA    | 0         | 3806     |
                #         # | 2020-10-19 | CA    | 0         | 3286     |
                #         # | 2020-10-20 | CA    | 0         | 3474     |
                #         # | 2020-10-21 | CA    | 0         | 3474     |
                #         # | 2020-10-22 | CA    | 0         | 3474     |
                #         # +------------+-------+-----------+----------+
                #         # 14 rows in set (2.52 sec)

                #         window_size = predictor_metadata[predictor]['window']
                #         nr_predictions = predictor_metadata[predictor]['nr_predictions']
                #         if len(data) >= (window_size + nr_predictions):
                #             data = data[window_size:]
                #             if len(data) > nr_predictions and nr_predictions > 1:
                #                 data = data[:-nr_predictions+1]
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
            elif type(step) == JoinStep:
                left_data = steps_data[step.left.step_num]
                right_data = steps_data[step.right.step_num]

                # FIXME https://github.com/mindsdb/mindsdb_sql/issues/136
                is_timeseries = False
                if True in [type(step) == ApplyTimeseriesPredictorStep for step in plan.steps]:
                    right_data = steps_data[step.left.step_num]
                    left_data = steps_data[step.right.step_num]
                    is_timeseries = True

                if step.query.condition is not None:
                    raise Exception('At this moment supported only JOIN without condition')
                if step.query.join_type.upper() not in ('LEFT JOIN', 'JOIN'):
                    raise Exception('At this moment supported only JOIN and LEFT JOIN')
                if (
                    len(left_data['tables']) != 1 or len(right_data['tables']) != 1
                    or left_data['tables'][0] == right_data['tables'][0]
                ):
                    raise Exception('At this moment supported only JOIN of two different tables')

                data = {
                    'values': [],
                    'columns': {},
                    'tables': list(set(left_data['tables'] + right_data['tables']))
                }

                for data_part in [left_data, right_data]:
                    for table_name in data_part['columns']:
                        if table_name not in data['columns']:
                            data['columns'][table_name] = data_part['columns'][table_name]
                        else:
                            data['columns'][table_name].extend(data_part['columns'][table_name])
                for table_name in data['columns']:
                    data['columns'][table_name] = list(set(data['columns'][table_name]))

                left_key = left_data['tables'][0]
                right_key = right_data['tables'][0]

                left_columns_map = {}
                left_columns_map_reverse = {}
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

                df_a = pd.DataFrame(left_df_data)
                df_b = pd.DataFrame(right_df_data)

                a_name = f'a{round(time.time()*1000)}'
                b_name = f'b{round(time.time()*1000)}'
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
                resp_df = resp_df.where(pd.notnull(resp_df), None)
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

            elif type(step) == FilterStep:
                raise Exception('FilterStep is not implemented')
            # elif type(step) == ApplyTimeseriesPredictorStep:
            #     raise Exception('ApplyTimeseriesPredictorStep is not implemented')
            elif type(step) == ProjectStep:
                step_data = steps_data[step.dataframe.step_num]
                columns_list = []
                for column_full_name in step.columns:
                    table_name = None
                    if type(column_full_name) == Star:
                        for table_name, table_columns_list in step_data['columns'].items():
                            for column in table_columns_list:
                                columns_list.append(table_name + column)
                    elif type(column_full_name) == Identifier:
                        column_name_parts = column_full_name.parts
                        column_alias = None if column_full_name.alias is None else '.'.join(column_full_name.alias.parts)
                        if len(column_name_parts) > 2:
                            raise Exception(f'Column name must contain no more than 2 parts. Got name: {".".join(column_full_name)}')
                        elif len(column_name_parts) == 1:
                            column_name = column_name_parts[0]

                            appropriate_table = None
                            if len(step_data['tables']) == 1:
                                appropriate_table = step_data['tables'][0]
                            else:
                                for table_name, table_columns in step_data['columns'].items():
                                    if (column_name, column_name) in table_columns:
                                        if appropriate_table is not None:
                                            raise Exception('Found multiple appropriate tables for column {column_name}')
                                        else:
                                            appropriate_table = table_name
                            if appropriate_table is None:
                                # it is probably constaint
                                # FIXME https://github.com/mindsdb/mindsdb_sql/issues/133
                                # column_name = column_name.strip("'")
                                # name_or_alias = column_alias or column_name
                                # column_alias = name_or_alias
                                # for row in step_data['values']:
                                #     for table in row:
                                #         row[table][(column_name, name_or_alias)] = row[table][(column_name, column_name)]
                                # appropriate_table = step_data['tables'][0]
                                columns_list.append(appropriate_table + (column_alias, column_alias))
                            else:
                                columns_list.append(appropriate_table + (column_name, column_alias or column_name))  # column_name
                        elif len(column_name_parts) == 2:
                            table_name_or_alias = column_name_parts[0]
                            column_name = column_name_parts[1]

                            appropriate_table = None
                            for table_name, table_columns in step_data['columns'].items():
                                checkig_table_name_or_alias = table_name[2] or table_name[1]
                                if table_name_or_alias == checkig_table_name_or_alias:
                                    for table_column_name in table_columns:
                                        if (
                                            table_column_name[1] == column_name
                                            or table_column_name[1] is None and table_column_name[0] == column_name
                                        ):
                                            break
                                    else:
                                        raise Exception(f'Can not find column "{column_name}" in table "{table_name}"')
                                    appropriate_table = table_name
                                    break
                            if appropriate_table is None:
                                raise Exception(f'Can not find approproate table for column {column_name}')

                            columns_to_copy = None
                            for column in step_data['columns'][appropriate_table]:
                                if column[0] == column_name and (column[1] is None or column[1] == column_name):
                                    columns_to_copy = column
                                    break
                            else:
                                raise Exception(f'Can not find approproate column in data: {(column_name, column_alias)}')

                            for row in step_data['values']:
                                row[appropriate_table][(column_name, column_alias)] = row[appropriate_table][columns_to_copy]

                            columns_list.append(appropriate_table + (column_name, column_alias))
                        else:
                            raise Exception('Undefined column name')
                    else:
                        raise Exception(f'Unexpected column name type: {column_full_name}')

                self.columns_list = columns_list
                data = step_data
            else:
                raise Exception(F'Unknown planner step: {step}')
            steps_data.append(data)

        if self.outer_query is not None:
            data = []
            # +++
            result = []
            for row in steps_data[-1]:
                data_row = {}
                for column_record in self.columns_list:
                    table_name = column_record[:3]
                    column_name = column_record[3]
                    data_row[column_record[4] or column_record[3]] = row[table_name][column_name]
                result.append(data_row)
            # ---
            data = self._make_list_result_view(result)
            df = pd.DataFrame(data)
            result = query_df(df, self.outer_query)

            try:
                self.columns_list = [
                    ('', '', '', x, x) for x in result.columns
                ]
            except Exception:
                self.columns_list = [
                    ('', '', '', result.name, result.name)
                ]

            # +++ make list result view
            new_result = []
            for row in result.to_dict(orient='records'):
                data_row = []
                for column_record in self.columns_list:
                    column_name = column_record[4] or column_record[3]
                    data_row.append(row.get(column_name))
                new_result.append(data_row)
            result = new_result
            # ---

            self.fetched_data = result
        else:
            self.fetched_data = steps_data[-1]

        if hasattr(self, 'columns_list') is False:
            self.columns_list = []
            for row in self.fetched_data:
                for table_key in row:
                    for column_name in row[table_key]:
                        if (table_key + (column_name, column_name)) not in self.columns_list:
                            self.columns_list.append((table_key + (column_name, column_name)))

        # if there was no 'ProjectStep', then get columns list from last step:
        if self.columns_list is None:
            self.columns_list = []
            for table_name in self.fetched_data['columns']:
                self.columns_list.extend([
                    table_name + column for column in self.fetched_data['columns'][table_name]
                ])

        self.columns_list = [x for x in self.columns_list if x[3] != '__mindsdb_row_id']

    def _apply_where_filter(self, row, where):
        if isinstance(where, Identifier):
            return row[where.value]
        elif isinstance(where, Constant):
            return where.value
        elif not isinstance(where, (UnaryOperation, BinaryOperation)):
            Exception(f'Unknown operation type: {where}')

        op_fn = operator_map.get(where.op)
        if op_fn is None:
            raise Exception(f'unknown operator {where.op}')

        args = [self._apply_where_filter(row, arg) for arg in where.args]
        result = op_fn(*args)
        return result

    def _make_list_result_view(self, data):
        if self.outer_query is not None:
            return data['values']
        result = []
        for row in data['values']:
            data_row = []
            for column_record in self.columns_list:
                table_name = column_record[:3]
                column_name = column_record[3:]
                data_row.append(row[table_name][column_name])
            result.append(data_row)
        return result

    def _make_dict_result_view(self, data):
        result = []
        for row in data:
            data_row = {}
            for table_name in row:
                data_row.update(row[table_name])
            result.append(data_row)
        return result

    @property
    def columns(self):
        result = []
        for column_record in self.columns_list:
            try:
                field_type = self.model_types.get(column_record[3])
            except Exception:
                field_type = None

            column_type = TYPES.MYSQL_TYPE_VAR_STRING
            if field_type == dtype.date:
                column_type = TYPES.MYSQL_TYPE_DATE
            elif field_type == dtype.datetime:
                column_type = TYPES.MYSQL_TYPE_DATETIME

            result.append({
                'database': column_record[0] or self.database,
                #  TODO add 'original_table'
                'table_name': column_record[1],
                'name': column_record[3],
                'alias': column_record[4] or column_record[3],
                # NOTE all work with text-type, but if/when wanted change types to real,
                # it will need to check all types casts in BinaryResultsetRowPacket
                'type': column_type
            })
        return result
