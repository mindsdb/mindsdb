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
import dfsql
import pandas as pd
import datetime

from lightwood import dtype
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
    ApplyPredictorRowStep,
    FetchDataframeStep,
    ApplyPredictorStep,
    MapReduceStep,
    MultipleSteps,
    ProjectStep,
    FilterStep,
    UnionStep,
    JoinStep
)

from mindsdb.api.mysql.mysql_proxy.classes.com_operators_new import operator_map as new_operator_map
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES, ERR
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.interfaces.ai_table.ai_table import AITableStore
import mindsdb.interfaces.storage.db as db


superset_subquery = re.compile(r'from[\s\n]*(\(.*\))[\s\n]*as[\s\n]*virtual_table', flags=re.IGNORECASE | re.MULTILINE | re.S)


def get_preditor_alias(step, mindsdb_database):
    return (mindsdb_database, '.'.join(step.predictor.parts), '.'.join(step.predictor.alias.parts))


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


class SQLQuery():
    def __init__(self, sql, session):
        self.session = session
        self.integration = session.integration
        self.database = session.database or 'mindsdb'
        self.datahub = session.datahub
        self.ai_table = None
        self.outer_query = None

        # +++ workaround for subqueries in superset
        if 'as virtual_table' in sql.lower():
            subquery = re.findall(superset_subquery, sql)
            if isinstance(subquery, list) and len(subquery) == 1:
                subquery = subquery[0]
                self.outer_query = sql.replace(subquery, 'dataframe')
                sql = subquery.strip('()')
        # ---

        # 'offset x, y' - specific just for mysql, parser dont understand it
        sql = re.sub(r'\n?limit([\n\d\s]*),([\n\d\s]*)', ' limit \g<2> offset \g<1> ', sql, flags=re.IGNORECASE)

        self.raw = sql
        self.model_types = {}
        self._parse_query(sql)

    def fetch(self, datahub, view='list'):
        data = self.fetched_data

        if view == 'list':
            self.result = self._make_list_result_view(data)
        else:
            raise Exception('Only "list" view supported atm')

        return {
            'success': True,
            'result': self.result
        }

    def _fetch_dataframe_step(self, step):
        dn = self.datahub.get(step.integration)
        query = step.query

        table_alias = get_table_alias(step.query.from_table, self.database)

        data = dn.select_query(
            query=query
        )
        data = [{table_alias: x} for x in data]
        return data

    def _multiple_steps(self, step):
        data = []
        for substep in step.steps:
            data.append(self._fetch_dataframe_step(substep))
        return data

    def _multiple_steps_reduce(self, step, values):
        if step.reduce != 'union':
            raise Exception(f'Unknown MultipleSteps type: {step.reduce}')

        result = []

        for substep in step.steps:
            if isinstance(substep, FetchDataframeStep) is False:
                raise Exception(f'Wrong step type for MultipleSteps: {step}')
            markQueryVar(substep.query.where)

        for v in values:
            for substep in step.steps:
                replaceQueryVar(substep.query.where, v)
            data = self._multiple_steps(step)
            for part in data:
                result.extend(part)

        return result

    def _parse_query(self, sql):
        mindsdb_sql_struct = parse_sql(sql, dialect='mindsdb')

        integrations_names = self.datahub.get_integrations_names()
        integrations_names.append('INFORMATION_SCHEMA')
        integrations_names.append('information_schema')

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
                            group_by = ts_settings.get('group_by')[0]
                            predictor_metadata[model_name] = {
                                'timeseries': True,
                                'window': window,
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
            predictor_namespace=self.database,
            predictor_metadata=predictor_metadata
        )
        steps_data = []

        for i, step in enumerate(plan.steps):
            data = []
            if isinstance(step, FetchDataframeStep):
                data = self._fetch_dataframe_step(step)
            elif isinstance(step, UnionStep):
                left_data = steps_data[step.left.step_num]
                right_data = steps_data[step.right.step_num]
                data = left_data + right_data
            elif isinstance(step, MapReduceStep):
                if step.reduce != 'union':
                    raise Exception(f'Unknown MapReduceStep type: {step.reduce}')

                step_data = steps_data[step.values.step_num]
                values = []
                for row in step_data:
                    for row_data in row.values():
                        for v in row_data.values():
                            values.append(v)

                data = []
                substep = step.step
                if isinstance(substep, FetchDataframeStep):
                    query = substep.query
                    markQueryVar(query.where)
                    for value in values:
                        replaceQueryVar(query.where, value)
                        data.extend(self._fetch_dataframe_step(substep))
                elif isinstance(substep, MultipleSteps):
                    data = self._multiple_steps_reduce(substep, values)
                else:
                    raise Exception(f'Unknown step type: {step.step}')
            elif isinstance(step, ApplyPredictorRowStep):
                predictor = '.'.join(step.predictor.parts)
                dn = self.datahub.get(self.database)
                where_data = step.row_dict
                data = dn.select(
                    table=predictor,
                    columns=None,
                    where_data=where_data,
                    where={}
                )
                data = [{get_preditor_alias(step, self.database): x} for x in data]
            elif isinstance(step, ApplyPredictorStep):
                dn = self.datahub.get(self.database)
                predictor = '.'.join(step.predictor.parts)
                where_data = []
                for row in steps_data[step.dataframe.step_num]:
                    new_row = {}
                    for table_name in row:
                        keys_intersection = set(new_row) & set(row[table_name])
                        if len(keys_intersection) > 0:
                            raise Exception(
                                f'The predictor got two identical keys from different datasources: {keys_intersection}'
                            )
                        new_row.update(row[table_name])
                    where_data.append(new_row)

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
                    where={},
                    is_timeseries=_mdb_make_predictions
                )
                data = [{get_preditor_alias(step, self.database): x} for x in data]
            elif isinstance(step, JoinStep):
                left_data = steps_data[step.left.step_num]
                right_data = steps_data[step.right.step_num]

                # if join predictor and data used for predictions, then pass that step
                is_left_predictions = isinstance(plan.steps[step.left.step_num], ApplyPredictorStep)
                is_right_predictions = isinstance(plan.steps[step.right.step_num], ApplyPredictorStep)
                if (
                    (
                        is_left_predictions
                        and plan.steps[step.left.step_num].dataframe.step_num == step.right.step_num
                        and predictor_metadata[plan.steps[step.left.step_num].predictor.parts[0]]['timeseries'] is True
                    ) or (
                        is_right_predictions
                        and plan.steps[step.right.step_num].dataframe.step_num == step.left.step_num
                        and predictor_metadata[plan.steps[step.right.step_num].predictor.parts[0]]['timeseries'] is True
                    )
                ):
                    data = left_data if is_left_predictions else right_data
                    steps_data.append(data)
                elif step.query.condition is None:
                    # line-to-line join
                    if len(left_data) != len(right_data):
                        raise Exception('wrong data length')
                    data = []
                    # +++ temp fix while we have only 1 to 1 join
                    if len(left_data) > 0:
                        left_alias = list(left_data[0].keys())[0]
                    if len(left_data) > 0:
                        right_alias = list(right_data[0].keys())[0]
                    # ---
                    for i in range(len(left_data)):
                        data.append({
                            left_alias: left_data[i][left_alias],
                            right_alias: right_data[i][right_alias]
                        })
                else:
                    raise Exception('Unknown join type')
            elif isinstance(step, FilterStep):
                raise Exception('FilterStep not implemented')
            elif isinstance(step, ProjectStep):
                step_data = steps_data[step.dataframe.step_num]
                row = step_data[0]  # TODO if rowcount = 0
                tables_columns = {}
                for table_name in row:
                    tables_columns[table_name] = list(row[table_name].keys())

                columns_list = []
                for column_full_name in step.columns:
                    table_name = None
                    if isinstance(column_full_name, Star) is False:
                        column_name_parts = column_full_name.parts
                        column_alias = None if column_full_name.alias is None else '.'.join(column_full_name.alias.parts)
                        if len(column_name_parts) > 2:
                            raise Exception(f'Column name must contain no more than 2 parts. Got name: {".".join(column_full_name)}')
                        elif len(column_name_parts) == 1:
                            column_name = column_name_parts[0]

                            appropriate_table = None
                            for table_name, table_columns in tables_columns.items():
                                if column_name in table_columns:
                                    if appropriate_table is not None:
                                        raise Exception('Fount multiple appropriate tables for column {column_name}')
                                    else:
                                        appropriate_table = table_name
                            if appropriate_table is None:
                                # it is probably constaint
                                column_name = column_name.strip("'")
                                name_or_alias = column_alias or column_name
                                column_alias = name_or_alias
                                # appropriate_table = ''
                                for row in step_data:
                                    for table in row:
                                        row[table][name_or_alias] = column_name
                                appropriate_table = list(step_data[0].keys())[0]
                                # raise Exception(f'Can not find approproate table for column {column_name}')
                                columns_list.append(appropriate_table + (column_alias, column_alias))
                            else:
                                columns_list.append(appropriate_table + (column_name, column_alias))
                        elif len(column_name_parts) == 2:
                            table_name_or_alias = column_name_parts[0]
                            column_name = column_name_parts[1]

                            appropriate_table = None
                            for table_name, table_columns in tables_columns.items():
                                checkig_table_name_or_alias = table_name[2] or table_name[1]
                                if table_name_or_alias == checkig_table_name_or_alias:
                                    if column_name not in table_columns:
                                        raise Exception(f'Can not find column "{column_name}" in table "{table_name}"')
                                    appropriate_table = table_name
                                    break
                            if appropriate_table is None:
                                raise Exception(f'Can not find approproate table for column {column_name}')
                            columns_list.append(appropriate_table + (column_name, column_alias))
                        else:
                            raise Exception('Undefined column name')
                    else:
                        for table_name, table_columns in tables_columns.items():
                            for column_name in table_columns:
                                columns_list.append(table_name + (column_name, None))

                self.columns_list = columns_list
                data = step_data
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
            # result = dfsql.sql_query(self.outer_query, virtual_table=df)      # make an issue about it
            result = dfsql.sql_query(self.outer_query, dataframe=df)

            try:
                self.columns_list = [
                    ('', '', '', x, x) for x in result.columns
                ]
            except Exception:
                self.columns_list = [
                    ('', '', '', result.name, result.name)
                ]

            if isinstance(result, pd.core.series.Series):
                result = result.to_frame()

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

            # result = result.to_dict(orient='records')
            # for row in result:
            #     for column in self.columns_list:
            #         if (column[4] or column[3]) not in row:
            #             row[column[4] or column[3]] = None
            self.fetched_data = result
        else:
            self.fetched_data = steps_data[-1]

        # TODO if self.columns_list is None
        # that possible if only one fetchData step in paln

    def _apply_where_filter(self, row, where):
        if isinstance(where, Identifier):
            return row[where.value]
        elif isinstance(where, Constant):
            return where.value
        elif not isinstance(where, (UnaryOperation, BinaryOperation)):
            Exception(f'Unknown operation type: {where}')

        op_fn = new_operator_map.get(where.op)
        if op_fn is None:
            raise Exception(f'unknown operator {where.op}')

        args = [self._apply_where_filter(row, arg) for arg in where.args]
        result = op_fn(*args)
        return result

    def _make_list_result_view(self, data):
        if self.outer_query is not None:
            return data
        result = []
        for row in data:
            data_row = []
            for column_record in self.columns_list:
                table_name = column_record[:3]
                column_name = column_record[3]
                data_row.append(row[table_name][column_name])
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
            result.append({
                'database': column_record[0] or self.database,
                #  TODO add 'original_table'
                'table_name': column_record[1],
                'name': column_record[3],
                'alias': column_record[4] or column_record[3],
                # NOTE all work with text-type, but if/when wanted change types to real,
                # it will need to check all types casts in BinaryResultsetRowPacket
                'type': TYPES.MYSQL_TYPE_VAR_STRING if field_type not in (dtype.date, dtype.datetime) else TYPES.MYSQL_TYPE_TIMESTAMP
            })
        return result
