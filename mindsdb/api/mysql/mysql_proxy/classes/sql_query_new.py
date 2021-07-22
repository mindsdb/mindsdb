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

from mindsdb_sql import parse_sql
from mindsdb_sql.planner import plan_query
from mindsdb_sql.parser.ast import Join, Identifier, Operation, Constant, UnaryOperation, BinaryOperation, OrderBy
from mindsdb_sql.planner.steps import (
    FetchDataframeStep,
    ApplyPredictorStep,
    ApplyPredictorRowStep,
    JoinStep,
    ProjectStep,
    FilterStep
)
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest

from mindsdb.api.mysql.mysql_proxy.classes.com_operators_new import operator_map as new_operator_map
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import TYPES
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import ERR
from mindsdb.interfaces.ai_table.ai_table import AITableStore

from mindsdb.api.mysql.mysql_proxy.utilities.sql import to_moz_sql_struct, plain_where_conditions


class SQLQuery():
    raw = ''
    struct = {}
    result = None

    def __init__(self, sql, integration=None, database=None, datahub=None):
        self.integration = integration
        if not database:
            self.database = 'mindsdb'
        else:
            self.database = database
        self.datahub = datahub

        self.ai_table = None

        # 'offset x, y' - specific just for mysql, parser dont understand it
        sql = re.sub(r'\n?limit([\n\d\s]*),([\n\d\s]*)', ' limit \g<1> offset \g<2> ', sql, flags=re.IGNORECASE)

        self.raw = sql
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

    def _parse_query(self, sql):
        def get_preditor_alias(step, mindsdb_database):
            return (mindsdb_database, step.predictor, step.alias)

        def get_table_alias(table_obj, default_db_name):
            # (database, table, alias)
            if len(table_obj.parts) > 2:
                raise Exception(f'Table name must contain no more than 2 parts. Got name: {table_obj.parts}')
            elif len(table_obj.parts) == 1:
                name = (default_db_name, table_obj.parts[0])
            else:
                name = tuple(table_obj.parts)
            name = name + (table_obj.alias,)
            return name

        def get_all_tables(from_stmt):
            result = []
            if isinstance(from_stmt, Identifier):
                result.append(from_stmt.parts[-1])
            elif isinstance(from_stmt, Join):
                result.extend(get_all_tables(from_stmt.left))
                result.extend(get_all_tables(from_stmt.right))
            return result

        mindsdb_sql_struct = parse_sql(sql, dialect='mindsdb')

        integrations_names = self.datahub.get_integrations_names()
        integrations_names.append('INFORMATION_SCHEMA')

        mindsdb_datanode = self.datahub.get(self.database)

        all_tables = get_all_tables(mindsdb_sql_struct.from_table)

        models = mindsdb_datanode.model_interface.get_models()
        model_names = [m['name'] for m in models]
        predictor_metadata = {}
        for model_name in (set(model_names) & set(all_tables)):
            model_meta = mindsdb_datanode.model_interface.get_model_data(name=model_name)
            window = model_meta.get('timeseries', {}).get('user_settings', {}).get('window')
            predictor_metadata[model_meta['name']] = {'timeseries': False}
            if window is not None:
                order_by = model_meta.get('timeseries', {}).get('user_settings', {}).get('order_by')[0]
                group_by = model_meta.get('timeseries', {}).get('user_settings', {}).get('group_by')[0]
                predictor_metadata[model_meta['name']] = {
                    'timeseries': True,
                    'window': window,
                    'order_by_column': order_by,
                    'group_by_column': group_by
                }

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
                dn = self.datahub.get(step.integration)
                query = step.query

                data = dn.select_query(
                    query=query
                )
                table_alias = get_table_alias(step.query.from_table, self.database)
                data = [{table_alias: x} for x in data]
            elif isinstance(step, ApplyPredictorRowStep):
                dn = self.datahub.get(self.database)
                where_data = step.row_dict
                data = dn.select(
                    table=step.predictor,
                    columns=None,
                    where_data=where_data,
                    where={}
                )
                data = [{get_preditor_alias(step, self.database): x} for x in data]
            elif isinstance(step, ApplyPredictorStep):
                dn = self.datahub.get(self.database)
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

                is_timeseries = predictor_metadata[step.predictor]['timeseries']
                if is_timeseries:
                    for row in where_data:
                        row['make_predictions'] = False

                data = dn.select(
                    table=step.predictor,
                    columns=None,
                    where_data=where_data,
                    where={},
                    is_timeseries=is_timeseries
                )
                data = [{get_preditor_alias(step, self.database): x} for x in data]
            elif isinstance(step, JoinStep):
                left_data = steps_data[step.left.step_num]
                right_data = steps_data[step.right.step_num]
                if step.query.condition is None:
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
                    if column_full_name != '*':
                        column_name_parts = column_full_name.split('.')
                        if len(column_name_parts) > 2:
                            raise Exception(f'Column name must contain no more than 2 parts. Got name: {column_full_name}')
                        elif len(column_name_parts) == 1:
                            column_name = column_name_parts[0]
                            column_alias = step.aliases.get(column_full_name)

                            appropriate_table = None
                            for table_name, table_columns in tables_columns.items():
                                if column_name in table_columns:
                                    if appropriate_table is not None:
                                        raise Exception('Fount multiple appropriate tables for column {column_name}')
                                    else:
                                        appropriate_table = table_name
                            if appropriate_table is None:
                                raise Exception(f'Can not find approproate table for column {column_name}')
                            columns_list.append(appropriate_table + (column_name, column_alias))
                        elif len(column_name_parts) == 2:
                            table_name_or_alias = column_name_parts[0]
                            column_name = column_name_parts[1]
                            column_alias = step.aliases.get(column_full_name)

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

        self.fetched_data = steps_data[-1]

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
            result.append({
                'database': column_record[0] or self.database,
                #  TODO add 'original_table'
                'table_name': column_record[1],
                'name': column_record[3],
                'alias': column_record[4] or column_record[3],
                # NOTE all work with text-type, but if/when wanted change types to real,
                # it will need to check all types casts in BinaryResultsetRowPacket
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            })
        return result
