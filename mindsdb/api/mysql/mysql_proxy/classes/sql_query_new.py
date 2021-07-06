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
from mindsdb_sql.parser.ast import Join, Identifier, Operation, Constant, UnaryOperation, BinaryOperation
from mindsdb_sql.planner.steps import FetchDataframeStep, ApplyPredictorStep, JoinStep, ProjectStep, FilterStep

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
        sql = re.sub(r'\n?limit([\n\d\s]*),([\n\d\s]*)', ' limit \g<1> offset \g<1> ', sql)

        self.raw = sql
        self._parse_query(sql)

    def fetch(self, datahub, view='list'):
        data = self.fetched_data

        if view == 'dict':
            self.result = self._make_dict_result_view(data)
        elif view == 'list':
            self.result = self._make_list_result_view(data)

        return {
            'success': True,
            'result': self.result
        }

    def _parse_query(self, sql):
        def get_table_alias(table_obj):
            if table_obj.alias is not None:
                return table_obj.alias
            return '.'.join(table_obj.parts)

        mindsdb_sql_struct = parse_sql(sql)

        integrations_names = self.datahub.get_integrations_names()

        plan = plan_query(mindsdb_sql_struct, integrations=integrations_names, predictor_namespace=self.database)
        steps_data = []
        for i, step in enumerate(plan.steps):
            data = []
            if isinstance(step, FetchDataframeStep):
                dn = self.datahub.get(step.integration)
                query = step.query

                data = dn.select_query(
                    query=query
                )
                table_alias = get_table_alias(step.query.from_table)
                data = [{table_alias: x} for x in data]
            elif isinstance(step, ApplyPredictorStep):
                dn = self.datahub.get('mindsdb')
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
                data = dn.select(
                    table=step.predictor,
                    columns=None,
                    where_data=where_data,
                    where={}
                )
                data = [{step.predictor: x} for x in data]
            elif isinstance(step, JoinStep):
                left_data = steps_data[step.left.step_num]
                right_data = steps_data[step.right.step_num]
                if step.query.condition is None:
                    # line-to-line join
                    if len(left_data) != len(right_data):
                        raise Exception('wrong data length')
                    data = []
                    for i in range(len(left_data)):
                        data.append({
                            step.query.left.alias: left_data[i][step.query.left.alias],
                            step.query.right.alias: right_data[i][step.query.right.alias]
                        })
                else:
                    raise Exception('Unknown join type')
            elif isinstance(step, FilterStep):
                x = 1
                pass
            elif isinstance(step, ProjectStep):
                step_data = steps_data[step.dataframe.step_num]
                row = step_data[0]  # TODO if rowcount = 0
                columns_list = []
                for column in step.columns:
                    table_name = None
                    column_name = column
                    if '.' in column:
                        name_parts = column.split('.')
                        if len(name_parts) > 2:
                            raise Exception('at this moment only 2 parts name supports')
                        table_name = name_parts[0]
                        column_name = name_parts[1]

                    # TODO check columns exists
                    if column_name == '*':
                        if table_name is None:
                            for tn in row.keys():
                                for key in row[tn].keys():
                                    columns_list.append({
                                        'table_name': tn,
                                        'column_name': key
                                    })
                        else:
                            for key in row[table_name].keys():
                                columns_list.append({
                                    'table_name': table_name,
                                    'column_name': key
                                })
                    else:
                        if table_name is not None:
                            columns_list.append({
                                'table_name': table_name,
                                'column_name': column_name
                            })
                        else:
                            for tn in row.keys():
                                if column in row[tn]:
                                    columns_list.append({
                                        'table_name': tn,
                                        'column_name': column
                                    })
                                    break
                            else:
                                raise Exception(f'can not find column with name {column}')
                column_list_dict_view = {}
                for column_record in columns_list:
                    column_list_dict_view[column_record['table_name']] = column_list_dict_view.get(column_record['table_name'], [])
                    column_list_dict_view[column_record['table_name']].append(column_record['column_name'])

                # TODO fix it
                for column_record in columns_list:
                    if column_record['column_name'] in step.aliases:
                        column_record['column_alias'] = step.aliases[column_record['column_name']]
                    elif f"{column_record['table_name']}.{column_record['column_name']}" in step.aliases:
                        column_record['column_alias'] = step.aliases[f"{column_record['table_name']}.{column_record['column_name']}"]

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

    def _make_dict_result_view(self, data):
        result = []
        columns = self.columns_list
        for record in data:
            row = {}
            for col in columns:
                col_name = f"{col['table_name']}.{col['column_name']}"
                table_record = record[col_name]
                row[col['name']] = table_record[col['name']]
            result.append(row)

        return result

    def _make_list_result_view(self, data):
        result = []
        for row in data:
            data_row = []
            for column_record in self.columns_list:
                data_row.append(row[column_record['table_name']][column_record['column_name']])
            result.append(data_row)
        return result

    @property
    def columns(self):
        result = []
        for column_record in self.columns_list:
            result.append({
                'database': self.database or 'mindsdb',  # TODO
                'table_name': column_record['table_name'],  # TODO
                'name': column_record['column_name'],
                'alias': column_record.get('column_alias', column_record['column_name']),
                # NOTE all work with text-type, but if/when wanted change types to real,
                # it will need to check all types casts in BinaryResultsetRowPacket
                'type': TYPES.MYSQL_TYPE_VAR_STRING
            })
        return result
