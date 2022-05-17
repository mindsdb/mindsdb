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
    FilterStep,
    UnionStep,
    JoinStep
)

from mindsdb.api.mysql.mysql_proxy.utilities import (
    SqlApiException,
    ErBadDbError,
    ErBadTableError,
    ErKeyColumnDoesNotExist,
    ErTableExistError,
    ErDubFieldName,
    ErDbDropDelete,
    ErNonInsertableTable,
    ErNotSupportedYet,
)


def execute_step(step, steps_data, data_handler, predictor):
    if type(step) == GetPredictorColumns:
        raise Exception(f'Step {type(step)} not supported yet')
    elif type(step) == GetTableColumns:
        raise Exception(f'Step {type(step)} not supported yet')
    elif type(step) == FetchDataframeStep:
        data = _fetch_dataframe_step(step, data_handler)
    elif type(step) == UnionStep:
        raise ErNotSupportedYet('Union step is not implemented')
    elif type(step) == MapReduceStep:
        # return steps_data[-1]
        try:
            if step.reduce != 'union':
                raise Exception(f'Unknown MapReduceStep type: {step.reduce}')

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
                    sub_data = _fetch_dataframe_step(substep)
                    if len(data['columns']) == 0:
                        data['columns'] = sub_data['columns']
                    if len(data['tables']) == 0:
                        data['tables'] = sub_data['tables']
                    data['values'].extend(sub_data['values'])
                    unmarkQueryVar(query.where)
            elif type(substep) == MultipleSteps:
                data = _multiple_steps_reduce(substep, vars)
            else:
                raise Exception(f'Unknown step type: {step.step}')
        except Exception as e:
            raise SqlApiException(f'error in map reduce step: {e}') from e
    elif type(step) == MultipleSteps:
        if step.reduce != 'union':
            raise Exception(f"Only MultipleSteps with type = 'union' is supported. Got '{step.type}'")
        data = None
        for substep in step.steps:
            subdata = execute_step(substep, steps_data, data_handler, predictor)
            if data is None:
                data = subdata
            else:
                data['values'].extend(subdata['values'])
    elif type(step) == ApplyPredictorRowStep:
        # return steps_data[-1]
        try:
            predictor = '.'.join(step.predictor.parts)
            dn = datahub.get(mindsdb_database_name)
            where_data = step.row_dict

            data = dn.select(
                table=predictor,
                columns=None,
                where_data=where_data,
                integration_name=session.integration,
                integration_type=session.integration_type
            )

            data = [{(key, key): value for key, value in row.items()} for row in data]

            table_name = get_preditor_alias(step, database)
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
            if type(e) == SqlApiException:
                raise e
            else:
                raise SqlApiException(f'error in apply predictor row step: {e}') from e
    elif type(step) in (ApplyPredictorStep, ApplyTimeseriesPredictorStep):
        try:
            is_timeseries = planner.predictor_metadata[predictor]['timeseries']
            _mdb_make_predictions = None
            if is_timeseries:
                if 'LATEST' in query_str:
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
                integration_name=session.integration,
                integration_type=session.integration_type
            )

        except Exception as e:
            raise SqlApiException(f'error in apply predictor step: {e}') from e
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
            raise SqlApiException(f'error in join step: {e}') from e

    elif type(step) == FilterStep:
        raise ErNotSupportedYet('FilterStep is not implemented')
    elif type(step) == LimitOffsetStep:
        try:
            step_data = steps_data[step.dataframe.step_num]
            data = {
                'values': step_data['values'].copy(),
                'columns': step_data['columns'].copy(),
                'tables': step_data['tables'].copy()
            }
            if isinstance(step.offset, Constant) and isinstance(step.offset.value, int):
                data['values'] = data['values'][step.offset.value:]
            if isinstance(step.limit, Constant) and isinstance(step.limit.value, int):
                data['values'] = data['values'][:step.limit.value]
        except Exception as e:
            raise SqlApiException(f'error in limit offset step: {e}') from e
    elif type(step) == ProjectStep:
        try:
            step_data = steps_data[step.dataframe.step_num]
            columns_list = []
            for column_identifier in step.columns:
                table_name = None
                if type(column_identifier) == Star:
                    for table_name, table_columns_list in step_data['columns'].items():
                        for column in table_columns_list:
                            columns_list.append(
                                Column(database=table_name[0],
                                       table_name=table_name[1],
                                       table_alias=table_name[2],
                                       name=column[0],
                                       alias=column[1])
                            )
                elif type(column_identifier) == Identifier:
                    column_name_parts = column_identifier.parts
                    column_alias = column_identifier.parts[-1] if column_identifier.alias is None else '.'.join(
                        column_identifier.alias.parts)
                    if len(column_name_parts) > 2:
                        raise Exception(
                            f'Column name must contain no more than 2 parts. Got name: {column_identifier}')
                    elif len(column_name_parts) == 1:
                        column_name = column_name_parts[0]

                        appropriate_table = None
                        if len(step_data['tables']) == 1:
                            appropriate_table = step_data['tables'][0]
                        else:
                            for table_name, table_columns in step_data['columns'].items():
                                table_column_names_list = [x[1] or x[0] for x in table_columns]
                                column_exists = get_column_in_case(table_column_names_list, column_name)
                                if column_exists:
                                    if appropriate_table is not None:
                                        raise Exception(
                                            'Found multiple appropriate tables for column {column_name}')
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
                            # FIXME: must be exception
                            columns_list.append(
                                Column(database=appropriate_table[0],
                                       table_name=appropriate_table[1],
                                       table_alias=appropriate_table[2],
                                       name=column_alias)
                            )
                        else:
                            columns_list.append(
                                Column(database=appropriate_table[0],
                                       table_name=appropriate_table[1],
                                       table_alias=appropriate_table[2],
                                       name=column_name,
                                       alias=column_alias))  # column_name
                    elif len(column_name_parts) == 2:
                        table_name_or_alias = column_name_parts[0]
                        column_name = column_name_parts[1]

                        appropriate_table = None
                        for table_name, table_columns in step_data['columns'].items():
                            table_column_names_list = [x[1] or x[0] for x in table_columns]
                            checkig_table_name_or_alias = table_name[2] or table_name[1]
                            if table_name_or_alias.lower() == checkig_table_name_or_alias.lower():
                                column_exists = get_column_in_case(table_column_names_list, column_name)
                                if column_exists:
                                    appropriate_table = table_name
                                    break
                                else:
                                    raise Exception(f'Can not find column "{column_name}" in table "{table_name}"')
                        if appropriate_table is None:
                            raise SqlApiException(f'Can not find appropriate table for column {column_name}')

                        columns_to_copy = None
                        table_column_names_list = [x[1] or x[0] for x in table_columns]
                        checking_name = get_column_in_case(table_column_names_list, column_name)
                        for column in step_data['columns'][appropriate_table]:
                            if column[0] == checking_name and (column[1] is None or column[1] == checking_name):
                                columns_to_copy = column
                                break
                        else:
                            raise ErKeyColumnDoesNotExist(
                                f'Can not find appropriate column in data: {(column_name, column_alias)}')

                        for row in step_data['values']:
                            row[appropriate_table][(column_name, column_alias)] = row[appropriate_table][
                                columns_to_copy]

                        columns_list.append(
                            Column(database=appropriate_table[0],
                                   table_name=appropriate_table[1],
                                   table_alias=appropriate_table[2],
                                   name=column_name,
                                   alias=column_alias)
                        )
                    else:
                        raise SqlApiException('Undefined column name')

                    # if column not exists in result - copy value to it
                    if (column_name, column_alias) not in step_data['columns'][appropriate_table]:
                        step_data['columns'][appropriate_table].append((column_name, column_alias))
                        for row in step_data['values']:
                            if (column_name, column_alias) not in row[appropriate_table]:
                                try:
                                    row[appropriate_table][(column_name, column_alias)] = row[appropriate_table][
                                        (column_name, column_name)]
                                except KeyError:
                                    raise ErKeyColumnDoesNotExist(f'Unknown column: {column_name}')

                else:
                    raise ErKeyColumnDoesNotExist(f'Unknown column type: {column_identifier}')

            columns_list = columns_list
            data = step_data
        except Exception as e:
            if isinstance(e, SqlApiException):
                raise e
            raise SqlApiException(f'error on project step: {e} ') from e

    elif type(step) == SaveToTable:
        step_data = step.dataframe.result_data
        integration_name = step.table.parts[0]
        table_name_parts = step.table.parts[1:]

        dn = datahub.get(integration_name)

        if hasattr(dn, 'create_table') is False:
            raise Exception(f"Creating table in '{integration_name}' is not supporting")

        # region del 'service' columns
        for table in step_data['columns']:
            new_table_columns = []
            for column in step_data['columns'][table]:
                if column[-1] not in ('__mindsdb_row_id', '__mdb_make_predictions'):
                    new_table_columns.append(column)
            step_data['columns'][table] = new_table_columns
        # endregion

        # region del columns filtered at projection step
        filtered_column_names = [x.name for x in columns_list]
        for table in step_data['columns']:
            new_table_columns = []
            for column in step_data['columns'][table]:
                if column[0].startswith('predictor.'):
                    new_table_columns.append(column)
                elif column[0] in filtered_column_names:
                    new_table_columns.append(column)
            step_data['columns'][table] = new_table_columns
        # endregion

        dn.create_table(table_name_parts=table_name_parts, columns=step_data['columns'], data=step_data['values'])
        data = None
    else:
        raise SqlApiException(F'Unknown planner step: {step}')
    return data


def _fetch_dataframe_step(step, data_handler):
    values = data_handler.query(step.query)['data_frame']
    data = {
        'values': values,
        'columns': {str(step.query.from_table.alias): values.columns},
        'tables': [step.query.from_table.parts[0]]
    }
    return data

