import datetime as dt
import re

import dateinfer
import pandas as pd

from mindsdb_sql.parser.ast import (
    BinaryOperation,
    Identifier,
    Constant,
    Latest,
    BetweenOperation,
    Parameter,
)
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (
    ApplyTimeseriesPredictorStep,
    ApplyPredictorRowStep,
    ApplyPredictorStep,
)

from mindsdb.api.executor.sql_query.result_set import ResultSet, Column
from mindsdb.utilities.cache import get_cache, json_checksum

from .base import BaseStepCall


def get_preditor_alias(step, mindsdb_database):
    predictor_name = '.'.join(step.predictor.parts)
    predictor_alias = '.'.join(step.predictor.alias.parts) if step.predictor.alias is not None else predictor_name
    return (mindsdb_database, predictor_name, predictor_alias)


class ApplyPredictorRowStepCall(BaseStepCall):

    bind = ApplyPredictorRowStep

    def call(self, step):
        project_name = step.namespace
        predictor_name = step.predictor.parts[0]
        where_data0 = step.row_dict
        project_datanode = self.session.datahub.get(project_name)

        # fill params
        where_data = {}
        for key, value in where_data0.items():
            if isinstance(value, Parameter):
                rs = self.steps_data[value.value.step_num]
                if rs.length() == 1:
                    # one value, don't do list
                    value = rs.get_records_raw()[0][0]
                else:
                    value = [i[0] for i in rs.get_records_raw()]
            where_data[key] = value

        version = None
        if len(step.predictor.parts) > 1 and step.predictor.parts[-1].isdigit():
            version = int(step.predictor.parts[-1])

        predictions = project_datanode.predict(
            model_name=predictor_name,
            data=where_data,
            version=version,
            params=step.params,
        )

        columns_dtypes = dict(predictions.dtypes)
        predictions = predictions.to_dict(orient='records')

        # update predictions with input data
        for row in predictions:
            for k, v in where_data.items():
                if k not in row:
                    row[k] = v

        table_name = get_preditor_alias(step, self.context.get('database'))

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

        return result


class ApplyPredictorStepCall(BaseStepCall):

    bind = ApplyPredictorStep

    def call(self, step):
        # set row_id
        data = self.steps_data[step.dataframe.step_num]

        params = step.params or {}

        for table in data.get_tables()[:1]:  # add  __mindsdb_row_id only for first table
            row_id_col = Column(
                name='__mindsdb_row_id',
                database=table['database'],
                table_name=table['table_name'],
                table_alias=table['table_alias']
            )

            row_id = self.context.get('row_id')
            values = list(range(row_id, row_id + data.length()))
            data.add_column(row_id_col, values)
            self.context['row_id'] += data.length()

        project_name = step.namespace
        predictor_name = step.predictor.parts[0]

        where_data = data.get_records()

        # add constants from where
        row_dict = {}
        if step.row_dict is not None:
            for k, v in step.row_dict.items():
                if isinstance(v, Result):
                    prev_result = self.steps_data[v.step_num]
                    # TODO we await only one value: model.param = (subselect)
                    v = prev_result.get_records_raw()[0][0]
                row_dict[k] = v

            for record in where_data:
                record.update(row_dict)

        predictor_metadata = {}
        for pm in self.context['predictor_metadata']:
            if pm['name'] == predictor_name and pm['integration_name'].lower() == project_name:
                predictor_metadata = pm
                break
        is_timeseries = predictor_metadata['timeseries']
        _mdb_forecast_offset = None
        if is_timeseries:
            if '> LATEST' in self.context['query_str']:
                # stream mode -- if > LATEST, forecast starts on inferred next timestamp
                _mdb_forecast_offset = 1
            elif '= LATEST' in self.context['query_str']:
                # override: when = LATEST, forecast starts on last provided timestamp instead of inferred next time
                _mdb_forecast_offset = 0
            else:
                # normal mode -- emit a forecast ($HORIZON data points on each) for each provided timestamp
                params['force_ts_infer'] = True
                _mdb_forecast_offset = None
            for row in where_data:
                if '__mdb_forecast_offset' not in row:
                    row['__mdb_forecast_offset'] = _mdb_forecast_offset

        # for row in where_data:
        #     for key in row:
        #         if isinstance(row[key], datetime.date):
        #             row[key] = str(row[key])

        table_name = get_preditor_alias(step, self.context['database'])
        result = ResultSet()
        result.is_prediction = True

        project_datanode = self.session.datahub.get(project_name)
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

            if self.session.predictor_cache is False:
                data = None
            else:
                predictor_cache = get_cache('predict')
                data = predictor_cache.get(key)

            if data is None:
                version = None
                if len(step.predictor.parts) > 1 and step.predictor.parts[-1].isdigit():
                    version = int(step.predictor.parts[-1])
                predictions = project_datanode.predict(
                    model_name=predictor_name,
                    data=where_data,
                    version=version,
                    params=params
                )
                data = predictions.to_dict(orient='records')
                columns_dtypes = dict(predictions.dtypes)

                if data is not None and isinstance(data, list) and self.session.predictor_cache is not False:
                    predictor_cache.set(key, data)
            else:
                columns_dtypes = {}
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

        return result

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
                # ('%Y-%m-%d %H:%M:%S%z', r'[\d]{4}-[\d]{2}-[\d]{2} [\d]{2}:[\d]{2}:[\d]{2}\+[\d]{2}:[\d]{2}'),
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

        model_types = predictor_metadata['model_types']
        if model_types.get(order_col) in ('float', 'integer'):
            # convert strings to digits
            fnc = {
                'integer': int,
                'float': float
            }[model_types[order_col]]

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

        if model_types.get(order_col) in ('date', 'datetime') or isinstance(predictor_data[0][order_col], pd.Timestamp):  # noqa
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


class ApplyTimeseriesPredictorStepCall(ApplyPredictorStepCall):

    bind = ApplyTimeseriesPredictorStep
