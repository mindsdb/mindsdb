import pandas as pd

from ts_utils import validate_ts_where_condition, find_time_filter, add_order_not_null, replace_time_filter, find_and_remove_time_filter, ts_select_dispatch, plan_fetch_timeseries_partitions

from mindsdb_sql.parser.ast import Identifier, Constant, Operation, Select, BinaryOperation, BetweenOperation
from mindsdb_sql.parser.ast import OrderBy, GroupBy


def get_join_input(query, model, data_handler):
    data_handler_table = getattr(query.from_table, data_clause).parts[-1]
    data_handler_cols = list(set([t.parts[-1] for t in query.targets]))

    data_query = Select(
        targets=[Identifier(col) for col in data_handler_cols],
        from_table=Identifier(data_handler_table),
        where=query.where,
        group_by=query.group_by,
        having=query.having,
        order_by=query.order_by,
        offset=query.offset,
        limit=query.limit
    )

    model_input = pd.DataFrame.from_records(
        data_handler.query(data_query)['data_frame']
    )

    return model_input


def get_ts_join_input(query, model, data_handler):
    if query.order_by:
        raise PlanningException(
            f'Can\'t provide ORDER BY to time series predictor join query. Found: {query.order_by}.')

    if query.group_by or query.having or query.offset:
        raise PlanningException(f'Unsupported query to timeseries predictor: {str(query)}')

    data_handler_table = getattr(query.from_table, data_clause).parts[-1]
    data_handler_cols = list(set([t.parts[-1] for t in query.targets]))

    oby_col = model.problem_definition.timeseries_settings.order_by[0]
    gby_col = [model.problem_definition.timeseries_settings.group_by[0]]  # todo add multiple group support

    # validate where of TODO subquery? or query?

    allowed_columns = oby_col
    if len(gby_col) > 0:
        allowed_columns += [i.lower() for i in gby_col]
    validate_ts_where_condition(query.where, allowed_columns=allowed_columns)

    time_filter = find_time_filter(query.where, time_column_name=oby_col)
    order_by = [OrderBy(Identifier(parts=[oby_col]), direction='DESC')]

    preparation_where = copy.deepcopy(query.where)
    preparation_where2 = copy.deepcopy(preparation_where)
    preparation_where = add_order_not_null(preparation_where)

    integration_selects = ts_selects_dispatch(time_filter) # TODO: Selects to data handler after building list

    partial_dfs = []
    for step in integration_selects:
        partial_dfs.append(pd.DataFrame.from_records(data_handler.query(step)['data_frame']))

    # Turn all these into direct executions
    if len(gby_col) == 0:
        # todo: pretty sure all of this IF can be stripped away!
        # ts query without grouping - one or multistep
        if len(integration_selects) == 1:
            select_partition_step = get_integration_select_step(integration_selects[0])  # TODO: where is it?
        else:
            select_partition_step = MultipleSteps(
                steps=[get_integration_select_step(s) for s in integration_selects], reduce='union')  # TODO: where is it?
    else:
        # todo: this one maybe not, ask andrey
        # inject $var to queries
        for integration_select in integration_selects:
            condition = integration_select.where
            for num, column in enumerate(gby_col):
                cond = BinaryOperation('=', args=[Identifier(column), Constant(f'$var[{column}]')])

                # join to main condition
                if condition is None:
                    condition = cond
                else:
                    condition = BinaryOperation('and', args=[condition, cond])

            integration_select.where = condition
        # one or multistep
        if len(integration_selects) == 1:
            select_partition_step = get_integration_select_step(integration_selects[0])
        else:
            select_partition_step = MultipleSteps(
                steps=[get_integration_select_step(s) for s in integration_selects], reduce='union')

        # get grouping values
        # TODO: this time filter removal also sounds like we need to keep
        no_time_filter_query = copy.deepcopy(query)
        no_time_filter_query.where = find_and_remove_time_filter(no_time_filter_query.where, time_filter)
        select_partitions_step = plan_fetch_timeseries_partitions(no_time_filter_query, table,
                                                                       gby_col)  # TODO: move query here, and execute directly

        # sub-query by every grouping value
        # TODO: replace with a pandas concatenation
        map_reduce_step = plan.add_step(
            MapReduceStep(values=select_partitions_step.result, reduce='union', step=select_partition_step))
        data_step = map_reduce_step


    # Original (working) code here:
    # 1) get all groups
    for col in [gby_col] + [oby_col]:
        if col not in data_handler_cols:
            data_handler_cols.append(col)

    groups_query = Select(
        targets=[Identifier(gby_col)],
        distinct=True,
        from_table=Identifier(data_handler_table),
    )
    groups = list(data_handler.query(groups_query)['data_frame'].squeeze().values)

    # 2) get LATEST available date and window for each group
    latests = {}
    windows = {}

    for group in groups:
        latest_oby_query = Select(
            targets=[Identifier(oby_col)],
            from_table=Identifier(data_handler_table),
            where=BinaryOperation(op='=',
                                  args=[
                                      Identifier(gby_col),
                                      Constant(group)
                                  ]),
            order_by=[OrderBy(
                field=Identifier(oby_col),
                direction='DESC'
            )],
            limit=Constant(1)
        )
        latests[group] = data_handler.query(latest_oby_query)['data_frame'].values[0][0]

        window_query =  Select(
            targets=[Identifier(col) for col in data_handler_cols],
            from_table=Identifier(data_handler_table),
            where=BinaryOperation(op='=',
                                  args=[
                                      Identifier(gby_col),
                                      Constant(group)
                                  ]),
        )
        # order and limit the df instead of SELECT to hedge against badly defined dtypes in the DB
        df = data_handler.query(window_query)['data_frame']

        if len(df) < window and not model.problem_definition.timeseries_settings.allow_incomplete_history:
            raise Exception(f"Not enough data for group {group}. Either pass more historical context or train a predictor with the `allow_incomplete_history` argument set to True.")
        df = df.sort_values(oby_col, ascending=False).iloc[0:window]
        windows[group] = df[::-1]

    # 3) concatenate all contexts into single data query
    model_input = pd.concat([v for k, v in windows.items()]).reset_index(drop=True)
    return model_input