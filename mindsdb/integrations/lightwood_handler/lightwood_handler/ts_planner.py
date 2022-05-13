from ts_planner_utils import validate_ts_where_condition, find_time_filter, add_order_not_null, replace_time_filter, find_and_remove_time_filter


def ts_join(query, integrations, predictor_namespace, predictor_metadata):
    predictor_name = predictor_metadata['model_name']
    predictor_steps = plan_timeseries_predictor(query, table, predictor_namespace, predictor)

    # Update reference
    _, table = self.get_integration_path_from_identifier_or_error(table)
    table_alias = table.alias or Identifier(table.to_string(alias=False).replace('.', '_'))

    left = Identifier(predictor_steps['predictor'].result.ref_name,
                       alias=predictor.alias or Identifier(predictor.to_string(alias=False)))
    right = Identifier(predictor_steps['data'].result.ref_name, alias=table_alias)

    if not predictor_is_left:
        # swap join
        left, right = right, left
    new_join = Join(left=left, right=right, join_type=join.join_type)

    left = predictor_steps['predictor'].result
    right = predictor_steps['data'].result
    if not predictor_is_left:
        # swap join
        left, right = right, left

    last_step = self.plan.add_step(JoinStep(left=left, right=right, query=new_join))

    # limit from timeseries
    if predictor_steps.get('saved_limit'):
        last_step = self.plan.add_step(LimitOffsetStep(dataframe=last_step.result,
                                                  limit=predictor_steps['saved_limit']))

    # TODO: 2) instead of planning steps, actually execute them and run predictor! refactor all logic in ts_planner.py


def plan_timeseries_predictor(query, table, predictor_namespace, predictor_metadata):
    if query.order_by:
        raise PlanningException(
            f'Can\'t provide ORDER BY to time series predictor, it will be taken from predictor settings. Found: {query.order_by}')  # noqa

    if query.group_by or query.having or query.offset:
        raise PlanningException(f'Unsupported query to timeseries predictor: {str(query)}')

    allowed_columns = predictor_metadata['order_by']
    if len(predictor_metadata['group_by']) > 0:
        allowed_columns += [i.lower() for i in predictor_metadata['group_by']]
    validate_ts_where_condition(query.where, allowed_columns=allowed_columns)

    time_filter = find_time_filter(query.where, time_column_name=predictor_time_column_name)

    order_by = [OrderBy(Identifier(parts=[predictor_time_column_name]), direction='DESC')]

    preparation_where = copy.deepcopy(query.where)

    # add {order_by_field} is not null
    preparation_where2 = copy.deepcopy(preparation_where)
    preparation_where = add_order_not_null(preparation_where)

    # TODO: Selects to actual data handler calls once the list is full
    # Obtain integration selects
    integration_selects = ts_selects_dispath(time_filter)

    if len(predictor_metadata['group_by']) == 0:
        # ts query without grouping - one or multistep
        if len(integration_selects) == 1:
            select_partition_step = get_integration_select_step(integration_selects[0])
        else:
            select_partition_step = MultipleSteps(
                steps=[get_integration_select_step(s) for s in integration_selects], reduce='union')

        # fetch data step
        data_step = plan.add_step(select_partition_step)
    else:
        # inject $var to queries
        for integration_select in integration_selects:
            condition = integration_select.where
            for num, column in enumerate(predictor_metadata['group_by']):
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
        no_time_filter_query = copy.deepcopy(query)
        no_time_filter_query.where = find_and_remove_time_filter(no_time_filter_query.where, time_filter)
        select_partitions_step = plan_fetch_timeseries_partitions(no_time_filter_query, table,
                                                                       predictor_metadata['group_by'])

        # sub-query by every grouping value
        map_reduce_step = plan.add_step(
            MapReduceStep(values=select_partitions_step.result, reduce='union', step=select_partition_step))
        data_step = map_reduce_step

    # predictor_step = plan.add_step(
    #     ApplyTimeseriesPredictorStep(
    #         output_time_filter=time_filter,
    #         namespace=predictor_namespace,
    #         dataframe=data_step.result,
    #         predictor=predictor,
    #     )
    # )

    return data_step, saved_limit  #, prediction_step?


def plan_fetch_timeseries_partitions(self, query, table, predictor_group_by_names):
    # TODO: call from LWhandler join so that this is the AST sent to the "query" method in the data_handler passed to join
    # TODO: this is the select partitions step
    targets = [
        Identifier(column)
        for column in predictor_group_by_names
    ]

    query = Select(
        distinct=True,
        targets=targets,
        from_table=table,
        where=query.where,
    )
    select_step = plan_integration_select(query)
    return select_step


def ts_selects_dispath(time_filter):
    if isinstance(time_filter, BetweenOperation):
        between_from = time_filter.args[1]
        preparation_time_filter = BinaryOperation('<', args=[Identifier(predictor_time_column_name), between_from])
        preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
        integration_select_1 = Select(targets=[Star()],
                                      from_table=table,
                                      where=add_order_not_null(preparation_where2),
                                      order_by=order_by,
                                      limit=Constant(predictor_window))

        integration_select_2 = Select(targets=[Star()],
                                      from_table=table,
                                      where=preparation_where,
                                      order_by=order_by)

        integration_selects = [integration_select_1, integration_select_2]
    elif isinstance(time_filter, BinaryOperation) and time_filter.op == '>' and time_filter.args[1] == Latest():
        integration_select = Select(targets=[Star()],
                                    from_table=table,
                                    where=preparation_where,
                                    order_by=order_by,
                                    limit=Constant(predictor_window),
                                    )
        integration_select.where = find_and_remove_time_filter(integration_select.where, time_filter)
        integration_selects = [integration_select]

    elif isinstance(time_filter, BinaryOperation) and time_filter.op in ('>', '>='):
        time_filter_date = time_filter.args[1]
        preparation_time_filter_op = {'>': '<=', '>=': '<'}[time_filter.op]

        preparation_time_filter = BinaryOperation(preparation_time_filter_op,
                                                  args=[Identifier(predictor_time_column_name), time_filter_date])
        preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
        integration_select_1 = Select(targets=[Star()],
                                      from_table=table,
                                      where=add_order_not_null(preparation_where2),
                                      order_by=order_by,
                                      limit=Constant(predictor_window))

        integration_select_2 = Select(targets=[Star()],
                                      from_table=table,
                                      where=preparation_where,
                                      order_by=order_by)

        integration_selects = [integration_select_1, integration_select_2]
    else:
        integration_select = Select(targets=[Star()],
                                    from_table=table,
                                    where=preparation_where,
                                    order_by=order_by,
                                    )
        integration_selects = [integration_select]

    return integration_selects
