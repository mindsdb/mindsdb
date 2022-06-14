from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.planner.utils import get_integration_path_from_identifier
from mindsdb_sql.parser.ast import Identifier, Operation, BinaryOperation, BetweenOperation, NullConstant, Latest, Select, Star, Constant


def get_time_selects(time_filter, table, window, order_by, preparation_where):
    # TODO: add tests for all these cases
    time_col = str(order_by[0].field)
    if isinstance(time_filter, BetweenOperation):
        between_from = time_filter.args[1]
        preparation_time_filter = BinaryOperation('<', args=[Identifier(time_col), between_from])
        preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
        integration_select_1 = Select(targets=[Star()],
                                      from_table=table,
                                      where=add_order_not_null(preparation_where2),
                                      order_by=order_by,
                                      limit=Constant(window))

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
                                    limit=Constant(window),
                                    )
        integration_select.where = find_and_remove_time_filter(integration_select.where, time_filter)
        integration_selects = [integration_select]

    elif isinstance(time_filter, BinaryOperation) and time_filter.op in ('>', '>='):
        time_filter_date = time_filter.args[1]
        preparation_time_filter_op = {'>': '<=', '>=': '<'}[time_filter.op]

        preparation_time_filter = BinaryOperation(preparation_time_filter_op,
                                                  args=[Identifier(time_col), time_filter_date])
        preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
        integration_select_1 = Select(targets=[Star()],
                                      from_table=table,
                                      where=add_order_not_null(preparation_where2),
                                      order_by=order_by,
                                      limit=Constant(window))

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


def find_time_filter(op, time_column_name):
    if not op:
        return
    if op.op == 'and':
        left = find_time_filter(op.args[0], time_column_name)
        right = find_time_filter(op.args[1], time_column_name)
        if left and right:
            raise PlanningException('Can provide only one filter by predictor order_by column, found two')

        return left or right
    elif ((isinstance(op.args[0], Identifier) and op.args[0].parts[-1].lower() == time_column_name.lower()) or
          (isinstance(op.args[1], Identifier) and op.args[1].parts[-1].lower() == time_column_name.lower())):
        return op


def replace_time_filter(op, time_filter, new_filter):
    if op == time_filter:
        return new_filter
    elif op.args[0] == time_filter:
        op.args[0] = new_filter
    elif op.args[1] == time_filter:
        op.args[1] = new_filter
    return op


def add_order_not_null(condition, time_column_name):
    order_field_not_null = BinaryOperation(op='is not', args=[
        Identifier(parts=[time_column_name]),
        NullConstant()
    ])
    if condition is not None:
        condition = BinaryOperation(op='and', args=[
            condition,
            order_field_not_null
        ])
    else:
        condition = order_field_not_null
    return condition


def find_and_remove_time_filter(op, time_filter):
    if isinstance(op, BinaryOperation) or isinstance(op, BetweenOperation):
        if op == time_filter:
            return None
        elif op.op == 'and':
            # TODO maybe OR operation too?

            # next level
            left_arg = find_and_remove_time_filter(op.args[0], time_filter)
            right_arg = find_and_remove_time_filter(op.args[1], time_filter)

            # if found in one arg return other
            if left_arg is None:
                return right_arg
            if right_arg is None:
                return left_arg

            op.args = [left_arg, right_arg]
            return op

    return op


def validate_ts_where_condition(op, allowed_columns, allow_and=True):
    """Error if the where condition caontains invalid ops, is nested or filters on some column that's not time or partition"""
    if not op:
        return
    allowed_ops = ['and', '>', '>=', '=', '<', '<=', 'between', 'in']
    if not allow_and:
        allowed_ops.remove('and')
    if op.op not in allowed_ops:
        raise PlanningException(
            f'For time series predictors only the following operations are allowed in WHERE: {str(allowed_ops)}, found instead: {str(op)}.')

    for arg in op.args:
        if isinstance(arg, Identifier):
            if arg.parts[-1].lower() not in allowed_columns:
                raise PlanningException(
                    f'For time series predictor only the following columns are allowed in WHERE: {str(allowed_columns)}, found instead: {str(arg)}.')

    if isinstance(op.args[0], Operation):
        validate_ts_where_condition(op.args[0], allowed_columns, allow_and=True)
    if isinstance(op.args[1], Operation):
        validate_ts_where_condition(op.args[1], allowed_columns, allow_and=True)
