from mindsdb_sql_parser.ast import Identifier, Operation, BinaryOperation, BetweenOperation, OrderBy

from mindsdb.api.executor.planner.exceptions import PlanningException


def find_time_filter(op, time_column_name):
    if not op:
        return
    if op.op == 'and':
        left = find_time_filter(op.args[0], time_column_name)
        right = find_time_filter(op.args[1], time_column_name)
        if left and right:
            raise PlanningException('Can provide only one filter by predictor order_by column, found two')

        return left or right
    elif ((isinstance(op.args[0], Identifier) and op.args[0].parts[-1].lower() == time_column_name.lower())
          or (isinstance(op.args[1], Identifier) and op.args[1].parts[-1].lower() == time_column_name.lower())):
        return op


def replace_time_filter(op, time_filter, new_filter):
    if op == time_filter:
        return new_filter
    if isinstance(op, BinaryOperation):
        op.args[0] = replace_time_filter(op.args[0], time_filter, new_filter)
        op.args[1] = replace_time_filter(op.args[1], time_filter, new_filter)
    return op


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
            # remove alias
            arg.parts = [arg.parts[-1]]

    if isinstance(op.args[0], Operation):
        validate_ts_where_condition(op.args[0], allowed_columns, allow_and=True)
    if isinstance(op.args[1], Operation):
        validate_ts_where_condition(op.args[1], allowed_columns, allow_and=True)


def recursively_check_join_identifiers_for_ambiguity(item, aliased_fields=None):
    if item is None:
        return
    elif isinstance(item, Identifier):
        if len(item.parts) == 1:
            if aliased_fields is not None and item.parts[0] in aliased_fields:
                # is alias
                return
            raise PlanningException(f'Ambigous identifier {str(item)}, provide table name for operations on a join.')
    elif isinstance(item, Operation):
        recursively_check_join_identifiers_for_ambiguity(item.args, aliased_fields=aliased_fields)
    elif isinstance(item, OrderBy):
        recursively_check_join_identifiers_for_ambiguity(item.field, aliased_fields=aliased_fields)
    elif isinstance(item, list):
        for arg in item:
            recursively_check_join_identifiers_for_ambiguity(arg, aliased_fields=aliased_fields)
