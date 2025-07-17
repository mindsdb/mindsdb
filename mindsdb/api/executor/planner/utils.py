import copy
from typing import List

from mindsdb_sql_parser.ast import Identifier, Select, BinaryOperation, Constant, Parameter
from mindsdb_sql_parser import ast

from mindsdb.integrations.utilities.query_traversal import query_traversal
from mindsdb.api.executor.planner.exceptions import PlanningException


def get_predictor_name_identifier(identifier):
    new_identifier = copy.deepcopy(identifier)
    if len(new_identifier.parts) > 1:
        new_identifier.parts.pop(0)
    return new_identifier


def disambiguate_predictor_column_identifier(identifier, predictor):
    """Removes integration name from column if it's present, adds table path if it's absent"""
    table_ref = predictor.alias.parts_to_str() if predictor.alias else predictor.parts_to_str()
    parts = list(identifier.parts)
    if parts[0] == table_ref:
        parts = parts[1:]

    new_identifier = Identifier(parts=parts)
    return new_identifier


def recursively_extract_column_values(op, row_dict, predictor):
    if isinstance(op, BinaryOperation) and op.op == '=':
        id = op.args[0]
        value = op.args[1]

        if not (
            isinstance(id, Identifier)
            and (isinstance(value, Constant) or isinstance(value, Parameter))
        ):
            raise PlanningException(f'The WHERE clause for selecting from a predictor'
                                    f' must contain pairs \'Identifier(...) = Constant(...)\','
                                    f' found instead: {id.to_tree()}, {value.to_tree()}')

        id = disambiguate_predictor_column_identifier(id, predictor)

        if str(id) in row_dict:
            raise PlanningException(f'Multiple values provided for {str(id)}')
        if isinstance(value, Constant):
            value = value.value
        row_dict[str(id)] = value
    elif isinstance(op, BinaryOperation) and op.op == 'and':
        recursively_extract_column_values(op.args[0], row_dict, predictor)
        recursively_extract_column_values(op.args[1], row_dict, predictor)
    else:
        raise PlanningException(f'Only \'and\' and \'=\' operations allowed in WHERE clause, found: {op.to_tree()}')


def get_deepest_select(select):
    if not select.from_table or not isinstance(select.from_table, Select):
        return select
    return get_deepest_select(select.from_table)


def convert_join_to_list(join):
    # join tree to table list

    if isinstance(join.right, ast.Join):
        raise NotImplementedError('Wrong join AST')

    items = []

    if isinstance(join.left, ast.Join):
        # dive to next level
        items.extend(convert_join_to_list(join.left))
    else:
        # this is first table
        items.append(dict(
            table=join.left
        ))

    # all properties set to right table
    items.append(dict(
        table=join.right,
        join_type=join.join_type,
        is_implicit=join.implicit,
        condition=join.condition
    ))

    return items


def get_query_params(query):
    # find all parameters
    params = []

    def params_find(node, **kwargs):
        if isinstance(node, ast.Parameter):
            params.append(node)
            return node

    query_traversal(query, params_find)
    return params


def fill_query_params(query, params):

    params = copy.deepcopy(params)

    def params_replace(node, **kwargs):
        if isinstance(node, ast.Parameter):
            value = params.pop(0)
            return ast.Constant(value)

    # put parameters into query
    query_traversal(query, params_replace)

    return query


def filters_to_bin_op(filters: List[BinaryOperation]):
    # make a new where clause without params
    where = None
    for flt in filters:
        if where is None:
            where = flt
        else:
            where = BinaryOperation(op='and', args=[where, flt])
    return where
