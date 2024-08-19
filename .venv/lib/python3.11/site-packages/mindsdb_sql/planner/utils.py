import copy
from typing import List

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import (Identifier, Operation, Star, Select, BinaryOperation, Constant,
                                    OrderBy, UnaryOperation, NullConstant, TypeCast, Parameter)
from mindsdb_sql.parser import ast


# def get_integration_path_from_identifier(identifier):
#     parts = identifier.parts
#     integration_name = parts[0]
#     new_parts = parts[1:]
#
#     if len(parts) == 1:
#         raise PlanningException(f'No integration specified for table: {str(identifier)}')
#     elif len(parts) > 4:
#         raise PlanningException(f'Too many parts (dots) in table identifier: {str(identifier)}')
#
#     new_identifier = copy.deepcopy(identifier)
#     new_identifier.parts = new_parts
#
#     return integration_name, new_identifier


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

        # if (
        #     isinstance(value, UnaryOperation)
        #     and value.op == '-'
        #     and isinstance(value.args[0], Constant)
        # ):
        #     value = Constant(-value.args[0].value)

        if not (
                isinstance(id, Identifier)
                and
                (isinstance(value, Constant) or isinstance(value, Parameter))
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


def query_traversal(node, callback, is_table=False, is_target=False, parent_query=None):
    '''
    :param node: element
    :param callback: function applied to every element
    :param is_table: it is table in query
    :param is_target: it is the target in select
    :param parent_query: current query (select/update/create/...) where we are now
    :return:
       new element if it is needed to be replaced
       or None to keep element and traverse over it
    '''
    # traversal query tree to find and replace nodes

    res = callback(node, is_table=is_table, is_target=is_target, parent_query=parent_query)
    if res is not None:
        # node is going to be replaced
        return res

    if isinstance(node, ast.Select):
        if node.from_table is not None:
            node_out = query_traversal(node.from_table, callback, is_table=True, parent_query=node)
            if node_out is not None:
                node.from_table = node_out

        array = []
        for node2 in node.targets:
            node_out = query_traversal(node2, callback, parent_query=node, is_target=True) or node2
            if isinstance(node_out, list):
                array.extend(node_out)
            else:
                array.append(node_out)
        node.targets = array

        if node.cte is not None:
            array = []
            for cte in node.cte:
                node_out = query_traversal(cte.query, callback, parent_query=node) or cte
                array.append(node_out)
            node.cte = array

        if node.where is not None:
            node_out = query_traversal(node.where, callback, parent_query=node)
            if node_out is not None:
                node.where = node_out

        if node.group_by is not None:
            array = []
            for node2 in node.group_by:
                node_out = query_traversal(node2, callback, parent_query=node) or node2
                array.append(node_out)
            node.group_by = array

        if node.having is not None:
            node_out = query_traversal(node.having, callback, parent_query=node)
            if node_out is not None:
                node.having = node_out

        if node.order_by is not None:
            array = []
            for node2 in node.order_by:
                node_out = query_traversal(node2, callback, parent_query=node) or node2
                array.append(node_out)
            node.order_by = array

    elif isinstance(node, ast.Union):
        node_out = query_traversal(node.left, callback, parent_query=node)
        if node_out is not None:
            node.left = node_out
        node_out = query_traversal(node.right, callback, parent_query=node)
        if node_out is not None:
            node.right = node_out

    elif isinstance(node, ast.Join):
        node_out = query_traversal(node.right, callback, is_table=True, parent_query=parent_query)
        if node_out is not None:
            node.right = node_out
        node_out = query_traversal(node.left, callback, is_table=True, parent_query=parent_query)
        if node_out is not None:
            node.left = node_out
        if node.condition is not None:
            node_out = query_traversal(node.condition, callback, parent_query=parent_query)
            if node_out is not None:
                node.condition = node_out

    elif isinstance(node, ast.Function) \
            or isinstance(node, ast.BinaryOperation)\
            or isinstance(node, ast.UnaryOperation) \
            or isinstance(node, ast.BetweenOperation):
        array = []
        for arg in node.args:
            node_out = query_traversal(arg, callback, parent_query=parent_query) or arg
            array.append(node_out)
        node.args = array

    elif isinstance(node, ast.WindowFunction):
        query_traversal(node.function, callback, parent_query=parent_query)
        if node.partition is not None:
            array = []
            for node2 in node.partition:
                node_out = query_traversal(node2, callback, parent_query=parent_query) or node2
                array.append(node_out)
            node.partition = array
        if node.order_by is not None:
            array = []
            for node2 in node.order_by:
                node_out = query_traversal(node2, callback, parent_query=parent_query) or node2
                array.append(node_out)
            node.order_by = array

    elif isinstance(node, ast.TypeCast):
        node_out = query_traversal(node.arg, callback, parent_query=parent_query)
        if node_out is not None:
            node.arg = node_out

    elif isinstance(node, ast.Tuple):
        array = []
        for node2 in node.items:
            node_out = query_traversal(node2, callback, parent_query=parent_query) or node2
            array.append(node_out)
        node.items = array

    elif isinstance(node, ast.Insert):
        if node.table is not None:
            node_out = query_traversal(node.table, callback, is_table=True, parent_query=node)
            if node_out is not None:
                node.table = node_out

        if node.values is not None:
            rows = []
            for row in node.values:
                items = []
                for item in row:
                    item2 = query_traversal(item, callback, parent_query=node) or item
                    items.append(item2)
                rows.append(items)
            node.values = rows

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback, parent_query=node)
            if node_out is not None:
                node.from_select = node_out

    elif isinstance(node, ast.Update):
        if node.table is not None:
            node_out = query_traversal(node.table, callback, is_table=True, parent_query=node)
            if node_out is not None:
                node.table = node_out

        if node.where is not None:
            node_out = query_traversal(node.where, callback, parent_query=node)
            if node_out is not None:
                node.where = node_out

        if node.update_columns is not None:
            changes = {}
            for k, v in node.update_columns.items():
                v2 = query_traversal(v, callback, parent_query=node)
                if v2 is not None:
                    changes[k] = v2
            if changes:
                node.update_columns.update(changes)

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback, parent_query=node)
            if node_out is not None:
                node.from_select = node_out

    elif isinstance(node, ast.CreateTable):
        array = []
        if node.columns is not None:
            for node2 in node.columns:
                node_out = query_traversal(node2, callback, parent_query=node) or node2
                array.append(node_out)
            node.columns = array

        if node.name is not None:
            node_out = query_traversal(node.name, callback, is_table=True, parent_query=node)
            if node_out is not None:
                node.name = node_out

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback, parent_query=node)
            if node_out is not None:
                node.from_select = node_out

    elif isinstance(node, ast.Delete):
        if node.where is not None:
            node_out = query_traversal(node.where, callback, parent_query=node)
            if node_out is not None:
                node.where = node_out

    elif isinstance(node, ast.OrderBy):
        if node.field is not None:
            node_out = query_traversal(node.field, callback, parent_query=parent_query)
            if node_out is not None:
                node.field = node_out

    elif isinstance(node, ast.Case):
        rules = []
        for condition, result in node.rules:
            condition2 = query_traversal(condition, callback, parent_query=parent_query)
            result2 = query_traversal(result, callback, parent_query=parent_query)

            condition = condition if condition2 is None else condition2
            result = result if result2 is None else result2
            rules.append([condition, result])
        node.rules = rules
        default = query_traversal(node.default, callback, parent_query=parent_query)
        if default is not None:
            node.default = default

    elif isinstance(node, list):
        array = []
        for node2 in node:
            node_out = query_traversal(node2, callback, parent_query=parent_query) or node2
            array.append(node_out)
        return array

    # keep original node
    return None


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