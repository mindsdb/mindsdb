from mindsdb_sql_parser import ast


def query_traversal(node, callback, is_table=False, is_target=False, parent_query=None, stack=None):
    """
    :param node: element
    :param callback: function applied to every element
    :param is_table: it is table in query
    :param is_target: it is the target in select
    :param parent_query: current query (select/update/create/...) where we are now
    :return:
       new element if it is needed to be replaced
       or None to keep element and traverse over it

    Usage:
    Create callback function to check or replace nodes
    Example:
    ```python
    def remove_predictors(node, is_table, **kwargs):
        if is_table and isinstance(node, Identifier):
            if is_predictor(node):
                return Constant(None)

    utils.query_traversal(ast_query, remove_predictors)
    ```

    """

    if stack is None:
        stack = []

    res = callback(node, is_table=is_table, is_target=is_target, parent_query=parent_query, callstack=stack)
    stack2 = [node] + stack

    if res is not None:
        # node is going to be replaced
        return res

    if isinstance(node, ast.Select):
        if node.from_table is not None:
            node_out = query_traversal(node.from_table, callback, is_table=True, parent_query=node, stack=stack2)
            if node_out is not None:
                node.from_table = node_out

        array = []
        for node2 in node.targets:
            node_out = query_traversal(node2, callback, parent_query=node, is_target=True, stack=stack2) or node2
            if isinstance(node_out, list):
                array.extend(node_out)
            else:
                array.append(node_out)
        node.targets = array

        if node.cte is not None:
            array = []
            for cte in node.cte:
                node_out = query_traversal(cte.query, callback, parent_query=node, stack=stack2) or cte
                array.append(node_out)
            node.cte = array

        if node.where is not None:
            node_out = query_traversal(node.where, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.where = node_out

        if node.group_by is not None:
            array = []
            for node2 in node.group_by:
                node_out = query_traversal(node2, callback, parent_query=node, stack=stack2) or node2
                array.append(node_out)
            node.group_by = array

        if node.having is not None:
            node_out = query_traversal(node.having, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.having = node_out

        if node.order_by is not None:
            array = []
            for node2 in node.order_by:
                node_out = query_traversal(node2, callback, parent_query=node, stack=stack2) or node2
                array.append(node_out)
            node.order_by = array

    elif isinstance(node, (ast.Union, ast.Intersect, ast.Except)):
        node_out = query_traversal(node.left, callback, parent_query=node, stack=stack2)
        if node_out is not None:
            node.left = node_out
        node_out = query_traversal(node.right, callback, parent_query=node, stack=stack2)
        if node_out is not None:
            node.right = node_out

    elif isinstance(node, ast.Join):
        node_out = query_traversal(node.right, callback, is_table=True, parent_query=parent_query, stack=stack2)
        if node_out is not None:
            node.right = node_out
        node_out = query_traversal(node.left, callback, is_table=True, parent_query=parent_query, stack=stack2)
        if node_out is not None:
            node.left = node_out
        if node.condition is not None:
            node_out = query_traversal(node.condition, callback, parent_query=parent_query, stack=stack2)
            if node_out is not None:
                node.condition = node_out

    elif isinstance(node, (ast.Function, ast.BinaryOperation, ast.UnaryOperation, ast.BetweenOperation,
                           ast.Exists, ast.NotExists)):
        array = []
        for arg in node.args:
            node_out = query_traversal(arg, callback, parent_query=parent_query, stack=stack2) or arg
            array.append(node_out)
        node.args = array

        if isinstance(node, ast.Function):
            if node.from_arg is not None:
                node_out = query_traversal(node.from_arg, callback, parent_query=parent_query, stack=stack2)
                if node_out is not None:
                    node.from_arg = node_out

    elif isinstance(node, ast.WindowFunction):
        query_traversal(node.function, callback, parent_query=parent_query, stack=stack2)
        if node.partition is not None:
            array = []
            for node2 in node.partition:
                node_out = query_traversal(node2, callback, parent_query=parent_query, stack=stack2) or node2
                array.append(node_out)
            node.partition = array
        if node.order_by is not None:
            array = []
            for node2 in node.order_by:
                node_out = query_traversal(node2, callback, parent_query=parent_query, stack=stack2) or node2
                array.append(node_out)
            node.order_by = array

    elif isinstance(node, ast.TypeCast):
        node_out = query_traversal(node.arg, callback, parent_query=parent_query, stack=stack2)
        if node_out is not None:
            node.arg = node_out

    elif isinstance(node, ast.Tuple):
        array = []
        for node2 in node.items:
            node_out = query_traversal(node2, callback, parent_query=parent_query, stack=stack2) or node2
            array.append(node_out)
        node.items = array

    elif isinstance(node, ast.Insert):
        if node.table is not None:
            node_out = query_traversal(node.table, callback, is_table=True, parent_query=node, stack=stack2)
            if node_out is not None:
                node.table = node_out

        if node.values is not None:
            rows = []
            for row in node.values:
                items = []
                for item in row:
                    item2 = query_traversal(item, callback, parent_query=node, stack=stack2) or item
                    items.append(item2)
                rows.append(items)
            node.values = rows

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.from_select = node_out

    elif isinstance(node, ast.Update):
        if node.table is not None:
            node_out = query_traversal(node.table, callback, is_table=True, parent_query=node, stack=stack2)
            if node_out is not None:
                node.table = node_out

        if node.where is not None:
            node_out = query_traversal(node.where, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.where = node_out

        if node.update_columns is not None:
            changes = {}
            for k, v in node.update_columns.items():
                v2 = query_traversal(v, callback, parent_query=node, stack=stack2)
                if v2 is not None:
                    changes[k] = v2
            if changes:
                node.update_columns.update(changes)

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.from_select = node_out

    elif isinstance(node, ast.CreateTable):
        array = []
        if node.columns is not None:
            for node2 in node.columns:
                node_out = query_traversal(node2, callback, parent_query=node, stack=stack2) or node2
                array.append(node_out)
            node.columns = array

        if node.name is not None:
            node_out = query_traversal(node.name, callback, is_table=True, parent_query=node, stack=stack2)
            if node_out is not None:
                node.name = node_out

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.from_select = node_out

    elif isinstance(node, ast.Delete):
        if node.where is not None:
            node_out = query_traversal(node.where, callback, parent_query=node, stack=stack2)
            if node_out is not None:
                node.where = node_out

    elif isinstance(node, ast.OrderBy):
        if node.field is not None:
            node_out = query_traversal(node.field, callback, parent_query=parent_query, stack=stack2)
            if node_out is not None:
                node.field = node_out

    elif isinstance(node, ast.Case):
        rules = []
        for condition, result in node.rules:
            condition2 = query_traversal(condition, callback, parent_query=parent_query, stack=stack2)
            result2 = query_traversal(result, callback, parent_query=parent_query, stack=stack2)

            condition = condition if condition2 is None else condition2
            result = result if result2 is None else result2
            rules.append([condition, result])
        node.rules = rules
        default = query_traversal(node.default, callback, parent_query=parent_query, stack=stack2)
        if default is not None:
            node.default = default

    elif isinstance(node, list):
        array = []
        for node2 in node:
            node_out = query_traversal(node2, callback, parent_query=parent_query, stack=stack2) or node2
            array.append(node_out)
        return array

    # keep original node
    return None
