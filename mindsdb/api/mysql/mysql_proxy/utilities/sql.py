from mindsdb_sql.ast import Join, Identifier, BinaryOperation, Constant


def get_alias(element):
    if element.value == '*':
        return '*'
    return element.value if element.alias is None else element.alias


def identifier_to_dict(identifier):
    res = {
        'value': identifier.value,
        'name': get_alias(identifier)
    }
    return res


def where_to_dict(root):
    if isinstance(root, BinaryOperation):
        op = root.op.lower()
        if op == '=':
            op = 'eq'
        return {op: [where_to_dict(root.args[0]), where_to_dict(root.args[1])]}
    elif isinstance(root, Identifier):
        return root.value
    elif isinstance(root, Constant):
        if isinstance(root.value, str):
            return {'literal': root.value}
        else:
            return root.value
    else:
        raise Exception(f'unknown type in "where": {root}')


def to_moz_sql_struct(mp):
    res = {
        'select': [],
        'from': []
    }
    res['select'] = [{
        'value': x.value,
        'name': get_alias(x)
    } for x in mp.targets]

    if isinstance(mp.from_table, Identifier):
        res['from'] = [identifier_to_dict(mp.from_table)]
    elif isinstance(mp.from_table, Join):
        if mp.from_table.join_type == 'left join':
            if not isinstance(mp.from_table.right, Identifier):
                raise Exception("only one 'level' of join supports")
            res['from'] = [
                identifier_to_dict(mp.from_table.left),
                {
                    'left join': identifier_to_dict(mp.from_table.right)
                }
            ]
            if mp.from_table.condition is not None:
                if mp.from_table.condition.op == '=':
                    res['from'][1]['on'] = {'eq': [identifier_to_dict(x) for x in mp.from_table.condition.args]}
        else:
            raise Exception('Only left join support')
    else:
        raise Exception(f'unexpected type {mp.from_table}')

    where = mp.where
    if where is not None:
        where = where_to_dict(where)
        res['where'] = where

    if mp.limit is not None:
        res['limit'] = mp.limit.value

    return res

