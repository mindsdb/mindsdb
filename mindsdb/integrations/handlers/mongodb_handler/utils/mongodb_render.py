from mindsdb_sql.parser.ast import *


class MongodbRender:

    def render(self, node):
        if isinstance(node, Select):
            return self.select(node)
        raise NotImplementedError(f'Unknown statement: {node.__name__}')

    def select(self, node):
        # collection
        if not isinstance(node.from_table, Identifier):
            raise Exception(f'Not supported from {node.from_table}')

        collection = node.from_table.to_string()

        # filter
        filters = {}

        if node.where is not None:
            filters = self.handle_where(node.where)

        group = {}
        project = {}
        if node.distinct:
            # is group by distinct fields
            group = {'_id': {}}
            # project = {'_id': 0} #  hide _id

        if node.targets is not None:
            for col in node.targets:
                if isinstance(col, Star):
                    # show all
                    project = {}
                    break
                if isinstance(col, Identifier):
                    name = col.parts[-1]
                    if col.alias is None:
                        alias = name
                    else:
                        alias = col.alias.parts[-1]

                    project[alias] = f'${name}'  # project field

                    # group by distinct fields
                    if node.distinct:
                        group['_id'][name] = f'${name}'  # group field
                        group[name] = {'$first': f'${name}'}  # show field

                elif isinstance(col, Constant):
                    val = str(col.value)  # str because int is interpreted as index
                    if col.alias is None:
                        alias = val
                    else:
                        alias = col.alias.parts[-1]
                    project[alias] = val

        if node.group_by is not None:
            # TODO
            ...

        # is dict of dicts for pymongo
        sort = []
        if node.order_by is not None:
            for col in node.order_by:
                name = col.field.parts[-1]
                direction = 1 if col.direction.upper() == 'ASC' else -1
                sort.append([name, direction])

        # compose mongo query

        call = []

        method = 'aggregate'
        arg = []
        if filters:
            arg.append({"$match": filters})

        if group:
            arg.append({"$group": group})

        if project:
            arg.append({"$project": project})

        if sort:
            arg.append({"$sort": sort})

        if node.offset is not None:
            arg.append({"$skip": int(node.offset.value)})

        if node.limit is not None:
            arg.append({"$limit": int(node.limit.value)})

        call.append({
            'method': method,
            'args': [arg]
        })

        return {
            'collection': collection,
            'call': call
        }

    def handle_where(self, node):
        # todo UnaryOperation, function
        if not type(node) in [BinaryOperation]:
            raise Exception(f'Not supported type {type(node)}')

        op = node.op.lower()
        arg1, arg2 = node.args

        if op in ('and', 'or'):
            query1 = self.handle_where(arg1)
            query2 = self.handle_where(arg2)

            ops = {
                'and': '$and',
                'or': '$or',
            }
            query = {ops[op]: [query1, query2]}
            return query

        if isinstance(arg1, Constant):
            # swap
            arg1, arg2 = arg2, arg1

            # change operator
            ops = {
                '>=': '<=',
                '>': '<',
                '<': '>',
                '<=': '>=',
            }
            op = ops.get(op, op)

            if isinstance(arg1, Constant):
                #  both constants
                if op == '=':
                    op = '=='
                # they are swapped
                return {"$where": f"{repr(arg2.value)} {op} {repr(arg1.value)}"}

        if isinstance(arg1, Identifier):
            var_name = arg1.parts[-1]

            if isinstance(arg2, Identifier):
                # both identifiers: {"$where" : "this.city > this.city"}
                var1 = f'this.{var_name}'
                var2 = f'this.{arg2.parts[-1]}'
                return {"$where": f"{var1} {op} {var2}"}

            if isinstance(arg2, Latest):
                var1 = f'this.{var_name}'
                var2 = 'latest'
                return {"$where": f"{var1} {op} {var2}"}

            elif isinstance(arg2, Tuple):
                # there is no swap: args doesn't content Constant

                # it should be IN, NOT IN
                ops = {
                    'in': '$in',
                    'not in': '$nin'
                }
                values = [
                    i.value
                    for i in arg2.items
                ]

                if op in ops:
                    op2 = ops.get(node.op)
                    cond = {op2: values}
                else:
                    raise Exception(f'Not supported operator {op}')

                return {var_name: cond}

            elif isinstance(arg2, Constant):
                # identifier and constant
                ops = {
                    '>=': '$ge',
                    '>': '$gt',
                    '<': '$lt',
                    '<=': '$le',
                    '<>': '$ne',
                    '!=': '$ne',
                }
                val = arg2.value
                if op in ('=', '=='):
                    pass
                elif op in ops:
                    op2 = ops.get(node.op)
                    val = {op2: val}
                else:
                    raise Exception(f'Not supported operator {op}')

                return {var_name: val}

        raise Exception(f'Not supported expression {node.to_string()}')

