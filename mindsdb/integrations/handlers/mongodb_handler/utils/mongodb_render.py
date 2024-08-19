import datetime as dt

from bson.objectid import ObjectId
from mindsdb_sql.parser.ast import *

from mindsdb.api.mongo.utilities.mongodb_query import MongoQuery


class MongodbRender:

    def to_mongo_query(self, node):
        if isinstance(node, Select):
            return self.select(node)
        elif isinstance(node, Update):
            return self.update(node)
        raise NotImplementedError(f'Unknown statement: {node.__class__.__name__}')

    def update(self, node: Update):
        collection = node.table.parts[-1]
        mquery = MongoQuery(collection)

        filters = self.handle_where(node.where)
        row = {
            k: v.value
            for k, v in node.update_columns.items()
        }
        mquery.add_step({
            'method': 'update_many',
            'args': [
                filters,
                {"$set": row}
            ]
        })
        return mquery

    def select(self, node: Select):
        # collection
        if not isinstance(node.from_table, Identifier):
            raise NotImplementedError(f'Not supported from {node.from_table}')

        collection = node.from_table.parts[-1]

        # filter
        filters = {}

        if node.where is not None:
            filters = self.handle_where(node.where)

        group = {}
        project = {'_id': 0}   # hide _id
        if node.distinct:
            # is group by distinct fields
            group = {'_id': {}}

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
            raise NotImplementedError(f'Group {node.group_by}')

        sort = {}
        if node.order_by is not None:
            for col in node.order_by:
                name = col.field.parts[-1]
                direction = 1 if col.direction.upper() == 'ASC' else -1
                sort[name] = direction

        # compose mongo query

        mquery = MongoQuery(collection)

        method = 'aggregate'
        arg = []

        # mongodb related pipeline steps for aggregate method
        if node.modifiers is not None:
            for modifier in node.modifiers:
                arg.append(modifier)

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

        mquery.add_step({
            'method': method,
            'args': [arg]
        })

        return mquery

    def handle_where(self, node):
        # todo UnaryOperation, function
        if not type(node) in [BinaryOperation]:
            raise NotImplementedError(f'Not supported type {type(node)}')

        # logic operation
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

        ops_map = {
            '>=': '$gte',
            '>': '$gt',
            '<': '$lt',
            '<=': '$lte',
            '<>': '$ne',
            '!=': '$ne',
            '=': '$eq',
            '==': '$eq',
            'is': '$eq',
            'is not': '$ne',
        }

        if isinstance(arg1, Identifier):
            var_name = arg1.parts[-1]
            # is simple operation
            if isinstance(arg2, Constant):
                # identifier and constant

                val = ObjectId(arg2.value) if var_name == '_id' else arg2.value
                if op in ('=', '=='):
                    pass
                elif op in ops_map:
                    op2 = ops_map[op]
                    val = {op2: val}
                else:
                    raise NotImplementedError(f'Not supported operator {op}')

                return {var_name: val}

            # is IN condition
            elif isinstance(arg2, Tuple):
                # it should be IN, NOT IN
                ops = {
                    'in': '$in',
                    'not in': '$nin'
                }
                # must be list of Constants
                values = [
                    i.value
                    for i in arg2.items
                ]

                if op in ops:
                    op2 = ops[op]
                    cond = {op2: values}
                else:
                    raise NotImplementedError(f'Not supported operator {op}')

                return {var_name: cond}

        # try to make expression

        val1 = self.where_element_convert(arg1)
        val2 = self.where_element_convert(arg2)

        if op in ops_map:
            op2 = ops_map[op]
        else:
            raise NotImplementedError(f'Not supported operator {op}')

        return {
            '$expr': {
                op2: [val1, val2]
            }
        }

    def where_element_convert(self, node):
        if isinstance(node, Identifier):
            return f'${node.parts[-1]}'
        elif isinstance(node, Latest):
            return f'LATEST'
        elif isinstance(node, Constant):
            return node.value
        elif isinstance(node, TypeCast)\
                and node.type_name.upper() in ('DATE', 'DATETIME'):
            formats = [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S.%f"
            ]
            for format in formats:
                try:
                    return dt.datetime.strptime(node.arg.value, format)
                except ValueError:
                    pass
            raise RuntimeError(f'Not supported date format. Supported: {formats}')
        else:
            raise NotImplementedError(f'Unknown where element {node}')

