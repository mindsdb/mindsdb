from typing import List, Tuple, Optional

from google.cloud import firestore
from google.cloud.firestore_v1 import FieldFilter
from mindsdb_sql import (
    Star,
    ASTNode,
    Select,
    Update,
    Constant,
    Identifier,
    BinaryOperation,
)


class FirestoreRender:

    def to_firestore_query(self, node: ASTNode):
        if isinstance(node, Select):
            return self.select(node)
        elif isinstance(node, Update):
            return self.update(node)
        raise NotImplementedError(f'Unknown statement: {node.__class__.__name__}')

    def select(self, node: Select):
        if not isinstance(node.from_table, Identifier):
            raise NotImplementedError(f'Not supported from {node.from_table}')

        collection = node.from_table.parts[-1]

        filters: List[FieldFilter] = []

        if node.where is not None:
            filter_fields = self.handle_where(node.where)
            if isinstance(filter_fields, List):
                filters = filters + filter_fields
            else:
                filters.append(filter_fields)

        if node.distinct:
            raise NotImplementedError(f'Distinct {node.distinct}')

        selected_columns: Optional[List[str]] = None
        if node.targets is not None:
            selected_columns = None
            for col in node.targets:
                if isinstance(col, Star):
                    break
                if isinstance(col, Identifier):
                    name = col.parts[-1]
                    selected_columns.append(name)
                else:
                    raise NotImplementedError(f'Not supported {col}')

        if node.group_by is not None:
            raise NotImplementedError(f'Group {node.group_by}')

        order_by: Optional[dict] = None
        if node.order_by is not None:
            order_by = {}
            for col in node.order_by:
                name = col.field.parts[-1]
                direction = (
                    firestore.Query.ASCENDING if col.direction.upper() == 'ASC' else firestore.Query.DESCENDING
                )
                order_by[name] = direction

        limit: Optional[int] = None
        if node.limit is not None:
            limit = int(node.limit.value)

        firestore_query = {
            'collection': collection,
            'filters': filters,
            'order_by': order_by,
            'limit': limit,
            'selected_columns': selected_columns,
        }

        return firestore_query

    def update(self, node: Update):
        # TODO: implement update
        pass

    def handle_where(self, node):
        if not type(node) in [BinaryOperation]:
            raise NotImplementedError(f'Not supported type {type(node)}')

        op = node.op.lower()
        arg1, arg2 = node.args

        supported_ops = ['>', '<', '>=', '<=', '!=', 'in', 'not-in', 'array-contains', 'array-contains-any']

        if op in ('and', 'or'):
            filter1 = self.handle_where(arg1)
            filter2 = self.handle_where(arg2)

            return [filter1, filter2]

        if isinstance(arg1, Identifier):
            name = arg1.parts[-1]

            if isinstance(arg2, Constant):

                value = arg2.value
                if op in ('=', '=='):
                    return FieldFilter(name, '==', value)
                elif op in supported_ops:
                    return FieldFilter(name, op, value)
                else:
                    raise NotImplementedError(f'Not supported operator {op}')

            elif isinstance(arg2, Tuple):
                if op in ('in', 'not-in'):
                    values = [v.value for v in arg2.items]
                    return FieldFilter(name, op, values)
                else:
                    raise NotImplementedError(f'Not supported operator {op}')
