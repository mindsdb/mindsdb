from datetime import datetime
from typing import List, Tuple, Optional

from google.cloud import firestore
from google.cloud.firestore_v1 import FieldFilter, Or
from mindsdb_sql import (
    Star,
    ASTNode,
    Select,
    Update,
    TypeCast,
    Constant,
    Identifier,
    BinaryOperation,
)


class FirestoreRender:
    """
    This class renders the SQL statement to Firestore query.
    """
    def __init__(self, connection: Optional[firestore.Client] = None):
        self.connection = connection

    def to_firestore_query(self, node: ASTNode):
        """
        Parse the SQL statement and return the Firestore query.
        """
        if isinstance(node, Select):
            return self.select(node)
        elif isinstance(node, Update):
            return self.update(node)
        raise NotImplementedError(f'Unknown statement: {node.__class__.__name__}')

    def select(self, node: Select):
        """
        Parse the select statement and return the Firestore query.
        """
        if not isinstance(node.from_table, Identifier):
            raise NotImplementedError(f'Not supported from {node.from_table}')

        collection_name = node.from_table.parts[-1]
        query = self.connection.collection(collection_name)

        if node.where is not None:
            filters = self.handle_where(node.where)
            if isinstance(filters, Or):
                query = query.where(
                    filter=filters
                )
            if isinstance(filters, list):
                for filter in filters:
                    query = query.where(
                        filter=filter)
            else:
                query = query.where(
                    filter=filters
                )

        if node.distinct:
            raise NotImplementedError(f'Distinct {node.distinct}')

        selected_columns: Optional[List[str]] = None
        if node.targets is not None:
            for col in node.targets:
                if isinstance(col, Star):
                    break
                if isinstance(col, Identifier):
                    name = col.parts[-1]
                    if selected_columns is None:
                        selected_columns = [name]
                    else:
                        selected_columns.append(name)
                else:
                    raise NotImplementedError(f'Not supported {col}')

        if node.group_by is not None:
            raise NotImplementedError(f'Group {node.group_by}')

        if node.order_by is not None:
            for col in node.order_by:
                name = col.field.parts[-1]
                direction = (
                    firestore.Query.ASCENDING if col.direction.upper() == 'ASC' else firestore.Query.DESCENDING
                )
                query.order_by(
                    name,
                    direction=direction,
                )

        if node.limit is not None:
            query = query.limit(int(node.limit.value))

        return {
            "collection": collection_name,
            "query": query,
            "requested_columns": selected_columns,
        }

    def update(self, node: Update):
        """
        Parse the update statement and return the Firestore query.
        """
        if not isinstance(node.table, Identifier):
            raise NotImplementedError(f'Not supported for {node.table}')

        collection_name = node.table.parts[-1]
        query = self.connection.collection(collection_name)

        if node.where is not None:
            filters = self.handle_where(node.where)
            if isinstance(filters, Or):
                query = query.where(
                    filter=filters
                )
            if isinstance(filters, list):
                for filter in filters:
                    query = query.where(
                        filter=filter)
            else:
                query = query.where(
                    filter=filters
                )

        update_values = self.parse_update_values(node.update_columns)

        return {
            "collection": collection_name,
            "query": query,
            "update_values": update_values
        }

    def handle_where(self, node):
        """
        Parse the where statement and return the Firestore query.
        """
        if not type(node) in [BinaryOperation]:
            raise NotImplementedError(f'Not supported type {type(node)}')

        op = node.op.lower()
        arg1, arg2 = node.args

        supported_ops = ['>', '<', '>=', '<=', '!=', 'in', 'not-in', 'array-contains', 'array-contains-any']

        if op in ('and', 'or'):
            filter1 = self.handle_where(arg1)
            filter2 = self.handle_where(arg2)

            if op == 'and':
                return [filter1, filter2]
            else:
                return Or(filters=[filter1, filter2])

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

            elif isinstance(node, TypeCast) \
                    and node.type_name.upper() in ('DATE', 'DATETIME'):
                formats = [
                    "%Y-%m-%d",
                    "%Y-%m-%dT%H:%M:%S.%f"
                ]
                for format in formats:
                    try:
                        return datetime.strptime(node.arg.value, format)
                    except ValueError:
                        pass
                raise RuntimeError(f'Not supported date format. Supported: {formats}')

            elif isinstance(arg2, Tuple):
                if op in ('in', 'not-in'):
                    values = [v.value for v in arg2.items]
                    return FieldFilter(name, op, values)
                else:
                    raise NotImplementedError(f'Not supported operator {op}')

    def parse_update_values(self, update_columns):
        """
        Parse the update statement and return the values.
        """
        update_values = {}

        for column, value in update_columns.items():
            if isinstance(column, str) and isinstance(value, Constant):
                update_values[column] = value.value
            else:
                raise NotImplementedError(f'Not supported set operation: {column = }, {value =}')

        return update_values
