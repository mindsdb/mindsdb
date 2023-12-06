from typing import Union, List
import copy
from collections import defaultdict

from mindsdb_sql.parser.ast import (
    Identifier, Select, BinaryOperation, Last, Constant, Star, ASTNode, NullConstant, OrderBy
)
from mindsdb_sql.planner.utils import query_traversal


class LastQuery:
    """
    Wrapper for AST query.
    Intended to ind, track, update last values in query
    """

    def __init__(self, query: ASTNode):
        self.query_orig = None
        self.query = None

        # check query type
        if not isinstance(query, Select):
            # just skip it
            return

        self.last_idx = defaultdict(list)
        last_tables = self.__find_last_columns(query)
        if last_tables is None:
            return

        self.query = query

        self.last_tables = last_tables

    def __find_last_columns(self, query: ASTNode) -> Union[dict, None]:
        """
          This function:
           - Searches LAST column in the input query
           - Replaces it with constants and memorises link to these constants
           - Link to constants will be used to inject values to query instead of LAST
           - Provide checks:
             - if it is possible to find the table for column
             - if column in select target
           - Generates and returns last_column variable which is dict
                last_columns[table_name] = {
                    'table': <table identifier>,
                    'column': <column name>,
                    'links': [<link to ast node>, ... ],
                    'target_idx': <number of column in select target>
                }
        """

        # index last variables in query
        tables_idx = defaultdict(dict)
        conditions = []

        def _index_query(node, is_table, parent_query, **kwargs):

            parent_query_id = id(parent_query)

            if is_table and isinstance(node, Identifier):
                # memorize table
                tables_idx[parent_query_id][node.parts[-1]] = node
                if node.alias is not None:
                    tables_idx[parent_query_id][node.alias.parts[-1]] = node

            # find last in where
            if isinstance(node, BinaryOperation):
                if isinstance(node.args[0], Identifier) and isinstance(node.args[1], Last):
                    # memorize node
                    conditions.append([parent_query_id, node])

        # find lasts
        query_traversal(query, _index_query)

        if len(conditions) == 0:
            return

        self.query_orig = copy.deepcopy(query)

        for parent_query_id, node in conditions:
            # inject constant
            link = Constant(None)
            node.args[1] = link
            self.last_idx[parent_query_id].append(node)

        # index query targets
        query_id = id(query)
        tables = tables_idx[query_id]
        is_star_in_target = False
        target_idx = {}
        for i, target in enumerate(query.targets):
            if isinstance(target, Star):
                is_star_in_target = True
                continue
            elif not isinstance(target, Identifier):
                continue

            col_name = target.parts[-1]
            if len(target.parts) > 1:
                table_name = target.parts[-2]
                table = tables.get(table_name)
            elif len(tables) == 1:
                table = list(tables.values())[0]
            else:
                continue

            target_idx[(table.parts[-1], col_name)] = i

        # make info about query

        last_columns = {}
        for parent_query_id, nodes in self.last_idx.items():
            for node in nodes:
                col, last = node.args
                tables = tables_idx[parent_query_id]

                uniq_tables = len(set([id(v) for v in tables.values()]))
                if len(col.parts) > 1:

                    table = tables.get(col.parts[-2])
                    if table is None:
                        raise Exception('cant find table')
                elif uniq_tables == 1:
                    table = list(tables.values())[0]
                else:
                    # or just skip it?
                    raise Exception('cant find table')

                col_name = col.parts[-1]

                table_name = table.parts[-1]
                if table_name not in last_columns:
                    # check column in target
                    target_idx = target_idx.get((table_name, col_name))
                    if target_idx is None:
                        if is_star_in_target:
                            # will try to get by name
                            ...
                        else:
                            raise Exception('Last value should be in query target')

                    last_columns[table_name] = {
                        'table': table,
                        'column': col_name,
                        'links': [last],
                        'target_idx': target_idx
                    }

                elif last_columns[table_name]['column'] == col_name:
                    last_columns[table_name]['column'].append(last)
                else:
                    raise Exception('possible to use only one column')

        return last_columns

    def to_string(self) -> str:
        """
            String representation of the query
            Used to identify query in query_context table
        """
        return self.query_orig.to_string()

    def get_last_columns(self) -> List[dict]:
        """
        Return information about LAST columns in query
        :return:
        """
        return [
            {
                'table': info['table'],
                'table_name': table_name,
                'column_name': info['column'],
                'target_idx': info['target_idx'],
            }
            for table_name, info in self.last_tables.items()
        ]

    def apply_values(self, values: dict) -> ASTNode:
        """
        Fills query with new values and return it
        """
        for table_name, info in self.last_tables.items():
            value = values.get(table_name, {}).get(info['column'])
            for last in info['links']:
                last.value = value

        return self.query

    def get_init_queries(self):

        back_up_values = []
        # replace values
        for nodes in self.last_idx.values():
            for node in nodes:
                back_up_values.append([node.op, node.args[1]])
                node.op = 'is not'
                node.args[1] = NullConstant()

        query2 = copy.deepcopy(self.query)

        # return values
        for nodes in self.last_idx.values():
            for node in nodes:
                op, arg1 = back_up_values.pop(0)
                node.op = op
                node.args[1] = arg1

        for info in self.get_last_columns():
            col = Identifier(info['column_name'])
            query2.targets = [col]
            query2.order_by = [
                OrderBy(col, direction='DESC')
            ]
            query2.limit = Constant(1)
            yield query2, info
