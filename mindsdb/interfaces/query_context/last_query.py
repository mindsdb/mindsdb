from typing import Union, List
import copy
from collections import defaultdict

from mindsdb_sql_parser.ast import (
    Identifier, Select, BinaryOperation, Last, Constant, Star, ASTNode, NullConstant, OrderBy, Function, TypeCast
)
from mindsdb.integrations.utilities.query_traversal import query_traversal


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
        last_tables = self._find_last_columns(query)
        if last_tables is None:
            return

        self.query = query

        self.last_tables = last_tables

    def _find_last_columns(self, query: ASTNode) -> Union[dict, None]:
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
                    'target_idx': <number of column in select target>,
                    'gen_init_query': if true: to generate query to initial values for LAST
                }
        """

        # index last variables in query
        tables_idx = defaultdict(dict)
        conditions = []

        def replace_last_in_tree(node: ASTNode, injected: Constant):
            """
            Recursively searches LAST in AST tree. Goes only into functions and type casts
            When LAST is found - it is replaced with injected constant
            """
            # go into functions and type casts
            if isinstance(node, TypeCast):
                if isinstance(node.arg, Last):
                    node.arg = injected
                    return injected
                return replace_last_in_tree(node.arg, injected)
            if isinstance(node, Function):
                for i, arg in enumerate(node.args):
                    if isinstance(arg, Last):
                        node.args[i] = injected
                        return injected
                    found = replace_last_in_tree(arg, injected)
                    if found:
                        return found

        def index_query(node, is_table, parent_query, **kwargs):

            parent_query_id = id(parent_query)
            last = None
            if is_table and isinstance(node, Identifier):
                # memorize table
                tables_idx[parent_query_id][node.parts[-1]] = node
                if node.alias is not None:
                    tables_idx[parent_query_id][node.alias.parts[-1]] = node

            # find last in where
            if isinstance(node, BinaryOperation):
                if isinstance(node.args[0], Identifier):
                    col = node.args[0]
                    gen_init_query = True

                    # col > last
                    if isinstance(node.args[1], Last):
                        last = Constant(None)
                        # inject constant
                        node.args[1] = last

                    # col > coalesce(last, 0) OR col > cast(coalense(last ...))
                    else:
                        injected = Constant(None)
                        last = replace_last_in_tree(node.args[1], injected)
                        gen_init_query = False

            if last is not None:
                # memorize
                conditions.append({
                    'query_id': parent_query_id,
                    'condition': node,
                    'last': last,
                    'column': col,
                    'gen_init_query': gen_init_query  # generate query to fetch initial last values from table
                })

        # find lasts
        query_traversal(query, index_query)

        if len(conditions) == 0:
            return

        self.query_orig = copy.deepcopy(query)

        for info in conditions:
            self.last_idx[info['query_id']].append(info)

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
        for parent_query_id, items in self.last_idx.items():
            for info in items:
                col = info['column']
                last = info['last']
                tables = tables_idx[parent_query_id]

                uniq_tables = len(set([id(v) for v in tables.values()]))
                if len(col.parts) > 1:

                    table = tables.get(col.parts[-2])
                    if table is None:
                        raise ValueError('cant find table')
                elif uniq_tables == 1:
                    table = list(tables.values())[0]
                else:
                    # or just skip it?
                    raise ValueError('cant find table')

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
                            raise ValueError('Last value should be in query target')

                    last_columns[table_name] = {
                        'table': table,
                        'column': col_name,
                        'links': [last],
                        'target_idx': target_idx,
                        'gen_init_query': info['gen_init_query']
                    }

                elif last_columns[table_name]['column'] == col_name:
                    last_columns[table_name]['column'].append(last)
                else:
                    raise ValueError('possible to use only one column')

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
                'gen_init_query': info['gen_init_query'],
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
        """
        A generator of queries to get initial value of the last
        """

        back_up_values = []
        # replace values
        for items in self.last_idx.values():
            for info in items:
                node = info['condition']
                back_up_values.append([node.op, node.args[1]])
                node.op = 'is not'
                node.args[1] = NullConstant()

        query2 = copy.deepcopy(self.query)

        # return values
        for items in self.last_idx.values():
            for info in items:
                node = info['condition']
                op, arg1 = back_up_values.pop(0)
                node.op = op
                node.args[1] = arg1

        for info in self.get_last_columns():
            if not info['gen_init_query']:
                continue
            col = Identifier(info['column_name'])
            query2.targets = [col]
            query2.order_by = [
                OrderBy(col, direction='DESC')
            ]
            query2.limit = Constant(1)
            yield query2, info
