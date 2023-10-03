import copy
from collections import defaultdict

from mindsdb_sql.parser.ast import Identifier, Select, BinaryOperation, Last, Constant, Star
from mindsdb_sql.planner.utils import query_traversal


class LastQuery:
    """
    Tracks last values in query
    """

    def __init__(self, query):
        self.query = None

        # check query type
        if not isinstance(query, Select):
            # just skip it
            return

        self.query_in = query

        query = copy.deepcopy(self.query_in)

        last_tables = self.__find_last_columns(query)
        if last_tables is None:
            return

        self.query = query
        self.last_tables = last_tables

    def __find_last_columns(self, query):

        # index last variables in query
        tables_idx = defaultdict(dict)
        last_idx = defaultdict(list)

        def _index_query(node, is_table, parent_query, **kwargs):

            parent_query_id = id(parent_query)

            if is_table and isinstance(node, Identifier):
                # memorize table
                tables_idx[parent_query_id][node.parts[-1]] = node
                if node.alias is not None:
                    tables_idx[parent_query_id][node.alias.parts[-1]] = node

            # find last in where
            if isinstance(node, BinaryOperation):
                arg0 = node.args[0]
                arg1 = node.args[1]
                if isinstance(arg0, Identifier) and isinstance(arg1, Last):

                    # TODO handle binary operation <, >

                    # inject constant
                    link = Constant(None)
                    node.args[1] = link
                    last_idx[parent_query_id].append([arg0, link])

                    return node

        # find lasts
        query_traversal(query, _index_query)

        if len(last_idx) == 0:
            return

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
        for parent_query_id, lasts in last_idx.items():
            for col, last in lasts:
                tables = tables_idx[parent_query_id]
                if len(col.parts) > 1:

                    table = tables.get(col.parts[-2])
                    if table is None:
                        raise Exception('cant find table')
                elif len(tables) == 1:
                    table = tables[0]
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

    def to_string(self):
        return self.query_in.to_string()

    def get_last_columns(self):
        return [
            {
                'table': info['table'],
                'table_name': table_name,
                'column_name': info['column'],
                'target_idx': info['target_idx'],
            }
            for table_name, info in self.last_tables.items()
        ]

    def apply_values(self, values):
        # fill the query:
        for table_name, info in self.last_tables.items():
            value = values[table_name][info['column']]
            for last in info['links']:
                # TODO how to inject
                last.value = value

        return self.query
