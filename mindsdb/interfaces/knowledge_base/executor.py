from dataclasses import dataclass
import copy

from mindsdb_sql_parser.ast import BinaryOperation, Identifier, Constant, UnaryOperation, Select, Star, Tuple
import pandas as pd

from mindsdb.integrations.utilities.query_traversal import query_traversal


@dataclass
class ConditionBlock:
    op: str
    items: list


class KnowledgeBaseQueryExecutor:

    def __init__(self, kb, content_column='content', id_column='chunk_id'):
        self.kb = kb
        self.content_column = content_column.lower()
        self.id_column = id_column
        self.limit = None

    def is_content_condition(self, node):
        if isinstance(node, BinaryOperation):
            if isinstance(node.args[0], Identifier):
                parts = node.args[0].parts
                if len(parts) == 1 and parts[0].lower() == self.content_column:
                    return True
        return False

    def invert_content_op(self, node):
        op_map = {
            '=': '!=',
            '!=': '=',
            'LIKE': '!=',
            'NOT LIKE': '=',
            'IN': 'NOT IN'
        }
        if not node.op.upper() in op_map:
            raise NotImplementedError(str(node))
        node.op = op_map[node.op.upper()]
        return node

    def convert_unary_ops(self, node, callstack, **kwargs):
        if isinstance(node, UnaryOperation):
            if node.op.upper() == 'NOT':
                # two options:
                # 1. NOT content <op> value
                if self.is_content_condition(node.args[0]):
                    self.invert_content_op(node.args[0])
                    return node.args[0]

                # 2. content <op> NOT value
                if self.is_content_condition(callstack[0]):
                    item = callstack[0]
                    self.invert_content_op(item)
                    return node.args[0]

    def union(self, results):
        if len(results) == 1:
            return results[0]

        res = pd.concat(results)
        df = res.drop_duplicates(subset=[self.id_column]).reset_index()
        return df

    def intersect(self, results):
        if len(results) == 1:
            return results[0]

        item = results[0]
        for item2 in results[1:]:
            item = item[item[self.id_column].isin(item2[self.id_column])]

        df = item
        return df

    def flatten(self, node):
        """
        collect 'AND' and 'OR' conditions of the same level to a single ConditionBlock
        """
        if isinstance(node, BinaryOperation):
            op = node.op.upper()
            if op in ('AND', 'OR'):
                block = ConditionBlock(op, [])
                for arg in node.args:
                    item = self.flatten(arg)
                    if isinstance(item, ConditionBlock):
                        if item.op == block.op:
                            block.items.extend(item.items)
                        else:
                            # new type of block
                            block.items.append(item)
                    else:
                        block.items.append(item)
                return block
            else:
                node.op = node.op.upper()
                return node

        raise NotImplementedError

    def make_query(self, conditions):
        # create AST query to KB
        where = None
        for condition in conditions:
            if where is None:
                where = condition
            else:
                where = BinaryOperation('AND', args=[where, condition])

        return Select(
            targets=[Star()],
            where=where
        )

    def call_kb(self, conditions, disable_reranking=False, limit=None):
        # call KB with list of prepared conditions

        query = self.make_query(conditions)
        if limit is not None:
            query.limit = Constant(limit)
        elif self.limit is not None:
            query.limit = Constant(self.limit)

        return self.kb.select(query, disable_reranking=disable_reranking)

    def execute_conditions(self, conditions, content_condition=None, disable_reranking=False, limit=None):
        # call KB with set of conditions. it might have a single condition for content
        # it can be several calls to KB (depending on type of condition)

        if content_condition is not None:
            if content_condition.op == 'IN':
                # (select where content = ‘a’) UNION (select where content = ‘b’)
                results = []
                for el in content_condition.args[1].items:
                    el_cond = BinaryOperation(op='=', args=[Identifier(self.content_column), Constant(el)])
                    results.append(self.call_kb([el_cond] + conditions, disable_reranking=disable_reranking))
                return self.union(results)
            elif content_condition.op in ('!=', '<>', 'NOT LIKE'):
                # id NOT IN (SELECT DISTINCT id FROM kb WHERE content =’...’ limit X)
                el_cond = BinaryOperation(op='=', args=content_condition.args)
                res = self.call_kb([el_cond] + conditions, disable_reranking=True, limit=100)

                return list(res[self.id_column])
                values = [Constant(i) for i in res[self.id_column]]
                cond2 = BinaryOperation(op='NOT IN', args=[Identifier(self.id_column), Tuple(values)])
                return self.call_kb([cond2]+conditions)

            elif content_condition.op in ('=', 'LIKE'):
                # just '='
                content_condition2 = copy.deepcopy(content_condition)
                content_condition2.op = '='
                return self.call_kb([content_condition2] + conditions)

            elif content_condition.op == 'NOT IN':
                # id NOT IN (
                #   (select id where content = ‘a’) UNION (select id where content = ‘b’)
                # )
                content_condition2 = copy.deepcopy(content_condition)
                content_condition2.op = 'IN'
                res = self.execute_conditions(conditions, content_condition2, disable_reranking=True, limit=100)

                values = [Constant(i) for i in res[self.id_column]]
                cond2 = BinaryOperation(op='NOT IN', args=[Identifier(self.id_column), Tuple(values)])
                return self.call_kb([cond2] + conditions)
            else:
                raise NotImplementedError

        return self.call_kb(conditions)

    def execute_blocks(self, block):
        """
        Split block to set of calls with conditions and execute them
        """

        if not isinstance(block, ConditionBlock):
              # single condition
              if self.is_content_condition(block):
                  # col_name = block.args[0].parts[-1].lower()
                  # if col_name == self.content_column:
                  return self.execute_conditions([], content_condition=block)
              else:
                  return self.execute_conditions([block])

        if block.op == 'AND':
            results = []

            content_filters, other_filters = [], []
            for item in block.items:
                if isinstance(item, ConditionBlock):
                    results.append(self.execute_blocks(item))
                else:
                    if self.is_content_condition(item):
                        # col_name = item.args[0].parts[-1].lower()
                        # if col_name == self.content_column:
                        content_filters.append(item)
                    else:
                        other_filters.append(item)
            if len(content_filters) > 0:
                for condition in content_filters:
                    result = self.execute_conditions(other_filters, content_condition=condition)
                    results.append(result)
            elif len(other_filters) > 0:
                results.append(self.execute_conditions(other_filters))

            return self.intersect(results)

        elif block.op == 'OR':
            results = []
            for item in block.items:
                results.append(self.execute_blocks(item))

            return self.union(results)

    def run(self, query):
        if query.where is not None:
            query_traversal(query.where, self.convert_unary_ops)
            blocks_tree = self.flatten(query.where)
            if query.limit is not None:
                self.limit = query.limit.value
            return self.execute_blocks(blocks_tree)
        else:
            return self.kb.select(query)