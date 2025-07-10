from dataclasses import dataclass
import copy
from typing import List, Optional, Union

from mindsdb_sql_parser.ast import (
    BinaryOperation,
    Identifier,
    Constant,
    UnaryOperation,
    Select,
    Star,
    Tuple,
    ASTNode,
    BetweenOperation,
    NullConstant,
)
import pandas as pd

from mindsdb.integrations.utilities.query_traversal import query_traversal


@dataclass
class ConditionBlock:
    op: str
    items: list


class KnowledgeBaseQueryExecutor:
    def __init__(self, kb, content_column="content", id_column="chunk_id"):
        self.kb = kb
        self.content_column = content_column.lower()
        self.id_column = id_column
        self.limit = None
        self._negative_set_size = 100
        self._negative_set_threshold = 0.5

    def is_content_condition(self, node: ASTNode) -> bool:
        """
        Checks if the node is a condition to Content column

        :param node: condition to check
        """
        if isinstance(node, BinaryOperation):
            if isinstance(node.args[0], Identifier):
                parts = node.args[0].parts
                if len(parts) == 1 and parts[0].lower() == self.content_column:
                    return True
        return False

    @staticmethod
    def invert_content_op(node: BinaryOperation) -> BinaryOperation:
        # Change operator of binary operation to opposite one
        op_map = {"=": "!=", "!=": "=", "LIKE": "!=", "NOT LIKE": "=", "IN": "NOT IN", "NOT IN": "IN"}
        if node.op.upper() not in op_map:
            raise NotImplementedError(f"Can't handle condition: '{str(node)}'")
        node.op = op_map[node.op.upper()]
        return node

    def convert_unary_ops(self, node: ASTNode, callstack: List[ASTNode], **kwargs) -> ASTNode:
        """
        Tries to remove unary operator and apply it to Binary operation.
        Supported cases:
        - "NOT content <op> value" => "content <!op> value"
        - "content <op> NOT value" => "content <!op> value"

        Where <!op> is inverted operator of <op>
        """

        if isinstance(node, UnaryOperation):
            if node.op.upper() == "NOT":
                # two options:
                # 1. NOT content <op> value
                if self.is_content_condition(node.args[0]):
                    self.invert_content_op(node.args[0])
                    return node.args[0]

                # 2. content <op> NOT value
                if self.is_content_condition(callstack[0]):
                    self.invert_content_op(callstack[0])
                    return node.args[0]

    def union(self, results: List[pd.DataFrame]) -> pd.DataFrame:
        # combine dataframes from input list to single one

        if len(results) == 1:
            return results[0]

        res = pd.concat(results)
        df = res.drop_duplicates(subset=[self.id_column]).reset_index()
        return df

    def intersect(self, results: List[pd.DataFrame]) -> pd.DataFrame:
        # intersect dataframes from input list: return dataframe with rows that exist in all input dataframes

        if len(results) == 1:
            return results[0]

        item = results[0]
        for item2 in results[1:]:
            item = item[item[self.id_column].isin(item2[self.id_column])]

        df = item
        return df

    @classmethod
    def flatten_conditions(cls, node: ASTNode) -> Union[ASTNode, ConditionBlock]:
        """
        Recursively inspect conditions tree and move conditions related to 'OR' or 'AND' operators of the same level
          to same ConditionBlock
        Example: or (a=1, or (b=2, c=3))
          is converted to: ConditionBlock(or, [a=1, b=2, c=3])
        """

        if isinstance(node, BinaryOperation):
            op = node.op.upper()
            if op in ("AND", "OR"):
                block = ConditionBlock(op, [])
                for arg in node.args:
                    item = cls.flatten_conditions(arg)
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

        elif isinstance(node, BetweenOperation):
            block = ConditionBlock(
                "AND",
                [
                    BinaryOperation(">=", args=[node.args[0], node.args[1]]),
                    BinaryOperation("<=", args=[node.args[0], node.args[2]]),
                ],
            )
            return block

        raise NotImplementedError(f"Unknown node '{node}'")

    def call_kb(
        self, conditions: List[BinaryOperation], disable_reranking: bool = False, limit: int = None
    ) -> pd.DataFrame:
        """
        Call KB with list of prepared conditions

        :param conditions: input conditions
        :param disable_reranking: flag disable reranking
        :param limit: use custom limit
        :return: result of querying KB
        """

        where = None
        for condition in conditions:
            if where is None:
                where = condition
            else:
                where = BinaryOperation("AND", args=[where, condition])

        query = Select(targets=[Star()], where=where)

        if limit is not None:
            query.limit = Constant(limit)
        elif self.limit is not None:
            query.limit = Constant(self.limit)

        return self.kb.select(query, disable_reranking=disable_reranking)

    def execute_content_condition(
        self,
        content_condition: BinaryOperation,
        other_conditions: List[BinaryOperation] = None,
        disable_reranking: bool = False,
        limit: int = None,
    ) -> pd.DataFrame:
        """
        Call KB using content condition. Only positive conditions for content can be here.
        Negative conditions can be only as filter of ID
        :param content_condition: condition for Content column
        :param other_conditions: conditions for other columns
        :param disable_reranking: turn off reranking
        :param limit: override default limit
        :return: result of the query
        """

        if other_conditions is None:
            other_conditions = []

        if content_condition.op == "IN":
            # (select where content = ‘a’) UNION (select where content = ‘b’)
            results = []
            for el in content_condition.args[1].items:
                el_cond = BinaryOperation(op="=", args=[Identifier(self.content_column), el])
                results.append(
                    self.call_kb([el_cond] + other_conditions, disable_reranking=disable_reranking, limit=limit)
                )
            return self.union(results)

        elif content_condition.op in ("=", "LIKE"):
            # just '='
            content_condition2 = copy.deepcopy(content_condition)
            content_condition2.op = "="
            return self.call_kb([content_condition2] + other_conditions)

        elif content_condition.op == "IS" and isinstance(content_condition.args[1], NullConstant):
            # return empty dataset, call to get column names
            return self.call_kb([], limit=1)[:0]
        elif content_condition.op == "IS NOT" and isinstance(content_condition.args[1], NullConstant):
            # execute without conditions
            return self.call_kb([])
        else:
            raise NotImplementedError(
                f'Operator "{content_condition.op}" is not supported for condition: {content_condition}'
            )

    @staticmethod
    def to_include_content(content_condition: BinaryOperation) -> List[str]:
        """
        Handles positive conditions for content. Returns list of content values
        """
        if content_condition.op == "IN":
            return [item.value for item in content_condition.args[1].items]

        elif content_condition.op in ("=", "LIKE"):
            return [content_condition.args[1].value]

    def to_excluded_ids(
        self, content_condition: BinaryOperation, other_conditions: List[BinaryOperation]
    ) -> Optional[List[str]]:
        """
        Handles negative conditions for content. If it is negative condition: extract and return list of IDs
         that have to be excluded by parent query

        :param content_condition: condition for Content column
        :param other_conditions:  conditions for other columns
        :return: list of IDs to exclude or None
        """

        if content_condition.op in ("!=", "<>", "NOT LIKE"):
            # id NOT IN (
            #    SELECT id FROM kb WHERE content =’...’ limit X
            # )
            el_cond = BinaryOperation(op="=", args=content_condition.args)
            threshold = BinaryOperation(op=">=", args=[Identifier("relevance"), Constant(self._negative_set_threshold)])
            res = self.call_kb(
                [el_cond, threshold] + other_conditions, disable_reranking=True, limit=self._negative_set_size
            )

            return list(res[self.id_column])

        elif content_condition.op == "NOT IN":
            # id NOT IN (
            #   select id where content in (‘a’, ‘b’)
            # )
            content_condition2 = copy.deepcopy(content_condition)
            content_condition2.op = "IN"

            threshold = BinaryOperation(op=">=", args=[Identifier("relevance"), Constant(self._negative_set_threshold)])
            res = self.execute_content_condition(
                content_condition2,
                other_conditions + [threshold],
                disable_reranking=True,
                limit=self._negative_set_size,
            )

            return list(res[self.id_column])
        else:
            return None

    def execute_blocks(self, block: ConditionBlock) -> pd.DataFrame:
        """
        Split block to set of calls with conditions and execute them. Nested blocks are supported

        :param block:
        :return: dataframe with result of block execution
        """

        if not isinstance(block, ConditionBlock):
            # single condition
            if self.is_content_condition(block):
                return self.execute_content_condition(block)
            else:
                return self.call_kb([block])

        if block.op == "AND":
            results = []

            content_filters, other_filters = [], []
            for item in block.items:
                if isinstance(item, ConditionBlock):
                    results.append(self.execute_blocks(item))
                else:
                    if self.is_content_condition(item):
                        content_filters.append(item)
                    else:
                        other_filters.append(item)
            if len(content_filters) > 0:
                content_filters2 = []
                exclude_ids = set()
                include_contents = set()
                # exclude content conditions
                for condition in content_filters:
                    ids = self.to_excluded_ids(condition, other_filters)
                    if ids is not None:
                        exclude_ids.update(ids)
                        continue
                    contents = self.to_include_content(condition)
                    if contents is not None:
                        include_contents.update(contents)
                        continue
                    else:
                        # keep origin content filter
                        content_filters2.append(condition)

                if exclude_ids:
                    # add to filter
                    values = [Constant(i) for i in exclude_ids]
                    condition = BinaryOperation(op="NOT IN", args=[Identifier(self.id_column), Tuple(values)])
                    other_filters.append(condition)
                # execute content filters
                if include_contents:
                    content = " AND ".join(include_contents)
                    result = self.execute_content_condition(
                        BinaryOperation(op="=", args=[Identifier(self.content_column), Constant(content)]),
                        other_filters,
                    )
                    results.append(result)
                for condition in content_filters2:
                    result = self.execute_content_condition(condition, other_filters)
                    results.append(result)
            elif len(other_filters) > 0:
                results.append(self.call_kb(other_filters))

            return self.intersect(results)

        elif block.op == "OR":
            results = []
            for item in block.items:
                results.append(self.execute_blocks(item))

            return self.union(results)

    def run(self, query: Select) -> pd.DataFrame:
        """
        Plan and execute query to KB. If query has complex conditions:
         - convert them to several queries with simple conditions, execute them and combine results

        Stages:
        - Remove unary NOT from condition: try to apply it to related operator
        - Flat conditions tree: convert into condition blocks:
           - having with same operators of the same levels in the same block
        - Recursively execute blocks
           - get data from OR blocks and union them
           - get data from AND blocks and intersect them

        :param query: select query
        :return: results
        """
        if query.where is not None:
            query_traversal(query.where, self.convert_unary_ops)
            blocks_tree = self.flatten_conditions(query.where)
            if query.limit is not None:
                self.limit = query.limit.value
            return self.execute_blocks(blocks_tree)
        else:
            return self.kb.select(query)
