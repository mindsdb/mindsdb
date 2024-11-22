from typing import List

import pandas as pd

from mindsdb_sql_parser.ast import (
    Identifier, BinaryOperation, Last, Constant, ASTNode
)
from mindsdb.integrations.utilities.query_traversal import query_traversal

from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx

from .last_query import LastQuery


class QueryContextController:
    IGNORE_CONTEXT = '<IGNORE>'

    def handle_db_context_vars(self, query: ASTNode, dn, session) -> tuple:
        """
        Check context variables in query and replace them with values.
        Should be used before exec query in database.

        Input:
        - query: input query
        - params are used to find current values of context variables
          - dn: datanode
          - session: mindsdb server session

        Returns:
         - query with replaced context variables
         - callback to call with result of the query. it is used to update context variables

        """
        context_name = self.get_current_context()

        l_query = LastQuery(query)
        if l_query.query is None:
            # no last keyword, exit
            return query, None

        if context_name == self.IGNORE_CONTEXT:
            # return with empty constants
            return l_query.query, None

        query_str = l_query.to_string()

        rec = self._get_context_record(context_name, query_str)

        if rec is None or len(rec.values) == 0:
            values = self._get_init_last_values(l_query, dn, session)
            if rec is None:
                self.__add_context_record(context_name, query_str, values)
                if context_name.startswith('job-if-'):
                    # add context for job also
                    self.__add_context_record(context_name.replace('job-if', 'job'), query_str, values)
            else:
                rec.values = values
        else:
            values = rec.values

        db.session.commit()

        query_out = l_query.apply_values(values)

        def callback(df, columns_info):
            self._result_callback(l_query, context_name, query_str, df, columns_info)

        return query_out, callback

    def remove_lasts(self, query):
        def replace_lasts(node, **kwargs):

            # find last in where
            if isinstance(node, BinaryOperation):
                if isinstance(node.args[0], Identifier) and isinstance(node.args[1], Last):
                    node.args = [Constant(0), Constant(0)]
                    node.op = '='

        # find lasts
        query_traversal(query, replace_lasts)
        return query

    def _result_callback(self, l_query: LastQuery,
                         context_name: str, query_str: str,
                         df: pd.DataFrame, columns_info: list):
        """
        This function handlers result from executed query and updates context variables with new values

        Input
        - l_query: LastQuery object
        - To identify context:
          - context_name: name of the context
          - query_str: rendered query to search in context table
        - result of the query
          - data: list of dicts
          - columns_info: list

        """
        if len(df) == 0:
            return

        values = {}
        # get max values
        for info in l_query.get_last_columns():
            target_idx = info['target_idx']
            if target_idx is not None:
                # get by index
                col_name = columns_info[target_idx]['name']
            else:
                col_name = info['column_name']
                # get by name
            if col_name not in df:
                continue

            column_values = df[col_name].dropna()
            try:
                value = max(column_values)
            except (TypeError, ValueError):
                try:
                    # try to convert to float
                    value = max(map(float, column_values))
                except (TypeError, ValueError):
                    try:
                        # try to convert to str
                        value = max(map(str, column_values))
                    except (TypeError, ValueError):
                        continue

            if value is not None:
                values[info['table_name']] = {info['column_name']: value}

        self.__update_context_record(context_name, query_str, values)

    def drop_query_context(self, object_type: str, object_id: int = None):
        """
        Drop context for object
        :param object_type: type of the object
        :param object_id: id
        """

        context_name = self.gen_context_name(object_type, object_id)
        for rec in db.session.query(db.QueryContext).filter_by(
            context_name=context_name,
            company_id=ctx.company_id
        ).all():
            db.session.delete(rec)
        db.session.commit()

    def _get_init_last_values(self, l_query: LastQuery, dn, session) -> dict:
        """
        Gets current last values for query.
        Creates and executes query for it:
           'select <col> from <table> order by <col> desc limit 1"
        """
        last_values = {}
        for query, info in l_query.get_init_queries():

            data, columns_info = dn.query(
                query=query,
                session=session
            )

            if len(data) == 0:
                value = None
            else:
                row = list(data.iloc[0])

                idx = None
                for i, col in enumerate(columns_info):
                    if col['name'].upper() == info['column_name'].upper():
                        idx = i
                        break

                if idx is None or len(row) == 1:
                    value = row[0]
                else:
                    value = row[idx]

            if value is not None:
                last_values[info['table_name']] = {info['column_name']: value}

        return last_values

    # Context

    def get_current_context(self) -> str:
        """
        returns current context name
        """
        try:
            context_stack = ctx.context_stack or []
        except AttributeError:
            context_stack = []
        if len(context_stack) > 0:
            return context_stack[-1]
        else:
            return ''

    def set_context(self, object_type: str = None, object_id: int = None):
        """
        Updates current context name, using object name and id
        Previous context names are stored on lower levels of stack
        """
        try:
            context_stack = ctx.context_stack or []
        except AttributeError:
            context_stack = []
        context_stack.append(self.gen_context_name(object_type, object_id))
        ctx.context_stack = context_stack

    def release_context(self, object_type: str = None, object_id: int = None):
        """
        Removed current context (defined by object type and id) and restored previous one
        """
        try:
            context_stack = ctx.context_stack or []
        except AttributeError:
            context_stack = []
        if len(context_stack) == 0:
            return
        context_name = self.gen_context_name(object_type, object_id)
        if context_stack[-1] == context_name:
            context_stack.pop()
        ctx.context_stack = context_stack

    def gen_context_name(self, object_type: str, object_id: int) -> str:
        """
        Generated name of the context according to object type and name
        :return: context name
        """

        if object_type is None:
            return ''
        if object_id is not None:
            object_type += '-' + str(object_id)
        return object_type

    def get_context_vars(self, object_type: str, object_id: int) -> List[dict]:
        """
        Return variables stored in context (defined by object type and id)

        :return: list of all context variables related to context name how they stored in context table
        """
        context_name = self.gen_context_name(object_type, object_id)
        vars = []
        for rec in db.session.query(db.QueryContext).filter_by(
            context_name=context_name,
            company_id=ctx.company_id
        ):
            if rec.values is not None:
                vars.append(rec.values)

        return vars

    # DB
    def _get_context_record(self, context_name: str, query_str: str) -> db.QueryContext:
        """
        Find and return record for context and query string
        """

        return db.session.query(db.QueryContext).filter_by(
            query=query_str,
            context_name=context_name,
            company_id=ctx.company_id
        ).first()

    def __add_context_record(self, context_name: str, query_str: str, values: dict) -> db.QueryContext:
        """
        Creates record (for context and query string) with values and returns it
        """
        rec = db.QueryContext(
            query=query_str,
            context_name=context_name,
            company_id=ctx.company_id,
            values=values)
        db.session.add(rec)
        return rec

    def __update_context_record(self, context_name: str, query_str: str, values: dict):
        """
        Updates context record with new values
        """
        rec = self._get_context_record(context_name, query_str)
        rec.values = values
        db.session.commit()


query_context_controller = QueryContextController()
