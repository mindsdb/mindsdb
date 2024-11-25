import pandas as pd
import json
import symbl
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities import log
from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class GetConversationTable(APITable):
    """The Get Conversation Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the conversation Id for the given audio file" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            conversation id

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_conversation_id',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'audio_url':
                if op == '=':
                    search_params["audio_url"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for audio_url column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("audio_url" in search_params)

        if not filter_flag:
            raise NotImplementedError("audio_url column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        payload = {"url": search_params.get("audio_url")}
        conversation_object = symbl.Audio.process_url(payload=payload, credentials=self.handler.credentials)

        df = pd.json_normalize({"conversation_id": conversation_object.get_conversation_id()})

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "conversation_id"
        ]


class GetMessagesTable(APITable):
    """The Get Messages Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the messages for the given conversation id" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Messages

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_messages',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'conversation_id':
                if op == '=':
                    search_params["conversation_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for conversation_id column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("conversation_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("conversation_id column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        resp = symbl.Conversations.get_messages(conversation_id=search_params.get("conversation_id"), credentials=self.handler.credentials)

        resp = self.parse_response(resp)

        df = pd.json_normalize(resp["messages"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def parse_response(self, res):
        return json.loads(json.dumps(res.to_dict(), default=str))

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "conversation_id",
            "end_time",
            "id",
            "phrases",
            "sentiment",
            "start_time",
            "text",
            "words",
            "_from.email",
            "_from.id",
            "_from.name"
        ]


class GetTopicsTable(APITable):
    """The Get Topics Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the topics for the given conversation id" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Topics

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_topics',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'conversation_id':
                if op == '=':
                    search_params["conversation_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for conversation_id column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("conversation_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("conversation_id column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        resp = symbl.Conversations.get_topics(conversation_id=search_params.get("conversation_id"), credentials=self.handler.credentials)

        resp = self.parse_response(resp)

        df = pd.json_normalize(resp["topics"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def parse_response(self, res):
        return json.loads(json.dumps(res.to_dict(), default=str))

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "id",
            "text",
            "type",
            "score",
            "message_ids",
            "entities",
            "sentiment",
            "parent_refs"
        ]


class GetQuestionsTable(APITable):
    """The Get Questions Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the questions for the given conversation id" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Questions

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_questions',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'conversation_id':
                if op == '=':
                    search_params["conversation_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for conversation_id column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("conversation_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("conversation_id column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        resp = symbl.Conversations.get_questions(conversation_id=search_params.get("conversation_id"), credentials=self.handler.credentials)

        resp = self.parse_response(resp)

        df = pd.json_normalize(resp["questions"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def parse_response(self, res):
        return json.loads(json.dumps(res.to_dict(), default=str))

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "id",
            "text",
            "type",
            "score",
            "message_ids",
            "_from.id",
            "_from.name",
            "_from.user_id"
        ]


class GetFollowUpsTable(APITable):
    """The Get FollowUps Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the follow ups for the given conversation id" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            follow up Questions

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_follow_ups',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'conversation_id':
                if op == '=':
                    search_params["conversation_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for conversation_id column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("conversation_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("conversation_id column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        resp = symbl.Conversations.get_follow_ups(conversation_id=search_params.get("conversation_id"), credentials=self.handler.credentials)

        resp = self.parse_response(resp)

        df = pd.json_normalize(resp["follow_ups"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def parse_response(self, res):
        return json.loads(json.dumps(res.to_dict(), default=str))

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "id",
            "text",
            "type",
            "score",
            "message_ids",
            "entities",
            "phrases",
            "definitive",
            "due_by",
            "_from.id",
            "_from.name",
            "_from.user_id",
            "assignee.id",
            "assignee.name",
            "assignee.email"
        ]


class GetActionItemsTable(APITable):
    """The Get Action items Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the action items for the given conversation id" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            action items

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_action_items',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'conversation_id':
                if op == '=':
                    search_params["conversation_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for conversation_id column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("conversation_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("conversation_id column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        resp = symbl.Conversations.get_action_items(conversation_id=search_params.get("conversation_id"), credentials=self.handler.credentials)

        resp = self.parse_response(resp)

        df = pd.json_normalize(resp["action_items"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def parse_response(self, res):
        return json.loads(json.dumps(res.to_dict(), default=str))

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "id",
            "text",
            "type",
            "score",
            "message_ids",
            "entities",
            "phrases",
            "definitive",
            "due_by",
            "_from.id",
            "_from.name",
            "_from.user_id",
            "assignee.id",
            "assignee.name",
            "assignee.email"
        ]


class GetAnalyticsTable(APITable):
    """The Get Analytics Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Get the analytics for the given conversation id" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            metrics

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'get_analytics',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'conversation_id':
                if op == '=':
                    search_params["conversation_id"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for conversation_id column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("conversation_id" in search_params)

        if not filter_flag:
            raise NotImplementedError("conversation_id column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        resp = symbl.Conversations.get_analytics(conversation_id=search_params.get("conversation_id"), credentials=self.handler.credentials)

        resp = self.parse_response(resp)

        df = pd.json_normalize(resp["metrics"])

        select_statement_executor = SELECTQueryExecutor(
            df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        df = select_statement_executor.execute_query()

        return df

    def parse_response(self, res):
        return json.loads(json.dumps(res.to_dict(), default=str))

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "type",
            "percent",
            "seconds"
        ]
