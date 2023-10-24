import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser import ast

logger = get_log("integrations.nlpcloud_handler")


class NLPCloudTranslationTable(APITable):
    """The NLPCloud Translation Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.nlpcloud.com/#translation" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Translated Text

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'translation',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")
            elif arg1 == 'source':
                if op == '=':
                    search_params["source"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for source column.")
            elif arg1 == 'target':
                if op == '=':
                    search_params["target"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for target column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params) and ("source" in search_params) and ("target" in search_params)

        if not filter_flag:
            raise NotImplementedError("`text`, `source` and `target` columns have to be present in where clause.")

        translation_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.translation(search_params["text"], search_params["source"], search_params["target"])

        translation_df = pd.json_normalize(response)

        select_statement_executor = SELECTQueryExecutor(
            translation_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        translation_df = select_statement_executor.execute_query()

        return translation_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "translation_text"
        ]


class NLPCloudSummarizationTable(APITable):
    """The NLPCloud Summarization Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.nlpcloud.com/#summarization" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            summarized Text

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'summarization',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")
            elif arg1 == 'size':
                if op == '=':
                    search_params["size"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for size column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("`text` column has to be present in where clause.")

        summarization_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.summarization(search_params["text"], search_params.get("size", None))

        summarization_df = pd.json_normalize(response)

        select_statement_executor = SELECTQueryExecutor(
            summarization_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        summarization_df = select_statement_executor.execute_query()

        return summarization_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "summary_text"
        ]


class NLPCloudSentimentTable(APITable):
    """The NLPCloud Sentiment Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.nlpcloud.com/#sentiment-analysis" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            sentiment analysis Text

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'sentiment_analysis',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("`text` column has to be present in where clause.")

        sentiment_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.sentiment(search_params["text"])

        sentiment_df = pd.json_normalize(response["scored_labels"])

        select_statement_executor = SELECTQueryExecutor(
            sentiment_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        sentiment_df = select_statement_executor.execute_query()

        return sentiment_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "label",
            "score"
        ]


class NLPCloudParaphrasingTable(APITable):
    """The NLPCloud Paraphrasing Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.nlpcloud.com/#paraphrasing-and-rewriting" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            paraphrased Text

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'paraphrase',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("`text` column has to be present in where clause.")

        paraphrase_df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.paraphrasing(search_params["text"])

        paraphrase_df = pd.json_normalize(response)

        select_statement_executor = SELECTQueryExecutor(
            paraphrase_df,
            selected_columns,
            subset_where_conditions,
            order_by_conditions,
            result_limit
        )

        paraphrase_df = select_statement_executor.execute_query()

        return paraphrase_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "paraphrased_text"
        ]


class NLPCloudLangDetectionTable(APITable):
    """The NLPCloud Language Detection Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.nlpcloud.com/#language-detection" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            language detection

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'language_detection',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("`text` column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.langdetection(search_params["text"])

        df = pd.json_normalize(response)

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
            "languages"
        ]


class NLPCloudNERTable(APITable):
    """The NLPCloud NER Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the https://docs.nlpcloud.com/#entities" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            NER data

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'named_entity_recognition',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        search_params = {}
        subset_where_conditions = []
        for op, arg1, arg2 in where_conditions:
            if arg1 == 'text':
                if op == '=':
                    search_params["text"] = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for text column.")

            elif arg1 in self.get_columns():
                subset_where_conditions.append([op, arg1, arg2])

        filter_flag = ("text" in search_params)

        if not filter_flag:
            raise NotImplementedError("`text` column has to be present in where clause.")

        df = pd.DataFrame(columns=self.get_columns())

        response = self.handler.client.entities(search_params["text"])

        df = pd.json_normalize(response["entities"])

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
            "start",
            "end",
            "type",
            "text"
        ]
