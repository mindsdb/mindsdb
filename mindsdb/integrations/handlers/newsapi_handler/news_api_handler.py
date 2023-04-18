
import ast
from typing import Any

import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from newsapi import NewsApiClient, NewsAPIException

from mindsdb.microservices_grpc.db.common_pb2 import StatusResponse

class NewsAPIArticleTable(APITable):

    def __init__(self, handler):
        super().__init__(handler)
    
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:

            if op == 'or':
                raise NotImplementedError(f'OR is not supported')
            if op == 'And':
                pass 
            if op == 'Eq':
                # Exact match
                pass 
    
    def get_columns(self) -> list:
        return [
            'author',
            'title',
            'description',
            'url',
            'urlToImage',
            'publishedAt',
            'content',
            'source_id',
            'source_name',
            'query',
            'searchIn',
            'sources',
            'domains',
            'excludedDomains',
        ]

class NewsAPIHandler(APIHandler):

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.api = None
        self.api = NewsApiClient(api_key=kwargs['api_key'])

        self.is_connected = False

        article = NewsAPIArticleTable(self)
        self._register_table('tweets', article)
    
    def _register_table(self, table_name: str, table_class: Any):
        return super()._register_table(table_name, table_class)

    def connect(self):
        return self.api
    
    def check_connection(self) -> HandlerStatusResponse:
        response = StatusResponse(False)

        response = StatusResponse(False)

        try:
            self.api.get_top_headlines()
            response.success = True

        except NewsAPIException as e:
            response.error_message = e.message

        return response
    
    def native_query(self, query: Any) -> HandlerResponse:
        return super().native_query(query)
    
    def call_newsapi_api(self, method_name:str = None, params:dict = None):
        # This will implement api base on the native query
        # By processing native query to convert it to api callable parameters
        self.api.get_everything(q=params["query"])
        pass
