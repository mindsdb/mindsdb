
import ast
from typing import Any

import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler, APITable, FuncParser
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from newsapi import NewsApiClient, NewsAPIException

from mindsdb.microservices_grpc.db.common_pb2 import StatusResponse
import urllib

class NewsAPIArticleTable(APITable):

    def __init__(self, handler):
        super().__init__(handler)
    
    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)

        params = {}
        
        
        for op, arg1, arg2 in conditions:

            if arg1 == 'query':
                params['query'] = urllib.parse.quote_plus(arg2)
            elif arg1 == 'sources':
                if len(arg2.split(",")) > 20:
                    raise ValueError("The number of items it sources should be 20 or less")
                else:
                    params[arg1] = arg2
            elif arg1 == 'publishedAt':
                if op == 'Gt' or op == 'GtE':
                    params['from'] = arg2
                if op == 'Lt' or op == 'LtE':
                    params['to'] = arg2
                elif op == 'Eq':
                    params['from'] = arg2
                    params['to'] = arg2
            else:
                params[arg1] = arg2
        
        if query.limit:
            if query.limit.value > 100:
                params['page_size'], params['page'] = divmod(query.limit.value, 100)
            params['page_size'] = query.limit.value
            params['page'] = 1
        else:
            params['page_size'] = 100
            params['page'] = 1
        
        if query.order_by:
            if len(query.order_by) == 1:
                if query.order_by[0] not in ['query', 'publishedAt']:
                    raise NotImplementedError("Not supported ordering by this field")
                params['sortBy'] == query.order_by[0]
            else: 
                raise ValueError("Multiple order by condition is not supported by the API")

        result = self.handler.call_newsapi_api(params=params)

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        return result[selected_columns]
    
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
        pages = params['pages']
        data = []
        
        for page in range(pages):
            params['pages'] = page
            result = self.api.get_everything(params)
            articles = result['articles']
            articles['source_id'] = articles['source']['id']
            articles['source_name'] = articles['source']['name']
            del articles['source']
            articles['query'] = params['query']
            articles['searchIn'] = params['qintitle']
            articles['domains'] = params['domains']
            articles['excludedDomains'] = params['exclude_domains']

            data.extend(articles)
            
        return pd.DataFrame(data=data)
        
