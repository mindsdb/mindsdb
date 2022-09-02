from select import select
from pandas import DataFrame
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Show, Select

from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

import_error = None

type_func = type


class Handler:
    def __init__(self, name, **kwargs):
        pass

    def check_connection(self):
        pass

    def get_tables(self):
        all_models_names = ['test']
        response = Response(
            RESPONSE_TYPE.TABLE,
            DataFrame(
                all_models_names,
                columns=['table_name']
            )
        )
        return response

    def get_columns(self, table_name: str):
        pass

    def native_query(self, query: str):
        query_ast = parse_sql(query, dialect='mindsdb')
        return self.query(query_ast)

    def query(self, query):
        statement = query
        if type_func(statement) == Show:
            if statement.category.lower() == 'tables':
                all_models_names = ['test']
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    DataFrame(
                        all_models_names,
                        columns=['table_name']
                    )
                )
                return response
        if type_func(statement) == Select:
            response = Response(
                RESPONSE_TYPE.TABLE,
                DataFrame(
                    [[1, 2, 3]],
                    columns=['x', 'y', 'z']
                )
            )
            return response

    def predict(self, model_name: str, data: list, pred_format):
        pass


version = '0.0.1'
title = 'dummyml'
name = 'dummyml'
type = HANDLER_TYPE.ML

__all__ = [
    'Handler', 'version', 'name', 'type', 'title', 'import_error'
]
