import pandas as pd
from mindsdb_sql import ASTNode, Star
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class GmailApiTable(APITable):
    def __init__(self, handler):
        super().__init__(handler)

    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)
        arg1 = conditions[0][1]
        arg2 = conditions[0][2]
        query= arg2
        emails = self.handler.call_application_api(method_name='get_emails', query=query)
        return emails

    def insert(self, query: ASTNode) -> None:
        pass

    def update(self, query: ASTNode) -> None:
        pass

    def delete(self, query: ASTNode) -> None:
        pass

    def get_columns(self) -> list:
        return ['id',
                'threadId',
                'labelIds',
                'snippet',
                'historyId',
                'mimeType',
                'filename',
                'Subject',
                'Sender',
                'To',
                'Date',
                'body',
                'sizeEstimate']
