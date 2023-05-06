import base64

import pandas as pd
from mindsdb_sql import ASTNode, Star
from mindsdb_sql.parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from email.mime.text import MIMEText


class GmailApiTable(APITable):
    def __init__(self, handler):
        super().__init__(handler)

    def select(self, query: ast.Select) -> pd.DataFrame:
        conditions = extract_comparison_conditions(query.where)
        params = {}
        if conditions:
            arg1 = conditions[0][1]
            arg2 = conditions[0][2]
            params = {}
            if query.limit is not None:
                if query.limit.value < 500:
                    params['maxResults'] = query.limit.value
                else:
                    params['maxResults'] = 50
            params['query'] = arg2
        emails = self.handler.call_application_api(method_name='get_emails', params=params)
        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")
        if len(selected_columns) > 0:
            emails = emails[selected_columns]
        # Rename columns
        for target in query.targets:
            print(emails.columns)
            print(target.alias)
            if target.alias:
                emails.rename(columns={target.parts[-1]: str(target.alias)}, inplace=True)
        print(selected_columns)
        return emails

    def insert(self, query: ASTNode) -> None:
        columns = [col.name for col in query.columns]
        values = query.values[0]
        insert_parameters = {}
        for k in zip(columns, values):
            insert_parameters[k[0]] = k[1]
        message = MIMEText(insert_parameters['body'])
        message['to'] = insert_parameters['to']
        message['subject'] = insert_parameters['subject']
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        params = {'raw': raw_message}
        self.handler.call_application_api(method_name='send_email', params=params)

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
