import pandas as pd
from mindsdb_sql import ASTNode, Star
from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.gmail_handler.gmail_handler import GmailHandler
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class GmailApiTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        print(query.where)
        conditions = extract_comparison_conditions(query.where)
        print(conditions)
        return pd.DataFrame()

    def insert(self, query: ASTNode) -> None:
        pass

    def update(self, query: ASTNode) -> None:
        pass

    def delete(self, query: ASTNode) -> None:
        pass

    def get_columns(self) -> list:
        pass


gmail = GmailHandler(name='gmail', connection_data={'path_to_credentials_file': '/home/marios/PycharmProjects/mindsdb'
                                                                                '/mindsdb/integrations/handlers'
                                                                                '/gmail_handler/credentials.json',
                                                    'scopes': 'https://www.googleapis.com/auth/gmail.readonly'})

gmail.connect()
print(gmail.check_connection())
print(gmail.get_last_n_emails(10))
gmail_table = GmailApiTable(gmail)
select_all = ast.Select(
    targets=[Star()],
    from_table='gmail_data',
    where='aggregated_trade_data.symbol = "symbol"'
)
print(gmail_table.select(select_all))
