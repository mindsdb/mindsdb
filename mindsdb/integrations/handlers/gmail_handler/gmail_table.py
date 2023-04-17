import pandas as pd
from mindsdb_sql import ASTNode

from mindsdb.integrations.libs.api_handler import APITable
class GmailApiTable(APITable):

    def select(self, query: ASTNode) -> pd.DataFrame:
        pass

    def insert(self, query: ASTNode) -> None:
        pass

    def update(self, query: ASTNode) -> None:
        pass

    def delete(self, query: ASTNode) -> None:
        pass

    def get_columns(self) -> list:
        pass