from typing import List
import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast


class StatusPages(APITable):

    # table name in the database
    name = 'status_pages'

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Receive query as AST (abstract syntax tree) and act upon it.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Select

        Returns:
            HandlerResponse
        """

        # TODO
        raise NotImplementedError()

        return pd.DataFrame()

    def insert(self, query: ast.Insert) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Insert

        Returns:
            None
        """
        # TODO
        raise NotImplementedError()

        return None

    def update(self, query: ast.Update) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Update
        Returns:
            None
        """
        # TODO
        raise NotImplementedError()

        return None

    def delete(self, query: ast.Delete) -> None:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.

        Args:
            query (ASTNode): sql query represented as AST. Usually it should be ast.Delete

        Returns:
            None
        """
        # TODO
        raise NotImplementedError()

        return None

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """
        return [item for item in self.columns if item not in ignore]
