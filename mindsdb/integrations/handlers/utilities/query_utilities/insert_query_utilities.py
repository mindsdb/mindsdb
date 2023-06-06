from typing import Text, List

from mindsdb_sql.parser import ast

from .exceptions import UnsupportedColumnException


class INSERTQueryParser:
    def __init__(self, query: ast.Insert, supported_columns: List[Text]):
        self.query = query
        self.supported_columns = supported_columns

    def parse_columns(self):
        columns = self.query.columns
        if not set(columns).issubset(self.supported_columns):
            unsupported_columns = set(columns).difference(self.supported_columns)
            raise UnsupportedColumnException(f"Unsupported columns: {', '.join(unsupported_columns)}")

        return self.query.columns

    def parse_values(self):
        return self.query.values