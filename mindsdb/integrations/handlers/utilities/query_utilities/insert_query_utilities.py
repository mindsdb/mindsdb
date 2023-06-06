from typing import Text, List

from mindsdb_sql.parser import ast

from .exceptions import UnsupportedColumnException, MandatoryColumnException


class INSERTQueryParser:
    def __init__(self, query: ast.Insert, supported_columns: List[Text], mandatory_columns: List[Text]):
        self.query = query
        self.supported_columns = supported_columns
        self.mandatory_columns = mandatory_columns

    def parse_columns(self):
        columns = self.query.columns
        if not set(columns).issubset(self.supported_columns):
            unsupported_columns = set(columns).difference(self.supported_columns)
            raise UnsupportedColumnException(f"Unsupported columns: {', '.join(unsupported_columns)}")

        if not set(self.mandatory_columns).issubset(columns):
            missing_mandatory_columns = set(self.mandatory_columns).difference(columns)
            raise MandatoryColumnException(f"Mandatory columns missing: {', '.join(missing_mandatory_columns)}")

        return self.query.columns

    def parse_values(self):
        return self.query.values