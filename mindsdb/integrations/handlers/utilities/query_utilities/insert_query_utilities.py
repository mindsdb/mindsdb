from typing import Text, List, Dict, Any, Optional

from mindsdb_sql.parser import ast

from .exceptions import UnsupportedColumnException, MandatoryColumnException


class INSERTQueryParser:
    def __init__(self, query: ast.Insert, supported_columns: Optional[List[Text]] = None, mandatory_columns: Optional[List[Text]] = None, all_mandatory: Optional[Any] = True):
        self.query = query
        self.supported_columns = supported_columns
        self.mandatory_columns = mandatory_columns
        self.all_mandatory = all_mandatory

    def parse_query(self) -> Dict[Text, Any]:
        columns = self.parse_columns()
        values = self.parse_values()

        return dict(zip(columns, values))

    def parse_columns(self):
        columns = [col.name for col in self.query.columns]

        if self.supported_columns:
            if not set(columns).issubset(self.supported_columns):
                unsupported_columns = set(columns).difference(self.supported_columns)
                raise UnsupportedColumnException(f"Unsupported columns: {', '.join(unsupported_columns)}")

        if self.mandatory_columns:
            if self.all_mandatory:
                if not set(self.mandatory_columns).issubset(columns):
                    missing_mandatory_columns = set(self.mandatory_columns).difference(columns)
                    raise MandatoryColumnException(f"Mandatory columns missing: {', '.join(missing_mandatory_columns)}")
            else:
                if not set(self.mandatory_columns).intersection(columns):
                    missing_mandatory_columns = set(self.mandatory_columns).difference(columns)
                    raise MandatoryColumnException(f"Mandatory columns missing: {', '.join(missing_mandatory_columns)}")

        return columns

    def parse_values(self):
        return self.query.values