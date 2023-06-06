from typing import Text, List, Dict, Tuple

from mindsdb_sql.parser import ast


class INSERTQueryParser:
    def __init__(self, query: ast.Select, columns: List[Text]):
        self.query = query
        self.columns = columns

    def parse_columns(self):
        return self.query.columns

    def parse_values(self):
        return self.query.values