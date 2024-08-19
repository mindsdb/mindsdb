from typing import List

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Data(ASTNode):

    def __init__(self, data: List[dict], *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.data = data

    def to_tree(self, *args, level=0, **kwargs):
        return indent(level) + \
               f'Data(len={len(self.data)})'

    def get_string(self, *args, **kwargs):
        return f'"<{len(self.data)} rows>"'
