from mindsdb_sql.parser.ast import ASTNode
from mindsdb_sql.parser.utils import indent


class Star(ASTNode):
    def __init__(self, *args, **kwargs):
        if 'alias' in kwargs:
            from mindsdb_sql import ParsingException
            raise ParsingException("Can't alias a star!")
        super().__init__(*args, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        return indent(level) + f'Star()'

    def get_string(self, *args, **kwargs):
        return '*'
