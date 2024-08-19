from mindsdb_sql.parser.ast.base import ASTNode


class Latest(ASTNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, alias=None, parentheses=False, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        return '\t'*level + 'Latest()'

    def get_string(self, *args, **kwargs):
        return 'LATEST'
