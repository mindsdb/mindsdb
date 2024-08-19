from mindsdb_sql.parser.ast.base import ASTNode


class Parameter(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def __repr__(self):
        return f'Parameter({self.value})'

    def to_tree(self, *args, level=0, **kwargs):
        return '\t' * level + f'Parameter({repr(self.value)})'

    def get_string(self, *args, **kwargs):
        return ':' + str(self.value)
