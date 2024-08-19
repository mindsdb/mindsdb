from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CommitTransaction(ASTNode):
    def __init__(self,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = f'{ind}CommitTransaction()'
        return out_str

    def get_string(self, *args, **kwargs):
        return f'commit'
