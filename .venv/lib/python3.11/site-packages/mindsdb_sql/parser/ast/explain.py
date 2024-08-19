from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Explain(ASTNode):
    def __init__(self,
                 target,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target = target

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        target_str = f'target={self.target.to_tree(level=level+2)},'

        out_str = f'{ind}Explain(' \
                  f'{target_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        return f'EXPLAIN {str(self.target)}'

