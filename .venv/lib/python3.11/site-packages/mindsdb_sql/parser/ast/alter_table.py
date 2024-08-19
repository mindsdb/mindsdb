from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Alter(ASTNode):
    ...


class AlterTable(ASTNode):
    def __init__(self,
                 target,
                 arg,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target = target
        self.arg = arg

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        target_str = f'target={self.target.to_tree(level=level+2)}, '
        arg_str = f'arg={repr(self.arg)},'

        out_str = f'{ind}AlterTable(' \
                  f'{target_str}' \
                  f'{arg_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        return f'ALTER TABLE {str(self.target)} {self.arg}'

