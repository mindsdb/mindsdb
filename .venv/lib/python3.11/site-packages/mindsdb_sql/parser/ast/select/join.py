from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Join(ASTNode):
    def __init__(self, join_type, left, right, condition=None, implicit=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if join_type is not None:
            join_type = join_type.upper()
        self.join_type = join_type
        self.left = left
        self.right = right
        self.condition = condition
        self.implicit = implicit

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        args = f'implicit={repr(self.implicit)}, join_type={repr(self.join_type)}'
        left_str = f'\n{ind1}left=\n{self.left.to_tree(level=level+2)}'
        right_str = f'\n{ind1}right=\n{self.right.to_tree(level=level+2)}'
        condition_str = f'\n{ind1}condition=\n{self.condition.to_tree(level=level+2)}' if self.condition else ''

        out_str = f'{ind}Join({args},{left_str},{right_str},{condition_str}\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        join_type_str = f' {self.join_type} ' if not self.implicit else ', '
        condition_str = f' ON {self.condition.to_string()}' if self.condition else ''
        return f'{self.left.to_string()}{join_type_str}{self.right.to_string()}{condition_str}'
