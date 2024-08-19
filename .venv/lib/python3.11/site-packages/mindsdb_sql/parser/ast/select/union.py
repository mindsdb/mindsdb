from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Union(ASTNode):

    def __init__(self,
                 left,
                 right,
                 unique=True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.unique = unique

        if self.alias:
            self.parentheses = True

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        left_str = f'\n{ind1}left=\n{self.left.to_tree(level=level + 2)},'
        right_str = f'\n{ind1}right=\n{self.right.to_tree(level=level + 2)},'

        out_str = f'{ind}Union(unique={repr(self.unique)},' \
                  f'{left_str}' \
                  f'{right_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        left_str = str(self.left)
        right_str = str(self.right)
        keyword = 'UNION' if self.unique else 'UNION ALL'
        out_str = f"""{left_str}\n{keyword}\n{right_str}"""

        return out_str
