from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Tuple(ASTNode):
    def __init__(self, items, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items = items

    def to_tree(self, *args, level=0, **kwargs):
        item_trees = ','.join([t.to_tree(level=0) for t in self.items])

        out_str = indent(level) + f'Tuple(items=({item_trees}))'
        return out_str

    def get_string(self, *args, **kwargs):
        item_strs = []
        for item in self.items:
            item_strs.append(str(item))

        return f'({", ".join(item_strs)})'
