from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Delete(ASTNode):
    def __init__(self,
                 table,
                 where=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.where = where

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)

        where_str = f'where=\n{self.where.to_tree(level=level + 2)},' if self.where else ''

        out_str = f'{ind}Delete(\n' \
                  f'{ind1}table={self.table.to_tree()}\n' \
                  f'{ind1}{where_str}\n' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):
        if self.where is not None:
            where_str = f' WHERE {self.where.to_string()}'
        else:
            where_str = ''

        return f'DELETE FROM {str(self.table)}{where_str}'
