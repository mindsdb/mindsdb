from mindsdb_sql.parser.ast import Show
from mindsdb_sql.parser.utils import indent


class ShowIndex(Show):
    def __init__(self,
                 table,
                 db=None,
                 *args_, **kwargs):
        super().__init__(*args_, category='INDEX', **kwargs)
        self.table = table
        self.db = db

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        table_str = f'{ind1}table={self.table.to_tree()},'
        db_str = f'{ind1}db={self.db.to_tree()},' if self.db else ''
        out_str = f'{ind}ShowIndex(' \
                  f'{table_str}' \
                  f'{db_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        db_str = f' FROM {self.db}' if self.db else ''
        return f'SHOW INDEX FROM {self.table}{db_str}'
