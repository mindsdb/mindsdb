from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast.drop import Drop

from mindsdb_sql.parser.utils import indent


class CreateTrigger(ASTNode):
    def __init__(self,
                 name,
                 table,
                 query_str,
                 columns=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.table = table
        self.query_str = query_str
        self.columns = columns

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_string()},'

        table_str = f'\n{ind1}table={self.table.to_string()},'

        query_str = f'\n{ind1}query_str={repr(self.query_str)},'

        columns_str = ''
        if self.columns:
            columns_str = ', '.join([k.to_string() for k in self.columns])
            columns_str = f'\n{ind1}columns=[{columns_str}],'

        out_str = f'{ind}CreateTrigger(' \
                  f'{name_str}' \
                  f'{table_str}' \
                  f'{columns_str}' \
                  f'{query_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        columns_str = ''
        if self.columns:
            columns_str = ', '.join([k.to_string() for k in self.columns])
            columns_str = f' columns {columns_str}'

        out_str = f'CREATE TRIGGER {self.name.to_string()} ON {self.table.to_string()}{columns_str} ({self.query_str})'
        return out_str


class DropTrigger(Drop):
    def __init__(self,
                 name,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_tree()},'

        out_str = f'{ind}DropTrigger(' \
                  f'{name_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP TRIGGER {str(self.name)}'
        return out_str

