from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from typing import List

try:
    from sqlalchemy import types as sa_types
except ImportError:
    sa_types = None


class TableColumn():
    def __init__(self, name, type='integer', length=None):
        self.name = name
        self.type = type
        self.is_primary_key = False
        self.default = None
        self.length = length

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        for k in ['name', 'is_primary_key', 'type', 'default', 'length']:

            if getattr(self, k) != getattr(other, k):
                return False

        return True


class CreateTable(ASTNode):
    def __init__(self,
                 name,
                 from_select=None,
                 columns: List[TableColumn] = None,
                 is_replace=False,
                 if_not_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.is_replace = is_replace
        self.from_select = from_select
        self.columns = columns
        self.if_not_exists = if_not_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        ind2 = indent(level + 2)

        replace_str = ''
        if self.is_replace:
            replace_str = f'{ind1}is_replace=True\n'

        from_select_str = ''
        if self.from_select is not None:
            from_select_str = f'{ind1}from_select={self.from_select.to_tree(level=level+1)}\n'

        columns_str = ''
        if self.columns is not None:
            columns = [
                f'{ind2}{col.name}: {col.type}'
                for col in self.columns
            ]

            columns_str = f'{ind1}columns=\n' + '\n'.join(columns)

        out_str = f'{ind}CreateTable(\n' \
                  f'{ind1}if_not_exists={self.if_not_exists},\n' \
                  f'{ind1}name={self.name}\n' \
                  f'{replace_str}' \
                  f'{from_select_str}' \
                  f'{columns_str}\n' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):

        replace_str = ''
        if self.is_replace:
            replace_str = f' OR REPLACE'

        columns_str = ''
        if self.columns is not None:
            columns = []
            for col in self.columns:

                if not isinstance(col.type, str) and sa_types is not None:
                    if issubclass(col.type, sa_types.Integer):
                        type = 'int'
                    elif issubclass(col.type, sa_types.Float):
                        type = 'float'
                    elif issubclass(col.type, sa_types.Text):
                        type = 'text'
                else:
                    type = str(col.type)
                if col.length is not None:
                    type = f'{type}({col.length})'
                columns.append( f'{col.name} {type}')

            columns_str = '({})'.format(', '.join(columns))

        from_select_str = ''
        if self.from_select is not None:
            from_select_str = self.from_select.to_string()

        name_str = str(self.name)
        return f'CREATE{replace_str} TABLE {"IF NOT EXISTS " if self.if_not_exists else ""}{name_str} {columns_str} {from_select_str}'
