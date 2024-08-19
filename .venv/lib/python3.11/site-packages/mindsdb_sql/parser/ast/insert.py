from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.create import TableColumn
from mindsdb_sql.parser.ast.select.identifier import Identifier
from mindsdb_sql.parser.ast.select.constant import Constant

class Insert(ASTNode):

    def __init__(self,
                 table,
                 columns=None,
                 values=None,
                 from_select=None,
                 is_plain=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table

        if columns is not None:
            self.columns = [
                self.to_column(col)
                for col in columns
            ]
        else:
            self.columns = None

        # TODO require one of [values, from_select] is set
        self.values = values
        self.from_select = from_select

        # True if values in query are constant (without subselects and operations)
        self.is_plain = is_plain

    def to_column(self, col):
        if isinstance(col, str):
            return TableColumn(col)
        elif isinstance(col, Identifier):
            return TableColumn(col.parts[0])
        elif isinstance(col, Constant):
            return TableColumn(col.value)
        return TableColumn(str(col))

    def to_value(self, val):
        if isinstance(val, ASTNode) :
            return val.to_string()
        return repr(val)

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        ind2 = indent(level + 2)
        if self.columns is not None:
            columns_str = ', '.join([i.name for i in self.columns])
        else:
            columns_str = ''

        if self.values is not None:
            values = []
            for row in self.values:
                row_str = f', '.join([self.to_value(i) for i in row])
                values.append(f'{ind2}[{row_str}]')
            values_str = f'\n'.join(values)
            values_str = f'{ind1}values=[\n{values_str}]\n'
        else:
            values_str = ''

        if self.from_select is not None:
            from_select_str = f'{ind1}from_select=\n{self.from_select.to_tree(level=level+2)}\n'
        else:
            from_select_str = ''

        out_str = f'{ind}Insert(table={self.table.to_tree()}\n' \
                  f'{ind1}columns=[{columns_str}]\n' \
                  f'{values_str}' \
                  f'{from_select_str}' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):
        if self.columns is not None:
            cols = ', '.join([i.name for i in self.columns])
            columns_str = f'({cols})'
        else:
            columns_str = ''

        if self.values is not None:
            values = []
            for row in self.values:
                row_str = ', '.join([self.to_value(i) for i in row])
                values.append(f'({row_str})')
            values_str = 'VALUES ' + ', '.join(values)
        else:
            values_str = ''

        if self.from_select is not None:
            from_select_str = self.from_select.to_string()
        else:
            from_select_str = ''

        return f'INSERT INTO {str(self.table)}{columns_str} {values_str}{from_select_str}'
