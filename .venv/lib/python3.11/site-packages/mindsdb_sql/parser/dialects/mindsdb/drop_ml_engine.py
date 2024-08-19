from mindsdb_sql.parser.ast.drop import Drop
from mindsdb_sql.parser.utils import indent


class DropMLEngine(Drop):
    def __init__(self,
                 name,
                 if_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_tree()},'

        out_str = f'{ind}DropMLEngine(' \
                  f'{ind1}if_exists={self.if_exists},' \
                  f'{name_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP ML_ENGINE {"IF EXISTS " if self.if_exists else ""}{str(self.name)}'
        return out_str

