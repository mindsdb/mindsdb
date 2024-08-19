from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.select.identifier import Identifier

class CreateView(ASTNode):
    def __init__(self,
                 name,
                 query_str,
                 from_table=None,
                 if_not_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        # todo remove it
        if isinstance(name, Identifier):
            name = name.to_string()
        self.name = name
        self.query_str = query_str
        self.from_table = from_table
        self.if_not_exists = if_not_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={repr(self.name)},'
        # name_str = f'\n{ind1}name={self.name.to_string()},'
        from_table_str = f'\n{ind1}from_table=\n{self.from_table.to_tree(level=level+2)},' if self.from_table else ''
        query_str = f'\n{ind1}query="{self.query_str}"'
        if_not_exists_str = f'\n{ind1}if_not_exists=True,' if self.if_not_exists else ''

        out_str = f'{ind}CreateView(' \
                  f'{if_not_exists_str}' \
                  f'{name_str}' \
                  f'{query_str}' \
                  f'{from_table_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        from_str = f'FROM {str(self.from_table)} ' if self.from_table else ''
        # out_str = f'CREATE VIEW {self.name.to_string()} {from_str}AS ( {self.query_str} )'
        out_str = f'CREATE VIEW {"IF NOT EXISTS " if self.if_not_exists else ""}{str(self.name)} {from_str}AS ( {self.query_str} )'

        return out_str

