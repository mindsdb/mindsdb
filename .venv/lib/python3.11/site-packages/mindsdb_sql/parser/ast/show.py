from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Show(ASTNode):
    def __init__(self,
                 category,
                 modes=None,
                 from_table=None,
                 in_table=None,
                 where=None,
                 name=None,
                 like=None,
                 *args_, **kwargs):
        super().__init__(*args_, **kwargs)

        if category == 'SLAVE HOSTS':
            category = 'REPLICAS'

        self.category = category.upper()
        self.modes = modes
        self.where = where
        self.from_table = from_table
        self.in_table = in_table
        self.like = like
        self.name = name


    def to_tree(self, *args, level=0, **kwargs):

        ind = indent(level)
        ind1 = indent(level+1)
        category_str = f'{ind1}category={repr(self.category)},'
        from_str = f'\n{ind1}from={self.from_table.to_string()},' if self.from_table else ''
        in_str = f'\n{ind1}in={self.in_table.to_tree(level=level + 2)},' if self.in_table else ''
        where_str = f'\n{ind1}where=\n{self.where.to_tree(level=level+2)},' if self.where else ''
        name_str = f'\n{ind1}name={self.name},' if self.name else ''
        like_str = f'\n{ind1}like={self.like},' if self.like else ''
        modes_str = f'\n{ind1}modes=[{",".join(self.modes)}],' if self.modes else ''
        out_str = f'{ind}Show(' \
                  f'{category_str}' \
                  f'{name_str}' \
                  f'{modes_str}' \
                  f'{from_str}' \
                  f'{in_str}' \
                  f'{like_str}' \
                  f'{where_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):

        from_str = ''
        if self.from_table:
            ar = [
                f'FROM {i}'
                for i in self.from_table.parts
            ]
            ar.reverse()
            from_str = ' ' + ' '.join(ar)

        in_str = ''
        if self.in_table:
            ar = [
                f'IN {i}'
                for i in self.in_table.parts
            ]
            ar.reverse()
            in_str = ' ' + ' '.join(ar)

        modes_str = f' {" ".join(self.modes)}' if self.modes else ''
        like_str = f" LIKE '{self.like}'" if self.like else ""
        where_str = f' WHERE {str(self.where)}' if self.where else ''

        # custom commands
        if self.category in ('FUNCTION CODE', 'PROCEDURE CODE', 'ENGINE') or self.category.startswith('ENGINE '):
            return f'SHOW {self.category} {self.name}'
        elif self.category == 'REPLICA STATUS':
            channel = ''
            if self.name is not None:
                channel = f' FOR CHANNEL {self.name}'
            return f'SHOW {self.category} {channel}'

        return f'SHOW{modes_str} {self.category}{from_str}{in_str}{like_str}{where_str}'







