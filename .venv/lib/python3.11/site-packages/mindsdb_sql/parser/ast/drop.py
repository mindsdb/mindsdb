from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Drop(ASTNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        pass

    def get_string(self, *args, **kwargs):
        pass


class DropTables(Drop):

    def __init__(self,
                 tables,
                 if_exists=False,
                 only_temporary=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tables = tables
        self.if_exists = if_exists
        self.only_temporary = only_temporary

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)

        tables_str = ', '.join([i.to_tree() for i in self.tables])

        out_str = f'{ind}DropTables(' \
                  f'[{tables_str}], ' \
                  f'if_exists={self.if_exists}, ' \
                  f'only_temporary={self.only_temporary}' \
                  f')'
        return out_str

    def get_string(self, *args, **kwargs):
        temporary_str = f'TEMPORARY' if self.only_temporary else ''
        exists_str = f'IF EXISTS' if self.if_exists else ''
        tables_str = ', '.join([i.to_string() for i in self.tables])

        return f'DROP {temporary_str} TABLE {exists_str} {tables_str}'


class DropDatabase(Drop):

    def __init__(self,
                 name,
                 if_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        name_str = f'name={self.name.to_tree()}'

        out_str = f'{ind}DropDatabase(' \
                  f'{name_str}, ' \
                  f'if_exists={self.if_exists}' \
                  f')'
        return out_str

    def get_string(self, *args, **kwargs):
        exists_str = f'IF EXISTS ' if self.if_exists else ''

        return f'DROP DATABASE {exists_str}{self.name}'


class DropView(Drop):

    def __init__(self,
                 names,
                 if_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        # DROP VIEW removes one or more views.
        # https://dev.mysql.com/doc/refman/8.0/en/drop-view.html

        self.names = names
        self.if_exists = if_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        names_str = ', '.join([i.to_tree() for i in self.names])

        out_str = f'{ind}DropView(' \
                  f'[{names_str}], ' \
                  f'if_exists={self.if_exists}' \
                  f')'
        return out_str

    def get_string(self, *args, **kwargs):
        exists_str = f'IF EXISTS ' if self.if_exists else ''
        names_str = ', '.join(map(str, self.names))

        return f'DROP VIEW {exists_str}{names_str}'




