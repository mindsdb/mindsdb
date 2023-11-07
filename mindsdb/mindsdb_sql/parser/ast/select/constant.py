import datetime as dt
from mindsdb.mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.mindsdb_sql.parser.utils import indent


class Constant(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={self.alias.to_tree()}' if self.alias else ''
        return indent(level) + f'Constant(value={repr(self.value)}{alias_str})'

    def get_string(self, *args, **kwargs):
        if isinstance(self.value, str):
            out_str = f"\'{self.value}\'"
        elif isinstance(self.value, bool):
            out_str = 'TRUE' if self.value else 'FALSE'
        elif isinstance(self.value, (dt.date, dt.datetime, dt.timedelta)):
            out_str = "'{}'".format(str(self.value).replace("'", "''"))
        else:
            out_str = str(self.value)
        return out_str


class NullConstant(Constant):
    def __init__(self, *args, **kwargs):
        super().__init__(value=None, *args, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        return '\t'*level +  'NullConstant()'

    def get_string(self, *args, **kwargs):
        return 'NULL'


# TODO replace it to just Constant?
# DEFAULT
class SpecialConstant(ASTNode):
    def __init__(self, name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def to_tree(self, *args, level=0, **kwargs):
        return indent(level) + f'SpecialConstant(name={self.name})'

    def get_string(self, *args, **kwargs):
        return self.name


class Last(Constant):
    def __init__(self, *args, **kwargs):
        self.value = 'last'
        super().__init__(self.value)

    def to_tree(self, *args, level=0, **kwargs):
        return indent(level) + f'{self.__class__.__name__}()'

    def get_string(self, *args, **kwargs):
        return self.value