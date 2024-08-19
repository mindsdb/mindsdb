import copy

from mindsdb_sql import ParsingException
from mindsdb_sql.parser.utils import to_single_line


class ASTNode:
    def __init__(self, alias=None, parentheses=False):
        self.alias = alias
        self.parentheses = parentheses

        if self.alias and len(self.alias.parts) > 1:
            raise ParsingException('Alias can not contain multiple parts (dots).')

    def maybe_add_alias(self, some_str, alias=True):
        if self.alias and alias:
            return f'{some_str} AS {self.alias.to_string(alias=False)}'
        else:
            return some_str

    def maybe_add_parentheses(self, some_str):
        if self.parentheses:
            return f'({some_str})'
        else:
            return some_str

    def to_tree(self, *args, **kwargs):
        pass

    def get_string(self):
        pass

    def to_string(self, alias=True):
        return self.maybe_add_alias(self.maybe_add_parentheses(self.get_string()), alias=alias)

    def copy(self):
        return copy.deepcopy(self)

    def __str__(self):
        return self.to_string()

    def __eq__(self, other):
        if isinstance(other, ASTNode):
            return self.to_tree() == other.to_tree() and to_single_line(str(self)) == to_single_line(str(other))
        else:
            return False

    def __repr__(self):
        sql = self.to_string().replace('\n', ' ')
        if len(sql) > 500:
            sql = sql[:500] + '...'
        return f'{self.__class__.__name__}:<{sql}>'
