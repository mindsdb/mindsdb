from mindsdb.mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.mindsdb_sql.parser.ast import Tuple
from mindsdb.mindsdb_sql.parser.utils import indent


class Set(ASTNode):
    def __init__(self,
                 category=None,
                 arg=None,
                 params=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category = category
        self.arg = arg
        self.params = params or {}

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        category_str = f'category={self.category}, '
        arg_str = f'arg={self.arg.to_tree()},' if self.arg else ''
        if self.params:
            param_str = 'param=' + ', '.join([f'{k}:{v}' for k,v in self.params.items()])
        else:
            param_str = ''
        out_str = f'{ind}Set(' \
                  f'{category_str}' \
                  f'{arg_str} ' \
                  f'{param_str}' \
                  f')'
        return out_str

    def get_string(self, *args, **kwargs):
        if self.params:
            param_str = ' ' + ' '.join([f'{k} {v}' for k, v in self.params.items()])
        else:
            param_str = ''

        if isinstance(self.arg, Tuple):
            arg_str = ', '.join([str(i) for i in self.arg.items])
        else:
            arg_str = f' {str(self.arg)}' if self.arg else ''
        return f'SET {self.category if self.category else ""}{arg_str}{param_str}'


class SetTransaction(ASTNode):
    def __init__(self,
                 isolation_level=None,
                 access_mode=None,
                 scope=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isolation_level is not None:
            isolation_level = isolation_level.upper()
        if access_mode is not None:
            access_mode = access_mode.upper()
        if scope is not None:
            scope = scope.upper()

        self.scope = scope
        self.access_mode = access_mode
        self.isolation_level = isolation_level

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        if self.scope is None:
            scope_str = ''
        else:
            scope_str = f'scope={self.scope}, '

        properties = []
        if self.isolation_level is not None:
            properties.append('ISOLATION LEVEL ' + self.isolation_level)
        if self.access_mode is not None:
            properties.append(self.access_mode)
        prop_str = ', '.join(properties)

        out_str = f'{ind}SetTransaction(' \
                  f'{scope_str}' \
                  f'properties=[{prop_str}]' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        properties = []
        if self.isolation_level is not None:
            properties.append('ISOLATION LEVEL ' + self.isolation_level)
        if self.access_mode is not None:
            properties.append(self.access_mode)

        prop_str = ', '.join(properties)

        if self.scope is None:
            scope_str = ''
        else:
            scope_str = self.scope + ' '

        return f'SET {scope_str}TRANSACTION {prop_str}'

