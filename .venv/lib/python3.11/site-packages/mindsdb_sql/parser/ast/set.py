from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Tuple
from mindsdb_sql.parser.utils import indent


class Set(ASTNode):
    def __init__(self,
                 category=None,
                 name=None,
                 value=None,
                 scope=None,
                 params=None,
                 set_list=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        # names / charset / transactions
        self.category = category

        # name for variable assigment. category is None it this case
        self.name = name

        self.value = value
        self.params = params or {}

        # global / session / ...
        self.scope = scope

        # contents all set subcommands
        self.set_list = set_list


    def to_tree(self, *args, level=0, **kwargs):
        if self.set_list is not None:
            items = [set.render() for set in self.set_list]
        else:
            items = self.render()

        ind = indent(level)

        return f'{ind}Set(items={items})'

    def get_string(self, *args, **kwargs):
        return 'SET ' + self.render()

    def render(self):
        if self.set_list is not None:
            render_list = [set.render() for set in self.set_list]
            return ', '.join(render_list)

        if self.params:
            params = []
            for k, v in self.params.items():
                if k.lower() == 'access_mode':
                    params.append(v)
                else:
                    params.append(f'{k} {v}')
            param_str = ' ' + ', '.join(params)
        else:
            param_str = ''

        if self.name is not None:
            # category should be empty
            content = f'{self.name.to_string()}={self.value.to_string()}'
        elif self.value is not None:
            content = f'{self.category} {self.value.to_string()}'
        else:
            content = f'{self.category}'

        scope = ''
        if self.scope is not None:
            scope = f'{self.scope} '

        return f'{scope}{content}{param_str}'


# class SetTransaction(ASTNode):
#     def __init__(self,
#                  isolation_level=None,
#                  access_mode=None,
#                  scope=None,
#                  *args, **kwargs):
#         super().__init__(*args, **kwargs)
#
#         if isolation_level is not None:
#             isolation_level = isolation_level.upper()
#         if access_mode is not None:
#             access_mode = access_mode.upper()
#         if scope is not None:
#             scope = scope.upper()
#
#         self.scope = scope
#         self.access_mode = access_mode
#         self.isolation_level = isolation_level
#
#     def to_tree(self, *args, level=0, **kwargs):
#         ind = indent(level)
#         if self.scope is None:
#             scope_str = ''
#         else:
#             scope_str = f'scope={self.scope}, '
#
#         properties = []
#         if self.isolation_level is not None:
#             properties.append('ISOLATION LEVEL ' + self.isolation_level)
#         if self.access_mode is not None:
#             properties.append(self.access_mode)
#         prop_str = ', '.join(properties)
#
#         out_str = f'{ind}SetTransaction(' \
#                   f'{scope_str}' \
#                   f'properties=[{prop_str}]' \
#                   f'\n{ind})'
#         return out_str
#
#     def get_string(self, *args, **kwargs):
#         properties = []
#         if self.isolation_level is not None:
#             properties.append('ISOLATION LEVEL ' + self.isolation_level)
#         if self.access_mode is not None:
#             properties.append(self.access_mode)
#
#         prop_str = ', '.join(properties)
#
#         if self.scope is None:
#             scope_str = ''
#         else:
#             scope_str = self.scope + ' '
#
#         return f'SET {scope_str}TRANSACTION {prop_str}'

