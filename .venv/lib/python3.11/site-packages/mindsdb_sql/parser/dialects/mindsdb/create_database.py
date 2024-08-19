import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateDatabase(ASTNode):
    def __init__(self,
                 name,
                 engine,
                 parameters,
                 is_replace=False,
                 if_not_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.engine = engine
        self.parameters = parameters
        self.is_replace = is_replace
        self.if_not_exists = if_not_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_string()},'
        engine_str = f'\n{ind1}engine={repr(self.engine)},'
        parameters_str = f'\n{ind1}parameters={str(self.parameters)},'

        replace_str = ''
        if self.is_replace:
            replace_str = f'\n{ind1}is_replace=True'

        out_str = f'{ind}CreateDatabase(' \
                  f'\n{ind1}if_not_exists={self.if_not_exists},' \
                  f'{name_str}' \
                  f'{engine_str}' \
                  f'{parameters_str}' \
                  f'{replace_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        replace_str = ''
        if self.is_replace:
            replace_str = f' OR REPLACE'

        engine_str = ''
        if self.engine:
            engine_str = f'ENGINE = {repr(self.engine)} '

        parameters_str = ''
        if self.parameters:
            parameters_str = f', PARAMETERS = {json.dumps(self.parameters)}'
        out_str = f'CREATE{replace_str} DATABASE {"IF NOT EXISTS " if self.if_not_exists else ""}{self.name.to_string()} {engine_str}{parameters_str}'
        return out_str
