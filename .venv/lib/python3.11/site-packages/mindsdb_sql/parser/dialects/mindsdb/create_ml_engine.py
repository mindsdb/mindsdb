from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateMLEngine(ASTNode):
    def __init__(self,
                 name,
                 handler,
                 params=None,
                 if_not_exists=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.handler = handler
        self.params = params
        self.if_not_exists = if_not_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        param_str = f'{repr(self.params)},'

        out_str = f'{ind}CreateMLEngine(' \
                  f'\n{ind1}if_not_exists={self.if_not_exists}' \
                  f'\n{ind1}name={self.name.to_tree()}' \
                  f'\n{ind1}handler={self.handler}' \
                  f'\n{ind1}using={param_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        using_str = ''
        if self.params is not None:
            using_ar = [f'{k}={repr(v)}' for k, v in self.params.items()]

            using_str = 'USING ' + ', '.join(using_ar)

        out_str = f'CREATE ML_ENGINE {"IF NOT EXISTS" if self.if_not_exists else ""} {self.name.to_string()} FROM {self.handler} {using_str}'

        return out_str.strip()

