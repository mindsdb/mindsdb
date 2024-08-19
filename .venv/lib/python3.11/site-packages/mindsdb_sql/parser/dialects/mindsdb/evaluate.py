from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.base import ASTNode


class Evaluate(ASTNode):
    def __init__(self,
                 name,
                 query_str,
                 using=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.using = using
        self.query_str = query_str
        self.data = None  # filled-in by mindsdb, as parse_sql cannot be used at init time due to circular imports

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        name_str = f'\n{ind1}name={self.name.to_string()},'

        query_str = f'\n{ind1}query_str={repr(self.query_str)},'

        out_str = f'{ind}Evaluate(' \
                  f'{name_str}' \
                  f'{query_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        inner_query_str = self.query_str
        out_str = f'EVALUATE {self.name.to_string()} from ({inner_query_str})'
        if self.using is not None:
            using_str = ", ".join([f"{k}={v}" for k, v in self.using.items()])
            out_str = f'{out_str} USING {using_str}'
        out_str += ';'
        return out_str
