from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CommonTableExpression(ASTNode):
    def __init__(self, name, query, columns=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.columns = columns or []
        self.query = query

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        name_str = f'\n{ind1}name={self.name.to_tree()}'
        columns_str = f'\n{ind1}columns=[{", ".join([c.to_tree() for c in self.columns])}]'
        query_str = f'\n{ind1}query=\n{self.query.to_tree(level=level + 2)}'

        out_str = f'{ind}Join({name_str},{columns_str},{query_str}\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        name_str = self.name.to_string(alias=False)
        columns_str = '' if not self.columns else f'( {", ".join([c.to_string(alias=False) for c in self.columns])} )'
        query_str = self.query.to_string()
        return f'{name_str}{columns_str} AS ( {query_str} )'
