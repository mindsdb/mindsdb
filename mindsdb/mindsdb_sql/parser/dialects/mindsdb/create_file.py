from urllib.parse import urlparse

from mindsdb.mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.mindsdb_sql.exceptions import ParsingException
from mindsdb.mindsdb_sql.parser.utils import indent


class CreateFile(ASTNode):
    def __init__(self, name, url, *args, **kwargs):
        super().__init__(*args, **kwargs)

        url_ = urlparse(url)
        if url_.scheme not in ('http', 'https'):
            raise ParsingException(f'Wrong url scheme: {url_.scheme}')
        self.url = url
        self.name = name

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        out_str = f'{ind}CreateFile(\n' \
                  f'{ind1}name={self.name.to_string()}\n' \
                  f'{ind1}url={self.url}\n' \
                  f'{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        return f"CREATE TABLE {str(self.name)} USING url='{self.url}'"
