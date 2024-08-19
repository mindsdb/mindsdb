from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class NativeQuery(ASTNode):
    """
        Not parsed query to integration
    """

    def __init__(self, integration, query: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.integration = integration
        self.query = query

    def to_tree(self, *args, level=0, **kwargs):
        return indent(level) + \
               f'NativeQuery(integration={self.integration.to_string()}, query="{self.query}")'

    def get_string(self, *args, **kwargs):
        # standard native query render is used in create view
        return f'{self.integration.to_string()} ({self.query})'

    def __repr__(self):
        return f'{self.__class__.__name__}:{self.integration.to_string()} ({self.query})'
