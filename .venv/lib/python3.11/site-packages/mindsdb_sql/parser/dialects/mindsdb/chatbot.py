import json
import datetime as dt

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateChatBot(ASTNode):
    def __init__(self,
                 name,
                 database,
                 model,
                 agent,
                 params=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.database=database
        self.model = model
        self.agent = agent
        if params is None:
            params = {}
        self.params = params

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        model_str = self.model.to_string() if self.model else 'NULL'
        agent_str = self.agent.to_string() if self.agent else 'NULL'
        out_str = f'{ind}CreateChatBot(' \
                  f'name={self.name.to_string()}, ' \
                  f'database={self.database.to_string()}, ' \
                  f'model={model_str}, ' \
                  f'agent={agent_str}, ' \
                  f'params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):

        params = self.params.copy()
        params['model'] = self.model.to_string() if self.model else 'NULL'
        params['database'] = self.database.to_string()
        if self.agent:
            params['agent'] = self.agent.to_string()

        using_ar = [f'{k}={repr(v)}' for k, v in params.items()]

        using_str = ', '.join(using_ar)

        out_str = f'CREATE CHATBOT {self.name.to_string()} USING {using_str}'
        return out_str


class UpdateChatBot(ASTNode):
    def __init__(self, name, updated_params, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.params = updated_params

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = f'{ind}UpdateChatBot(' \
                  f'name={self.name.to_string()}, ' \
                  f'updated_params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):
        params = self.params.copy()

        set_ar = [f'{k}={repr(v)}' for k, v in params.items()]
        set_str = ', '.join(set_ar)

        out_str = f'UPDATE CHATBOT {self.name.to_string()} SET {set_str}'
        return out_str


class DropChatBot(ASTNode):
    def __init__(self,
                 name,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = f'{ind}DropChatBot(name={self.name.to_string()})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP CHATBOT {str(self.name.to_string())}'
        return out_str

