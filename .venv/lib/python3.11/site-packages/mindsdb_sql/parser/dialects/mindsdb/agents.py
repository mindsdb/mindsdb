from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateAgent(ASTNode):
    """
    Node for creating a new agent
    """

    def __init__(self, name, model, params, if_not_exists=False, *args, **kwargs):
        """
        Parameters:
            name (Identifier): name of the agent to create
            model (str): name of the underlying model to use with the agent
            params (dict): USING parameters to create the agent with
            if_not_exists (bool): if True, do not raise an error if the agent exists 
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.model = model
        self.params = params
        self.if_not_exists = if_not_exists

    def to_tree(self, level=0, *args, **kwargs):
        ind = indent(level)
        out_str = f'{ind}CreateAgent(' \
                  f'if_not_exists={self.if_not_exists}' \
                  f'name={self.name.to_string()}, ' \
                  f'model={self.model}, ' \
                  f'params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):
        using_ar = [f'model={repr(self.model)}']
        using_ar += [f'{k}={repr(v)}' for k, v in self.params.items()]
        using_str = ', '.join(using_ar)

        out_str = f'CREATE AGENT {"IF NOT EXISTS " if self.if_not_exists else ""}{self.name.to_string()} USING {using_str}'
        return out_str


class UpdateAgent(ASTNode):
    """
    Node for updating an agent
    """

    def __init__(self, name, updated_params, *args, **kwargs):
        """
        Parameters:
            name (Identifier): name of the agent to update
            updated_params (dict): new SET parameters of the agent to update
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.params = updated_params

    def to_tree(self, level=0, *args, **kwargs):
        ind = indent(level)
        out_str = f'{ind}UpdateAgent(' \
                  f'name={self.name.to_string()}, ' \
                  f'updated_params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):
        set_ar = [f'{k}={repr(v)}' for k, v in self.params.items()]
        set_str = ', '.join(set_ar)

        out_str = f'UPDATE AGENT {self.name.to_string()} SET {set_str}'
        return out_str


class DropAgent(ASTNode):
    """
    Node for dropping an agent
    """

    def __init__(self, name, if_exists=False, *args, **kwargs):
        """
        Parameters:
            name (Identifier): name of the agent to drop
            if_exists (bool): if True, do not raise an error if the agent does not exist
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, level=0, *args, **kwargs):
        ind = indent(level)
        out_str = f'{ind}DropAgent(if_exists={self.if_exists}, name={self.name.to_string()})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP AGENT {"IF EXISTS " if self.if_exists else ""}{str(self.name.to_string())}'
        return out_str
