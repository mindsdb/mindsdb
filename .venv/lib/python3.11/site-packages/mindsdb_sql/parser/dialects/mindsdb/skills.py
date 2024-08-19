from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateSkill(ASTNode):
    """
    Node for creating a new skill
    """

    def __init__(self, name, type, params, if_not_exists=False, *args, **kwargs):
        """
        Parameters:
            name (Identifier): name of the skill to create
            type (str): type of the skill to create
            params (dict): USING parameters to create the skill with
            if_not_exists (bool): if True, do not raise an error if the skill exists 
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.type = type
        self.params = params
        self.if_not_exists = if_not_exists

    def to_tree(self, level=0, *args, **kwargs):
        ind = indent(level)
        out_str = f'{ind}CreateSkill(' \
                  f'if_not_exists={self.if_not_exists}' \
                  f'name={self.name.to_string()}, ' \
                  f'type={self.type}, ' \
                  f'params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):
        using_ar = [f'type={repr(self.type)}']
        using_ar += [f'{k}={repr(v)}' for k, v in self.params.items()]
        using_str = ', '.join(using_ar)

        out_str = f'CREATE SKILL {"IF NOT EXISTS " if self.if_not_exists else ""}{self.name.to_string()} USING {using_str}'
        return out_str


class UpdateSkill(ASTNode):
    """
    Node for updating a skill
    """

    def __init__(self, name, updated_params, *args, **kwargs):
        """
        Parameters:
            name (Identifier): name of the skill to update
            updated_params (dict): new SET parameters of the skill to update
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.params = updated_params

    def to_tree(self, level=0, *args, **kwargs):
        ind = indent(level)
        out_str = f'{ind}UpdateSkill(' \
                  f'name={self.name.to_string()}, ' \
                  f'updated_params={self.params})'
        return out_str

    def get_string(self, *args, **kwargs):
        set_ar = [f'{k}={repr(v)}' for k, v in self.params.items()]
        set_str = ', '.join(set_ar)

        out_str = f'UPDATE SKILL {self.name.to_string()} SET {set_str}'
        return out_str


class DropSkill(ASTNode):
    """
    Node for dropping a skill
    """

    def __init__(self, name, if_exists=False, *args, **kwargs):
        """
        Parameters:
            name (Identifier): name of the skill to drop
            if_exists (bool): if True, do not raise an error if the skill does not exist
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, level=0, *args, **kwargs):
        ind = indent(level)
        out_str = f'{ind}DropSkill(if_exists={self.if_exists}, name={self.name.to_string()})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP SKILL {"IF EXISTS " if self.if_exists else ""}{str(self.name.to_string())}'
        return out_str
