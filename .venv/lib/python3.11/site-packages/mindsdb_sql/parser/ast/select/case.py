from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.utils import indent


class Case(ASTNode):
    def __init__(self, rules, default, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # structure:
        # [
        #   [ condition, result ]
        # ]
        self.rules = rules
        self.default = default

    def get_value(self, record):
        # TODO get value from record using "case" conditions
        ...

    def assert_arguments(self):
        pass

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        # rules
        rules_ar = []
        for condition, result in self.rules:
            rules_ar.append(
                f'{ind1}{condition.to_string()} => {result.to_string()}'
            )
        rules_str = '\n'.join(rules_ar)

        return f'{ind}Case(\n' \
               f'{rules_str}\n' \
               f'{ind1}default => {self.default.to_string()}\n' \
               f'{ind})'

    def get_string(self, *args, alias=True, **kwargs):
        # rules
        rules_ar = []
        for condition, result in self.rules:
            rules_ar.append(
                f'WHEN {condition.to_string()} THEN {result.to_string()}'
            )
        rules_str = ' '.join(rules_ar)

        return f"CASE {rules_str} ELSE {self.default.to_string()} END"
