import re
from copy import copy, deepcopy

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.select import Star


no_wrap_identifier_regex = re.compile(r'[a-zA-Z_][a-zA-Z_0-9]*')
path_str_parts_regex = re.compile(r'(?:(?:(`[^`]+`))|([^.]+))')


def path_str_to_parts(path_str: str):
    match = re.finditer(path_str_parts_regex, path_str)
    parts = [x[0].strip('`') for x in match]
    return parts


RESERVED_KEYWORDS = {
    'PERSIST', 'IF', 'EXISTS', 'NULLS', 'FIRST', 'LAST',
    'ORDER', 'BY', 'GROUP', 'PARTITION'
}


def get_reserved_words():
    from mindsdb_sql.parser.lexer import SQLLexer
    from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer

    reserved = RESERVED_KEYWORDS
    for word in SQLLexer.tokens | MindsDBLexer.tokens:
        if '_' not in word:
            # exclude combinations
            reserved.add(word)
    return reserved


class Identifier(ASTNode):
    def __init__(self, path_str=None, parts=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert path_str or parts, "Either path_str or parts must be provided for an Identifier"
        assert not (path_str and parts), "Provide either path_str or parts, but not both"
        if isinstance(path_str, Star) and not parts:
            parts = [Star()]

        if path_str and not parts:
            parts = path_str_to_parts(path_str)
        assert isinstance(parts, list)
        self.parts = parts

    @classmethod
    def from_path_str(self, value, *args, **kwargs):
        parts = path_str_to_parts(value)
        return Identifier(parts=parts, *args, **kwargs)

    def parts_to_str(self):
        out_parts = []
        reserved_words = get_reserved_words()
        for part in self.parts:
            if isinstance(part, Star):
                part = str(part)
            else:
                if (
                    not no_wrap_identifier_regex.fullmatch(part)
                    or
                    part.upper() in reserved_words
                ):
                    part = f'`{part}`'

            out_parts.append(part)
        return '.'.join(out_parts)

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={self.alias.to_tree()}' if self.alias else ''
        return indent(level) + f'Identifier(parts={[str(i) for i in self.parts]}{alias_str})'

    def get_string(self, *args, **kwargs):
        return self.parts_to_str()

    def __copy__(self):
        identifier = Identifier(parts=copy(self.parts))
        identifier.alias = deepcopy(self.alias)
        identifier.parentheses = self.parentheses
        if hasattr(self, 'sub_select'):
            identifier.sub_select = deepcopy(self.sub_select)
        return identifier

    def __deepcopy__(self, memo):
        identifier = Identifier(parts=copy(self.parts))
        identifier.alias = deepcopy(self.alias)
        identifier.parentheses = self.parentheses
        if hasattr(self, 'sub_select'):
            identifier.sub_select = deepcopy(self.sub_select)
        return identifier
