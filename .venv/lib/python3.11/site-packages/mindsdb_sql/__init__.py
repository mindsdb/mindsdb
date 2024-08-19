import re
from collections import defaultdict

from sly.lex import Token

from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.ast import *


class ErrorHandling:

    def __init__(self, lexer, parser):
        self.parser = parser
        self.lexer = lexer

    def process(self, error_info):
        self.tokens = [t for t in error_info['tokens'] if t is not None]
        self.bad_token = error_info['bad_token']
        self.expected_tokens = error_info['expected_tokens']

        if len(self.tokens) == 0:
            return 'Empty input'

        # show error location
        msgs = self.error_location()

        # suggestion
        suggestions = self.make_suggestion()

        if suggestions:
            prefix = 'Possible inputs: ' if len(suggestions) > 1 else 'Expected symbol: '
            msgs.append(prefix + ', '.join([f'"{item}"' for item in suggestions]))
        return '\n'.join(msgs)

    def error_location(self):

        # restore query text
        lines_idx = defaultdict(str)

        # used + unused tokens
        for token in self.tokens:
            if token is None:
                continue
            line = lines_idx[token.lineno]

            if len(line) > token.index:
                line = line[: token.index]
            else:
                line = line.ljust(token.index)

            line += token.value
            lines_idx[token.lineno] = line

        msgs = []

        # error message and location
        if self.bad_token is None:
            msgs.append('Syntax error, unexpected end of query:')
            error_len = 1
            # last line
            error_line_num = list(lines_idx.keys())[-1]
            error_index = len(lines_idx[error_line_num])
        else:
            msgs.append('Syntax error, unknown input:')
            error_len = len(self.bad_token.value)
            error_line_num = self.bad_token.lineno
            error_index = self.bad_token.index

        # shift lines indexes (it removes spaces from beginnings of the lines)
        lines = []
        shift = 0
        error_line = 0
        for i, line_num in enumerate(lines_idx.keys()):
            if line_num == error_line_num:
                error_index -= shift
                error_line = i

            line = lines_idx[line_num]
            lines.append(line[shift:])
            shift = len(line)

        # add source code
        first_line = error_line - 2 if error_line > 1 else 0
        for line in lines[first_line: error_line + 1]:
            msgs.append('>' + line)

        # error position
        msgs.append('-' * (error_index + 1) + '^' * error_len)
        return msgs

    def make_suggestion(self):
        if len(self.expected_tokens) == 0:
            return []

        # find error index
        error_index = None
        for i, token in enumerate(self.tokens):
            if token is self.bad_token :
                error_index = i

        expected = {}  # value: token

        for token_name in self.expected_tokens:
            value = getattr(self.lexer, token_name, None)
            if token_name == 'ID':
                # a lot of other tokens could be ID
                expected = {'[identifier]': token_name}
                break
            elif token_name in ('FLOAT', 'INTEGER'):
                expected['[number]'] = token_name

            elif token_name in ('DQUOTE_STRING', 'QUOTE_STRING'):
                expected['[string]'] = token_name

            elif isinstance(value, str):
                value = value.replace('\\b', '').replace('\\', '')

                # doesn't content regexp
                if '\\s' not in value and '|' not in value:
                    expected[value] = token_name

        suggestions = []
        if len(expected) == 1:
            # use only it
            first_value = list(expected.keys())[0]
            suggestions.append(first_value)

        elif 1 < len(expected) < 20:
            if self.bad_token is None:
                # if this is the end of query, just show next expected keywords
                return list(expected.keys())

            # not every suggestion satisfy the end of the query. we have to check if it works
            for value, token_name in expected.items():
                # make up a token
                token = Token()
                token.type = token_name
                token.value = value
                token.end = 0
                token.index = 0
                token.lineno = 0

                # try to add token
                tokens2 = self.tokens[:error_index] + [token] + self.tokens[error_index:]
                if self.query_is_valid(tokens2):
                    suggestions.append(value)
                    continue

                # try to replace token
                tokens2 = self.tokens[:error_index - 1] + [token] + self.tokens[error_index:]
                if self.query_is_valid(tokens2):
                    suggestions.append(value)
                    continue

        return suggestions

    def query_is_valid(self, tokens):
        # try to parse list of tokens

        ast = self.parser.parse(iter(tokens))
        return ast is not None


def get_lexer_parser(dialect):
    if dialect == 'sqlite':
        from mindsdb_sql.parser.lexer import SQLLexer
        from mindsdb_sql.parser.parser import SQLParser
        lexer, parser = SQLLexer(), SQLParser()
    elif dialect == 'mysql':
        from mindsdb_sql.parser.dialects.mysql.lexer import MySQLLexer
        from mindsdb_sql.parser.dialects.mysql.parser import MySQLParser
        lexer, parser = MySQLLexer(), MySQLParser()
    elif dialect == 'mindsdb':
        from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer
        from mindsdb_sql.parser.dialects.mindsdb.parser import MindsDBParser
        lexer, parser = MindsDBLexer(), MindsDBParser()
    else:
        raise ParsingException(f'Unknown dialect {dialect}. Available options are: sqlite, mysql.')
    return lexer, parser


def parse_sql(sql, dialect='mindsdb'):
    # remove ending semicolon and spaces
    sql = re.sub(r'[\s;]+$', '', sql)

    lexer, parser = get_lexer_parser(dialect)
    tokens = lexer.tokenize(sql)
    ast = parser.parse(tokens)

    if ast is None:

        eh = ErrorHandling(lexer, parser)
        message = eh.process(parser.error_info)

        raise ParsingException(message)

    return ast
