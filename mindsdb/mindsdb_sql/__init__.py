import re

from mindsdb.mindsdb_sql.exceptions import ParsingException
from mindsdb.mindsdb_sql.parser.ast import *


def get_lexer_parser(dialect):
    if dialect == 'sqlite':
        from mindsdb.mindsdb_sql.parser.lexer import SQLLexer
        from mindsdb.mindsdb_sql.parser.parser import SQLParser
        lexer, parser = SQLLexer(), SQLParser()
    elif dialect == 'mysql':
        from mindsdb.mindsdb_sql.parser.dialects.mysql.lexer import MySQLLexer
        from mindsdb.mindsdb_sql.parser.dialects.mysql.parser import MySQLParser
        lexer, parser = MySQLLexer(), MySQLParser()
    elif dialect == 'mindsdb':
        from mindsdb.mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer
        from mindsdb.mindsdb_sql.parser.dialects.mindsdb.parser import MindsDBParser
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
    return ast
