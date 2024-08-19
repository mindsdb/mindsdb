from mindsdb_sql.parser.lexer import SQLLexer


class MySQLLexer(SQLLexer):
    tokens = SQLLexer.tokens.union({VARIABLE, SYSTEM_VARIABLE})

    @_(r'@[a-zA-Z_.$]+',
       r"@'[a-zA-Z_.$][^']*'",
       r"@`[a-zA-Z_.$][^`]*`",
       r'@"[a-zA-Z_.$][^"]*"'
       )
    def VARIABLE(self, t):
        t.value = t.value.lstrip('@')

        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        elif t.value[0] == "'":
            t.value = t.value.strip('\'')
        elif t.value[0] == "`":
            t.value = t.value.strip('`')
        return t

    @_(r'@@[a-zA-Z_.$]+',
       r"@@'[a-zA-Z_.$][^']*'",
       r"@@`[a-zA-Z_.$][^`]*`",
       r'@@"[a-zA-Z_.$][^"]*"'
       )
    def SYSTEM_VARIABLE(self, t):
        t.value = t.value.lstrip('@')

        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        elif t.value[0] == "'":
            t.value = t.value.strip('\'')
        elif t.value[0] == "`":
            t.value = t.value.strip('`')
        return t
