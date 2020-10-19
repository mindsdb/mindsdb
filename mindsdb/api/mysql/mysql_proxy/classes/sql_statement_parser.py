from pyparsing import (
    ParserElement,
    nestedExpr,
    ZeroOrMore,
    StringEnd,
    Word,
    alphas,
    printables,
    Literal,
    QuotedString,
    quotedString,
    originalTextFor,
    OneOrMore,
    delimitedList,
    Optional,
    Suppress,
    CaselessKeyword,
    ParseException,
    SkipTo
)
import re


RE_INT = re.compile(r'^[-+]?([1-9]\d*|0)$')
RE_FLOAT = re.compile(r'^[-+]?([1-9]\d*\.\d*|0\.|0\.\d*)$')


class SqlStatementParseError(Exception):
    pass


class SQLParameter:
    pass


SQL_PARAMETER = SQLParameter()


class SQLDefault:
    pass


SQL_DEFAULT = SQLDefault()


class SqlStatementParser():
    """Parser for initial analysis of sql statements.
    Example of usage:

        sql = "insert into a.b (col1) values ('val1')"
        statement = SqlStatementParser(sql)
        print(statement.keyword)    # insert
        print(statement.struct)     # {'database': 'a', 'table': 'b', 'columns': ['col1'], 'values': ['val1']}
    """
    _original_sql: str = None
    _sql: str = None
    _struct: str = None

    def __init__(self, text, init_parse=True):
        self._original_sql = text
        self._sql = SqlStatementParser.clear_sql(text)
        self._keyword = SqlStatementParser.get_keyword(self._sql)
        if init_parse:
            if self._keyword == 'insert':
                self._struct = self.parse_as_insert()
            elif self._keyword == 'delete':
                self._struct = self.parse_as_delete()

    @property
    def keyword(self):
        return self._keyword

    @property
    def sql(self):
        return self._sql

    @property
    def struct(self):
        return self._struct

    @staticmethod
    def clear_sql(sql: str) -> str:
        ParserElement.defaultWhitespaceChars = (" \t")
        comment = nestedExpr('/*', '*/').suppress()
        starting = ZeroOrMore(comment.suppress())
        ending = ZeroOrMore(comment | ';').suppress() + StringEnd()
        expr = starting + SkipTo(ending) + ending
        r = expr.parseString(sql)

        if len(r) != 1:
            raise Exception('Error while parsing expr')

        return r[0]

    @staticmethod
    def get_keyword(sql):
        ''' Return keyword of sql statement. Should be one of:

            start (transaction)
            set (autocommit, names etc)
            use
            show
            delete
            insert
            update
            alter
            select
            rollback
            commit
            explain
        '''
        key_word = Word(alphas)
        other_words = Word(printables)
        expr = key_word + Optional(other_words).suppress()

        r = expr.parseString(sql)

        if len(r) != 1:
            raise Exception('Cant get keyword from statement')

        word = r[0].lower()

        return word

    @staticmethod
    def is_quoted_str(text):
        if isinstance(text, str) is False:
            return False
        for quote in ['"', "'", '`']:
            if text.startswith(quote) and text.endswith(quote):
                return True
        return False

    @staticmethod
    def is_int_str(text):
        if isinstance(text, str) and RE_INT.match(text):
            return True
        return False

    @staticmethod
    def is_float_str(text):
        if isinstance(text, str) and RE_FLOAT.match(text):
            return True
        return False

    @staticmethod
    def unquote(text):
        for quote in ['"', "'", '`']:
            if text.startswith(quote) and text.endswith(quote):
                return text[1:-1]
        return text

    def ends_with(self, text):
        ''' Check if sql ends with 'text'. Not case sensitive.
        '''
        test_sql = ' '.join(self._sql.split()).lower()
        return test_sql.endswith(text.lower())

    def cut_from_tail(self, text):
        ''' Removes 'text' from end of sql. Not case sensitive.
        '''
        text_arr = text.split(' ')

        ending = CaselessKeyword(text_arr[0])
        for x in text_arr[1:]:
            ending = ending + CaselessKeyword(x)
        ending = ending + StringEnd()

        expr = (originalTextFor(SkipTo(ending)))('original') + (originalTextFor(ending))('ending')

        try:
            r = expr.parseString(self._sql)
        except ParseException:
            return False

        self._sql = r.asDict()['original'].strip()
        return True

    def parse_as_delete(self) -> dict:
        ''' Parse delete. Example: 'delete from database.table where column_a= 1 and column_b = 2;'
        '''

        result = {
            'database': None,
            'table': None,
            'where': {}
        }

        suppressed_word = Word(alphas).suppress()
        and_ = Literal("and")

        from_value = (
                QuotedString('`')
                | originalTextFor(
                    Word(printables, excludeChars='.`')
                )
        )

        expr = (
                suppressed_word + suppressed_word
                + (delimitedList(from_value, delim='.'))('db_table')
                + Optional(
                    Word("where").suppress()
                    + OneOrMore(
                        Word(printables).setResultsName('columns', listAllMatches=True)
                        + Word('=').suppress()
                        + Word(printables).setResultsName('values', listAllMatches=True)
                        + Optional(and_).suppress()
                    )
                )

        )

        r = expr.parseString(self._sql).asDict()

        if len(r['db_table']) == 2:
            result['database'] = r['db_table'][0]
            result['table'] = r['db_table'][1]
        else:
            result['table'] = r['db_table'][0]

        if 'columns' in r and 'values' in r:
            if not isinstance(r['columns'], list) \
                    and not isinstance(r['values'], list):
                r['columns'] = [r['columns']]
                r['values'] = [r['values']]
            if len(r['columns']) != len(r['values']):
                raise SqlStatementParseError(f"Columns and values have different amounts")

            for i, val in enumerate(r['values']):
                if isinstance(val, str) and val.lower() == 'null':
                    result['where'][r['columns'][i]] = None
                elif val == '?':
                    result['where'][r['columns'][i]] = SQL_PARAMETER
                elif isinstance(val, str) and val.lower() == 'default':
                    result['where'][r['columns'][i]] = SQL_DEFAULT
                elif SqlStatementParser.is_int_str(val):
                    result['where'][r['columns'][i]] = int(val)
                elif SqlStatementParser.is_float_str(val):
                    result['where'][r['columns'][i]] = float(val)
                elif SqlStatementParser.is_quoted_str(val):
                    result['where'][r['columns'][i]] = SqlStatementParser.unquote(val)
                elif isinstance(val, str):
                    # it should be in one case, only if server send function as argument, for example:
                    # insert into table (datetime) values (now())
                    raise Exception(f"Error: cant determine type of '{val}'")

        for key, value in result['where'].items():
            if SqlStatementParser.is_quoted_str(value):
                result['where'][key] = SqlStatementParser.unquote(value)

        return result

    def parse_as_insert(self) -> dict:
        ''' Parse insert. Example: 'insert into database.table (columns) values (values)'
        '''

        text = self._sql

        result = {
            'database': None,
            'table': None,
            'columns': [],
            'values': []
        }

        word = Word(alphas)

        from_value = (
            QuotedString('`')
            | originalTextFor(
                Word(printables, excludeChars='.`')
            )
        )

        list_value = (
            quotedString
            | originalTextFor(
                OneOrMore(
                    Word(printables, excludeChars="(),")
                    | nestedExpr()
                )
            )
        )

        expr = (
            word.suppress() + word.suppress()
            + (delimitedList(from_value, delim='.'))('db_table')
            + (Optional(originalTextFor(nestedExpr())))('columns')
            + word.suppress()
            + (originalTextFor(nestedExpr()))('values')
        )

        r = expr.parseString(text).asDict()

        if len(r['db_table']) == 2:
            result['database'] = r['db_table'][0]
            result['table'] = r['db_table'][1]
        else:
            result['table'] = r['db_table'][0]

        LPAR, RPAR = map(Suppress, "()")
        parenthesis_list_expr = LPAR + delimitedList(list_value) + RPAR

        if 'columns' in r:
            columns = r['columns']
            if isinstance(columns, list):
                if len(columns) != 1:
                    raise Exception(f"Error when parse columns list: {columns}")
                columns_str = columns[0]
            else:
                columns_str = columns
            columns = parenthesis_list_expr.parseString(columns_str)
            result['columns'] = columns.asList()

        values = r['values']
        if isinstance(values, list):
            if len(values) != 1:
                raise Exception(f"Error when parse values list: {values}")
            values_str = values[0]
        else:
            values_str = values
        values = parenthesis_list_expr.parseString(values_str)
        result['values'] = values.asList()

        for i, val in enumerate(result['values']):
            if isinstance(val, str) and val.lower() == 'null':
                result['values'][i] = None
            elif val == '?':
                result['values'][i] = SQL_PARAMETER
            elif isinstance(val, str) and val.lower() == 'default':
                result['values'][i] = SQL_DEFAULT
            elif SqlStatementParser.is_int_str(val):
                result['values'][i] = int(val)
            elif SqlStatementParser.is_float_str(val):
                result['values'][i] = float(val)
            elif SqlStatementParser.is_quoted_str(val):
                result['values'][i] = SqlStatementParser.unquote(val)
            elif isinstance(val, str):
                # it should be in one case, only if server send function as argument, for example:
                # insert into table (datetime) values (now())
                raise Exception(f"Error: cant determine type of '{val}'")

        for i, val in enumerate(result['columns']):
            if SqlStatementParser.is_quoted_str(val):
                result['columns'][i] = SqlStatementParser.unquote(val)

        return result

    @staticmethod
    def test():
        tests = [
            [
                'start transaction',
                {'keyword': 'start'}
            ], [
                ' START transaction',
                {'keyword': 'start'}
            ], [
                "insert into a.b (col1, col2) values ('val1', 'val2');",
                {
                    'keyword': 'insert',
                    'struct': {
                        'database': 'a',
                        'table': 'b',
                        'columns': ['col1', 'col2'],
                        'values': ['val1', 'val2']
                    }
                }
            ], [
                "insert into a values (1, 1.1, 'a A', '()', '?', ?);",
                {
                    'keyword': 'insert',
                    'struct': {
                        'database': None,
                        'table': 'a',
                        'columns': [],
                        'values': [1, 1.1, 'a A', '()', '?', SQL_PARAMETER]
                    }
                }
            ], [
                "insert into `a a`.`B B` (col1) values (1);",
                {
                    'keyword': 'insert',
                    'struct': {
                        'database': 'a a',
                        'table': 'B B',
                        'columns': ['col1'],
                        'values': [1]
                    }
                }
            ], [
                "delete from database_a.table_a where column_a = 1",
                {
                    'keyword': 'delete',
                    'struct': {
                        'database': 'database_a',
                        'table': 'table_a',
                        'where': {
                            'column_a': 1
                        }
                    }
                }
            ], [
                "delete from table_a where column_a = 1 and column_b = ?;",
                {
                    'keyword': 'delete',
                    'struct': {
                        'database': None,
                        'table': 'table_a',
                        'where': {
                            'column_a': 1,
                            'column_b': SQL_PARAMETER
                        }
                    }
                }
            ], [
                "delete from database_c.table_a where column_a = ? and column_b = ?;",
                {
                    'keyword': 'delete',
                    'struct': {
                        'database': 'database_c',
                        'table': 'table_a',
                        'where': {
                            'column_a': SQL_PARAMETER,
                            'column_b': SQL_PARAMETER
                        }
                    }
                }
            ]
        ]
        for test in tests:
            sql = test[0]
            print(sql)
            checks = test[1]
            statement = SqlStatementParser(sql)
            if 'keyword' in checks:
                assert(statement.keyword == checks['keyword'])
            if 'struct' in checks:
                struct_check = checks['struct']
                if 'database' in struct_check:
                    assert(struct_check['database'] == statement.struct['database'])
                if 'table' in struct_check:
                    assert(struct_check['table'] == statement.struct['table'])
                if 'where' in struct_check:
                    for idx, key in enumerate(struct_check['where'].keys()):
                        assert(struct_check['where'][key] == statement.struct['where'][key])
                for key in ['columns', 'values']:
                    if key in struct_check:
                        assert(len(struct_check[key]) == len(statement.struct[key]))
                        for i, el in enumerate(struct_check[key]):
                            assert(el == statement.struct[key][i])
        print('tests done!')


if __name__ == "__main__":
    SqlStatementParser.test()
