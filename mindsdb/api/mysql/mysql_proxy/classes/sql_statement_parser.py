from pyparsing import (
    CaselessKeyword,
    ParseException,
    ParserElement,
    QuotedString,
    ZeroOrMore,
    StringEnd,
    OneOrMore,
    Suppress,
    Optional,
    Literal,
    SkipTo,
    Group,
    Word,
    originalTextFor,
    delimitedList,
    quotedString,
    printables,
    nestedExpr,
    restOfLine,
    alphanums,
    tokenMap,
    alphas,
    nums
)
import re
import json

import moz_sql_parser as moz_sql


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
            elif self._keyword == 'create_predictor':
                self._struct = self.parse_as_create_predictor()
            elif self._keyword in 'create_ai_table':
                self._struct = self.parse_as_create_ai_table()

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
        ''' remove comments from sql
            TODO current implementation is not remove /**/ from mid of string:
            select a, /*comment*/ from b
        '''
        # remove /*comment*/
        ParserElement.defaultWhitespaceChars = (" \t")
        comment = nestedExpr('/*', '*/').suppress()
        starting = ZeroOrMore(comment.suppress())
        ending = ZeroOrMore(comment | ';').suppress() + StringEnd()
        expr = starting + SkipTo(ending) + ending
        sql = expr.transformString(sql)

        # remove -- and # comments
        oracleSqlComment = Literal("--") + restOfLine
        mySqlComment = Literal("#") + restOfLine

        expr = (
            originalTextFor(QuotedString("'"))
            | originalTextFor(QuotedString('"'))
            | originalTextFor(QuotedString('`'))
            | (oracleSqlComment | mySqlComment).suppress()
        )

        sql = expr.transformString(sql)
        sql = sql.strip(' \n\t')

        return sql

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

            create_predictor
            create_ai_table
        '''
        START, SET, USE, SHOW, DELETE, INSERT, UPDATE, ALTER, SELECT, ROLLBACK, COMMIT, EXPLAIN, CREATE, AI, TABLE, PREDICTOR, VIEW = map(
            CaselessKeyword, "START SET USE SHOW DELETE INSERT UPDATE ALTER SELECT ROLLBACK COMMIT EXPLAIN CREATE AI TABLE PREDICTOR VIEW".split()
        )
        CREATE_PREDICTOR = CREATE + PREDICTOR
        CREATE_AI_TABLE = CREATE + AI + TABLE
        CREATE_VIEW = CREATE + VIEW

        expr = (
            START | SET | USE
            | SHOW | DELETE | INSERT
            | UPDATE | ALTER | SELECT
            | ROLLBACK | COMMIT | EXPLAIN
            | CREATE_PREDICTOR | CREATE_AI_TABLE
            | CREATE_VIEW
        )('keyword')

        r = expr.parseString(sql)

        keyword = '_'.join(r.get('keyword', [])).lower()

        if keyword == 0:
            raise Exception('Cant get keyword from statement')

        if keyword == 'create_view':
            keyword = 'create_ai_table'

        return keyword

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

    def parse_as_create_ai_table(self) -> dict:
        CREATE, AI, TABLE, VIEW, FROM, USING, AS = map(
            CaselessKeyword, "CREATE AI TABLE VIEW FROM USING AS".split()
        )

        AI_TABLE = AI + TABLE

        word = Word(alphanums + "_")

        expr = (
            CREATE + (AI_TABLE | VIEW) + word('ai_table_name') + AS
            + originalTextFor(nestedExpr('(', ')'))('select')
        )

        r = expr.parseString(self._sql)
        r = r.asDict()

        if r['select'].startswith('(') and r['select'].endswith(')'):
            r['select'] = r['select'][1:-1]
        r['select'] = r['select'].strip(' \n')

        select = moz_sql.parse(r['select'])

        if 'from' not in select \
           or len(select['from']) != 2 \
           or 'join' not in select['from'][1]:
            raise Exception("'from' must be like: 'from integration.table join predictor'")

        # add 'name' to each statement
        for s in [*select['select'], select['from'][0], select['from'][1]['join']]:
            if 'name' not in s:
                if '.' in s['value']:
                    s['name'] = s['value'][s['value'].find('.') + 1:]
                else:
                    s['name'] = s['value']

        f = {
            'integration': select['from'][0],
            'predictor': select['from'][1]['join']
        }

        # remove predictor join
        select['from'].pop()

        new_select = []
        predictor_fields = []
        integration_prefix = f"{f['integration']['name']}."
        for s in select['select']:
            if s['value'].startswith(integration_prefix):
                s['value'] = s['value'][len(integration_prefix):]
                new_select.append(s)
            else:
                predictor_fields.append(s)

        predictor_prefix = f"{f['predictor']['name']}."
        for pf in predictor_fields:
            if pf['value'].startswith(predictor_prefix):
                pf['value'] = pf['value'][len(predictor_prefix):]

        integration_name = f['integration']['value'][:f['integration']['value'].find('.')]
        f['integration']['value'] = f['integration']['value'][len(integration_name) + 1:]
        select['select'] = new_select
        integration_sql = moz_sql.format(select)

        res = {
            'ai_table_name': r['ai_table_name'],
            'integration_name': integration_name,
            'integration_query': integration_sql,
            'query_fields': select['select'],
            'predictor_name': f['predictor']['value'],
            'predictor_fields': predictor_fields
        }

        return res

    def parse_as_create_predictor(self) -> dict:
        CREATE, PREDICTOR, FROM, WHERE, PREDICT, AS, ORDER, GROUP, BY, WINDOW, HORIZON, USING, ASK, DESC = map(
            CaselessKeyword, "CREATE PREDICTOR FROM WHERE PREDICT AS ORDER GROUP BY WINDOW HORIZON USING ASK DESC".split()
        )
        ORDER_BY = ORDER + BY
        GROUP_BY = GROUP + BY

        word = Word(alphanums + "_")

        s_int = Word(nums).setParseAction(tokenMap(int))

        predict_item = Group(word('name') + Optional(AS.suppress() + word('alias')))

        order_item = Group(word('name') + Optional(ASK | DESC)('sort'))

        using_item = Group(word('name') + Word('=').suppress() + (word | QuotedString("'"))('value'))

        expr = (
            CREATE + PREDICTOR + word('predictor_name') + FROM + Optional(word)('integration_name')
            + originalTextFor(nestedExpr('(', ')'))('select') + Optional(AS + word('datasource_name'))
            + PREDICT
            + delimitedList(predict_item, delim=',')('predict')
            + Optional(ORDER_BY + delimitedList(order_item, delim=',')('order_by'))
            + Optional(GROUP_BY + delimitedList(word, delim=',')('group_by'))
            + Optional(WINDOW + s_int('window'))
            + Optional(HORIZON + s_int('nr_predictions'))
            + Optional(
                (USING + delimitedList(using_item, delim=',')('using'))
                | (USING + originalTextFor(nestedExpr('{', '}'))('using'))
            )
        )

        r = expr.parseString(self._sql)

        # postprocessing
        r = r.asDict()
        if r['select'].startswith('(') and r['select'].endswith(')'):
            r['select'] = r['select'][1:-1]
        r['select'] = r['select'].strip(' \n')

        using = r.get('using')
        if isinstance(using, str):
            r['using'] = json.loads(using)
        elif isinstance(using, list):
            new_using = {}
            for el in using:
                if el['name'] == 'stop_training_in_x_seconds':
                    new_using['time_aim'] = el['value']
                else:
                    new_using[el['name']] = el['value']
            r['using'] = new_using

        if isinstance(r.get('order_by'), list):
            r['order_by'] = [x['name'] for x in r['order_by']]

        return r

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
                Word(printables, excludeChars='().`')
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
    def test_create():
        def check_recursive(a, b):
            assert type(a) == type(b)
            if isinstance(a, dict):
                for key in a:
                    check_recursive(a[key], b[key])
            elif isinstance(a, list):
                for i in range(len(a)):
                    check_recursive(a[i], b[i])
            else:
                assert a == b

        tests = [[
            '''
                CREATE PREDICTor debt_model_1
                FROM integration_name (select whatever) as ds_name
                PREDICT f1 as f1_alias, f2, f3 as f3_alias
                order by f_order_1 ASK, f_order_2, f_order_3 DESC
                group by f_group_1, f_group_2
                window 100
                HORIZON 7
                using {"x": 1, "y": "a"}
            ''', {
                'predictor_name': 'debt_model_1',
                'integration_name': 'integration_name',
                'select': 'select whatever',
                'datasource_name': 'ds_name',
                'predict': [{'name': 'f1', 'alias': 'f1_alias'}, {'name': 'f2'}, {'name': 'f3', 'alias': 'f3_alias'}],
                # 'order_by': [{'name': 'f_order_1', 'sort': 'ASK'}, {'name': 'f_order_2'}, {'name': 'f_order_3', 'sort': 'DESC'}],
                'order_by': ['f_order_1', 'f_order_2', 'f_order_3'],
                'group_by': ['f_group_1', 'f_group_2'],
                'window': 100,
                'nr_predictions': 7,
                'using': {'x': 1, 'y': 'a'}
            }
        ], [
            # '''
            # CREATE AI table ai_table_name
            # FROM integration (select * from table)
            # USING model_name
            # ''',
            '''
            CREATE AI table ai_table_name as (
                SELECT
                    a.col1,
                    a.col2,
                    a.col3,
                    p.col3 as pred_col3
                FROM integration_name.table_name as a
                JOIN predictor_name as p
            )
            ''',
            {
                'ai_table_name': 'ai_table_name',
                'integration_name': 'integration',
                'select': 'select * from table',
                'predictor_name': 'model_name'
            }
        ]]
        for sql, result in tests:
            statement = SqlStatementParser(sql)
            struct = statement.struct
            check_recursive(struct, result)
            check_recursive(result, struct)

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
                "insert into a.b(col1, col2) values ('val1', 'val2');",
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
    SqlStatementParser.test_create()
    SqlStatementParser.test()
