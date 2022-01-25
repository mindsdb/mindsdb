import re
import json

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
from mindsdb_sql.parser.ast import (
    Join
)
from mindsdb_sql import parse_sql


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
            if self._keyword in 'create_ai_table':
                self._struct = self.parse_as_create_ai_table()
            else:
                self._struct = None

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
            drop
            retrain
            describe

            create_predictor
            create_table
            create_ai_table
            create_datasource
            create_database
        '''
        START, SET, USE, SHOW, DELETE, INSERT, UPDATE, ALTER, SELECT, ROLLBACK, COMMIT, EXPLAIN, CREATE, AI, TABLE, PREDICTOR, VIEW, DATASOURCE, DROP, RETRAIN, DESCRIBE, DATABASE = map(
            CaselessKeyword,
            "START SET USE SHOW DELETE INSERT UPDATE ALTER SELECT ROLLBACK COMMIT EXPLAIN CREATE AI TABLE PREDICTOR VIEW DATASOURCE DROP RETRAIN DESCRIBE DATABASE".split()
        )
        CREATE_PREDICTOR = CREATE + PREDICTOR
        CREATE_AI_TABLE = CREATE + AI + TABLE
        CREATE_VIEW = CREATE + VIEW
        CREATE_DATASOURCE = CREATE + DATASOURCE
        CREATE_DATABASE = CREATE + DATABASE
        CREATE_TABLE = CREATE + TABLE

        expr = (
            START | SET | USE
            | SHOW | DELETE | INSERT
            | UPDATE | ALTER | SELECT
            | ROLLBACK | COMMIT | EXPLAIN
            | CREATE_PREDICTOR | CREATE_AI_TABLE
            | CREATE_VIEW | DROP | RETRAIN
            | CREATE_DATASOURCE | DESCRIBE
            | CREATE_DATABASE | CREATE_TABLE
        )('keyword')

        r = expr.parseString(sql)

        keyword = '_'.join(r.get('keyword', [])).lower()

        if keyword == 0:
            raise Exception('Cant get keyword from statement')

        if keyword == 'create_view':
            keyword = 'create_ai_table'

        return keyword

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

        select = parse_sql(r['select'])

        if isinstance(select.from_table, Join) is False:
            raise Exception("'from' must be like: 'from integration.table join predictor'")

        integration_name = select.from_table.left.parts[0]
        select.from_table.left.parts = select.from_table.left.parts[1:]
        integration_name_alias = select.from_table.left.alias.parts[0]

        predictor_name = select.from_table.right.parts[0]
        predictor_name_alias = select.from_table.right.alias.parts[0]
        select.from_table = select.from_table.left

        query_fields = []
        predictor_fields = []
        predictor_fields_targets = []

        integration_sql = str(select)

        for target in select.targets:
            if target.parts[0] == integration_name_alias:
                query_fields.append(target.parts[1])
                predictor_fields_targets.append(target)
            elif target.parts[0] == predictor_name_alias:
                predictor_fields.append(target.parts[1])
        select.targets = predictor_fields_targets

        res = {
            'ai_table_name': r['ai_table_name'],
            'integration_name': integration_name,
            'integration_query': integration_sql,
            'query_fields': query_fields,
            'predictor_name': predictor_name,
            'predictor_fields': predictor_fields
        }

        return res
