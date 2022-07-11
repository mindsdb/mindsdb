import re

from pyparsing import (
    CaselessKeyword,
    originalTextFor,
    ParseException,
    ParserElement,
    QuotedString,
    ZeroOrMore,
    nestedExpr,
    restOfLine,
    StringEnd,
    Literal,
    SkipTo
)
from mindsdb.api.mysql.mysql_proxy.utilities import exceptions as exc

RE_INT = re.compile(r'^[-+]?([1-9]\d*|0)$')
RE_FLOAT = re.compile(r'^[-+]?([1-9]\d*\.\d*|0\.|0\.\d*)$')


class SqlStatementParser():
    def __init__(self, text):
        self._original_sql = text
        self._sql = SqlStatementParser.clear_sql(text)
        self._keyword = SqlStatementParser.get_keyword(self._sql)
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
            begin

            create_predictor
            create_table
            create_datasource
            create_database
        '''
        START, SET, USE, SHOW, DELETE, INSERT, UPDATE, ALTER, SELECT, ROLLBACK, COMMIT, EXPLAIN, CREATE, AI, TABLE, PREDICTOR, VIEW, DATASOURCE, DROP, RETRAIN, DESCRIBE, DATABASE, BEGIN = map(
            CaselessKeyword,
            "START SET USE SHOW DELETE INSERT UPDATE ALTER SELECT ROLLBACK COMMIT EXPLAIN CREATE AI TABLE PREDICTOR VIEW DATASOURCE DROP RETRAIN DESCRIBE DATABASE BEGIN".split()
        )
        CREATE_PREDICTOR = CREATE + PREDICTOR
        CREATE_VIEW = CREATE + VIEW
        CREATE_DATASOURCE = CREATE + DATASOURCE
        CREATE_DATABASE = CREATE + DATABASE
        CREATE_TABLE = CREATE + TABLE

        expr = (
            START | SET | USE
            | SHOW | DELETE | INSERT
            | UPDATE | ALTER | SELECT
            | ROLLBACK | COMMIT | EXPLAIN
            | CREATE_PREDICTOR
            | CREATE_VIEW | DROP | RETRAIN
            | CREATE_DATASOURCE | DESCRIBE
            | CREATE_DATABASE | CREATE_TABLE | BEGIN
        )('keyword')

        r = expr.parseString(sql)

        keyword = '_'.join(r.get('keyword', [])).lower()

        if keyword == 0:
            raise exc.ErSqlSyntaxError('Cant get keyword from statement')

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
