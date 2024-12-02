import datetime as dt

from mindsdb_sql_parser.ast import (
    Identifier, Select, Star, Constant, Tuple, BinaryOperation, CreateTable, TableColumn, Insert
)
from mindsdb_sql_parser import parse_sql
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender


class TestRender:
    def test_create_table(self):

        query = CreateTable(
            name='tbl1',
            columns=[
                TableColumn(name='a', type='DATE'),
                TableColumn(name='b', type='INTEGER'),
            ]
        )

        sql = SqlalchemyRender('mysql').get_string(query, with_failback=False)

        sql2 = '''CREATE TABLE tbl1 (a DATE, b INTEGER)'''

        assert sql.replace('\n', '').replace('\t', '').replace('  ', ' ') == sql2

    def test_datetype(self):
        query = Select(targets=[Constant(value=dt.datetime(2011, 1, 1))])

        sql = SqlalchemyRender('mysql').get_string(query, with_failback=False)

        sql2 = '''SELECT '2011-01-01 00:00:00' AS `2011-01-01 00:00:00`'''
        assert sql == sql2

        query = Select(targets=[Star()],
                       from_table=Identifier('tb1'),
                       where=BinaryOperation(op='in', args=[
                           Identifier('x'),
                           Tuple(items=[Constant(value=dt.datetime(2011, 1, 1)),
                                        Constant(value=dt.datetime(2011, 1, 2))])
                       ]))
        sql = SqlalchemyRender('mysql').get_string(query, with_failback=False)

        sql2 = '''SELECT * FROM tb1 WHERE x IN ('2011-01-01 00:00:00', '2011-01-02 00:00:00')'''
        assert sql.replace('\n', '').replace('\t', '').replace('  ', ' ') == sql2

    def test_exec_params(self):

        values = [
            [1, '2'],
            [3, 'b'],
        ]

        query = Insert(
            table=Identifier('tbl1'),
            columns=[
                Identifier('a'),
                Identifier('b'),
            ],
            values=values,
            is_plain=True
        )

        sql, params = SqlalchemyRender('mysql').get_exec_params(query, with_failback=False)

        assert sql == '''INSERT INTO tbl1 (a, b) VALUES (%s, %s)'''
        assert params == values

    def test_alias_in_case(self):
        sql = """
           select case mean when 0 then null else stdev/mean end cov from table1
        """

        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        # check queries are the same after render
        assert str(query) == str(parse_sql(rendered))

    def test_extra_cast_in_division(self):
        sql = """
           select a / b from table1
        """

        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        # check queries are the same after render
        assert str(query) == str(parse_sql(rendered))

    def test_quoted_case(self):

        query = Select(targets=[Identifier('Test')])
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)
        assert rendered == 'SELECT Test'

        query = Select(targets=[Identifier('table')])
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)
        assert rendered == 'SELECT "table"'
