import datetime as dt
from textwrap import dedent

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
           select a / b as col1 from table1
        """

        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        # check queries are the same after render
        assert str(query) == str(parse_sql(rendered))

    def test_quoted_mixed_case(self):

        query = Select(targets=[Identifier('Test', alias=Identifier('Test2'))])
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)
        assert rendered == 'SELECT Test AS Test2'

        query = Select(targets=[Identifier('table')])
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)
        assert rendered == 'SELECT "table"'

    def test_star_in_path(self):
        sql = "select t.* from table t"

        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        # check queries are the same after render
        assert str(query) == str(parse_sql(rendered))

    def test_div(self):

        sql0 = 'select 1 / 2 - (9 / 4 - 1) * 3 as x'
        query = parse_sql(sql0)

        sql = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        assert sql.lower() == sql0

    def test_quoted_identifier(self):
        sql = "SELECT `A`.*, A.`B` AS `Bb`, `c` as Cc FROM Tbl.`Tab` AS `Tt`"

        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        # check queries are the same after render
        assert rendered.replace('\n', '') == 'SELECT "A".*, A."B" AS "Bb", "c" AS Cc FROM Tbl."Tab" AS "Tt"'

    def test_intersect_except(self):
        for op in ('EXCEPT', 'INTERSECT'):
            sql = dedent(f"""
            SELECT * FROM tbl1
            {op} SELECT * FROM tbl2
            """).strip()

            query = parse_sql(sql)
            rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

            assert rendered.replace('\n', '') == sql.replace('\n', ' ')

    def test_in_with_single_value(self):
        sql = "SELECT * FROM tbl1 WHERE x IN (1)"
        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        assert rendered.replace('\n', '') == sql

    def test_mixed_join(self):
        sql = """
            SELECT * FROM tbl1
            join tbl2 on tbl1.x = tbl2.x,
            tbl3
        """
        query = parse_sql(sql)
        rendered = SqlalchemyRender('postgres').get_string(query, with_failback=False)

        expected = dedent("""
            SELECT * FROM tbl1
            JOIN tbl2 ON tbl1.x = tbl2.x
            JOIN tbl3 ON 1=1
        """).strip()

        assert rendered.replace('\n', '') == expected.replace('\n', ' ')
