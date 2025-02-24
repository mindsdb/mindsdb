import copy
import inspect

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import (
    Select, Constant, WindowFunction, Function
)

from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.integrations.utilities.query_traversal import query_traversal


def parse_sql2(sql, dialect='mindsdb'):
    # convert to ast
    query = parse_sql(sql, dialect)

    # skip

    # step1: use mysql dialect and parse again
    try:
        sql2 = SqlalchemyRender('mysql').get_string(query, with_failback=False)
    except NotImplementedError:
        # skip not implemented, immediately exit
        return query

    # remove generated join condition
    sql2 = sql2.replace('ON 1=1', '')

    # workarounds for joins
    if 'INNER JOIN' not in sql:
        sql2 = sql2.replace('INNER JOIN', 'JOIN')

    if 'LEFT OUTER JOIN' not in sql:
        sql2 = sql2.replace('LEFT OUTER JOIN', 'LEFT JOIN')

    if 'FULL OUTER JOIN' not in sql:
        sql2 = sql2.replace('FULL OUTER JOIN', 'FULL JOIN')

    if 'RIGHT JOIN' in sql:
        # TODO skip now, but fix later
        return query

    # cast
    # TODO fix parse error 'SELECT CAST(4 AS SIGNED INTEGER)'
    if ' CAST(4 AS SIGNED INTEGER)' in sql2:
        return query
    sql2 = sql2.replace(' FLOAT', ' float')

    query2 = parse_sql(sql2, 'mindsdb')

    # exclude cases when sqlalchemy replaces some parts of sql
    if not (
        'not a=' in sql  # replaced to a!=
        or 'NOT col1 =' in sql  # replaced to col1!=
        or ' || ' in sql  # replaced to concat(
        or 'current_user()' in sql  # replaced to CURRENT_USER
        or 'user()' in sql  # replaced to USER
        or 'not exists' in sql  # replaced to not(exits(
        or "WHEN R.DELETE_RULE = 'CASCADE'" in sql  # wrapped in parens by sqlalchemy
    ):

        # sqlalchemy could add own aliases for constant
        def clear_target_aliases(node, **args):
            # clear target aliases
            if isinstance(node, Select):
                if node.targets is not None:
                    for target in node.targets:
                        if isinstance(target, Constant) \
                                or isinstance(target, Select) \
                                or isinstance(target, WindowFunction) \
                                or isinstance(target, Function):
                            target.alias = None

                # clear subselect alias
                if isinstance(node.from_table, Select):
                    node.from_table.alias = None

        query_ = copy.deepcopy(query)
        query_traversal(query_, clear_target_aliases)
        query_traversal(query2, clear_target_aliases)

        # and compare with ast before render
        repr1, repr2 = query2.to_tree(), query_.to_tree()
        if 'unbounded preceding' in repr2:
            # sqlalchemy changes case
            assert repr1.lower() == repr2.lower()
        else:
            assert repr1 == repr2

    # step 2: render to different dialects
    dialects = ('postgresql', 'sqlite', 'mssql', 'oracle')

    for dialect2 in dialects:

        try:
            SqlalchemyRender(dialect2).get_string(query, with_failback=False)
        except Exception as e:
            # skips for dialects
            if dialect2 == 'oracle' \
                    and 'does not support in-place multirow inserts' in str(e):
                pass
            elif dialect2 == 'mssql' \
                    and 'requires an order_by when using an OFFSET or a non-simple LIMIT clause' in str(e):
                pass
            elif dialect2 == 'sqlite' and 'extract(MONTH' in sql:
                pass
            else:
                print(dialect2, query.to_string())
                raise

    # keep original behavior
    return query


class TestFromParser:

    def test_from_parser(self):
        # parser tests have to be in PYTHONPATH
        try:
            from test_base_sql import (
                test_select_operations,
                test_delete,
                test_insert,
                test_select_common_table_expression,
                test_select_structure,
                test_union,
                test_misc_sql_queries,
            )

        except ImportError:
            raise RuntimeError("Unable to import render's tests. Make sure they are in PYTHONPATH. It can be done by:"
                               "- git clone https://github.com/mindsdb/mindsdb_sql_parser.git parser_tests"
                               "- env PYTHONPATH=./:parser_tests/tests pytest")

        modules = (
            test_select_operations,
            test_delete,
            test_insert,
            test_select_common_table_expression,
            test_select_structure,
            test_union,
            test_misc_sql_queries
        )
        for module in modules:
            # inject function
            module.parse_sql = parse_sql2

            for class_name, klass in inspect.getmembers(module, predicate=inspect.isclass):
                if not class_name.startswith('Test'):
                    continue

                tests = klass()
                for test_name, test_method in inspect.getmembers(tests, predicate=inspect.ismethod):
                    if (
                        not test_name.startswith('test_')
                        or test_name.endswith('_error')
                        or test_name.endswith('_render_skip')
                    ):
                        continue
                    if test_name == 'test_mixed_join':
                        # FIXME alchemy can't render it
                        continue

                    sig = inspect.signature(test_method)
                    args = []
                    # add dialect
                    if 'dialect' in sig.parameters:
                        args.append('mysql')

                    test_method(*args)
