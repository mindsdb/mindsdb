import copy

import pytest

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import (
    Identifier,
    Select,
    Join,
    Constant,
    Star,
    BinaryOperation,
    OrderBy,
    Function,
)
from mindsdb_sql_parser.utils import JoinType

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.step_result import Result
from mindsdb.api.executor.planner.steps import (
    FetchDataframeStep,
    ProjectStep,
    JoinStep,
    ApplyPredictorStep,
    SubSelectStep,
    QueryStep,
)


class TestPlanJoinTables:
    def test_join_tables_plan(self):
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("tab2.column1"), Identifier("tab2.column2")],
            from_table=Join(
                left=Identifier("int.tab1"),
                right=Identifier("int2.tab2"),
                condition=BinaryOperation(op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]),
                join_type=JoinType.INNER_JOIN,
            ),
        )
        plan = plan_query(query, integrations=["int", "int2"])
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],  # Column pruning
                        from_table=Identifier("tab1"),
                    ),
                ),
                FetchDataframeStep(
                    integration="int2",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Identifier("column2", alias=Identifier("column2")),
                        ],  # Column pruning
                        from_table=Identifier("tab2"),
                    ),
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(
                            op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]
                        ),
                        join_type=JoinType.INNER_JOIN,
                    ),
                ),
                QueryStep(
                    parse_sql("select tab1.column1, tab2.column1, tab2.column2"),
                    from_table=Result(2),
                    strict_where=False,
                ),
            ],
        )

        assert plan.steps == expected_plan.steps

    def test_join_tables_where_plan(self):
        query = parse_sql(
            """
              SELECT tab1.column1, tab2.column1, tab2.column2
              FROM int.tab1
              INNER JOIN int2.tab2 ON tab1.column1 > tab2.column1
              WHERE ((tab1.column1 = 1)
                AND (tab2.column1 = 0))
                AND (tab1.column3 = tab2.column3)
            """
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None
        subquery.offset = None

        plan = plan_query(query, integrations=["int", "int2"])
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=parse_sql("SELECT column1 AS column1, column3 AS column3 FROM tab1 WHERE (column1 = 1)"),
                ),
                FetchDataframeStep(
                    integration="int2",
                    query=parse_sql(
                        "SELECT column1 AS column1, column2 AS column2, column3 AS column3 FROM tab2 WHERE (column1 = 0)"
                    ),
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(
                            op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]
                        ),
                        join_type=JoinType.INNER_JOIN,
                    ),
                ),
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )

        assert plan.steps == expected_plan.steps

    def test_join_tables_plan_groupby(self):
        query = Select(
            targets=[
                Identifier("tab1.column1"),
                Identifier("tab2.column1"),
                Function("sum", args=[Identifier("tab2.column2")], alias=Identifier("total")),
            ],
            from_table=Join(
                left=Identifier("int.tab1"),
                right=Identifier("int2.tab2"),
                condition=BinaryOperation(op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]),
                join_type=JoinType.INNER_JOIN,
            ),
            group_by=[Identifier("tab1.column1"), Identifier("tab2.column1")],
            having=BinaryOperation(op="=", args=[Identifier("tab1.column1"), Constant(0)]),
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None
        subquery.offset = None

        plan = plan_query(query, integrations=["int", "int2"])
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],  # Column pruning
                        from_table=Identifier("tab1"),
                    ),
                ),
                FetchDataframeStep(
                    integration="int2",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Identifier("column2", alias=Identifier("column2")),
                        ],  # Column pruning
                        from_table=Identifier("tab2"),
                    ),
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(
                            op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]
                        ),
                        join_type=JoinType.INNER_JOIN,
                    ),
                ),
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )
        assert plan.steps == expected_plan.steps

    def test_join_tables_plan_limit_offset(self):
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("tab2.column1"), Identifier("tab2.column2")],
            from_table=Join(
                left=Identifier("int.tab1"),
                right=Identifier("int2.tab2"),
                condition=BinaryOperation(op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]),
                join_type=JoinType.INNER_JOIN,
            ),
            limit=Constant(10),
            offset=Constant(15),
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None

        plan = plan_query(query, integrations=["int", "int2"])
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],  # Column pruning
                        from_table=Identifier("tab1"),
                        # LIMIT should NOT be pushed down to individual table fetches in joins
                    ),
                ),
                FetchDataframeStep(
                    integration="int2",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Identifier("column2", alias=Identifier("column2")),
                        ],  # Column pruning
                        from_table=Identifier("tab2"),
                    ),
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(
                            op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]
                        ),
                        join_type=JoinType.INNER_JOIN,
                    ),
                ),
                # LIMIT and OFFSET applied after join
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )

        assert plan.steps == expected_plan.steps

    def test_join_tables_plan_order_by(self):
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("tab2.column1"), Identifier("tab2.column2")],
            from_table=Join(
                left=Identifier("int.tab1"),
                right=Identifier("int2.tab2"),
                condition=BinaryOperation(op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]),
                join_type=JoinType.INNER_JOIN,
            ),
            limit=Constant(10),
            offset=Constant(15),
            order_by=[OrderBy(field=Identifier("tab1.column1"))],
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None

        plan = plan_query(query, integrations=["int", "int2"])
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],  # Column pruning
                        from_table=Identifier("tab1"),
                        # ORDER BY and LIMIT should NOT be pushed down to individual table fetches in joins
                    ),
                ),
                FetchDataframeStep(
                    integration="int2",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Identifier("column2", alias=Identifier("column2")),
                        ],  # Column pruning
                        from_table=Identifier("tab2"),
                    ),
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(
                            op=">", args=[Identifier("tab1.column1"), Identifier("tab2.column1")]
                        ),
                        join_type=JoinType.INNER_JOIN,
                    ),
                ),
                # ORDER BY, LIMIT and OFFSET applied after join
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )

        assert plan.steps == expected_plan.steps

    # This quiery should be sent to integration without raising exception
    # def test_join_tables_where_ambigous_column_error(self):
    #     query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
    #                    from_table=Join(left=Identifier('int.tab1'),
    #                                    right=Identifier('int.tab2'),
    #                                    condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
    #                                                                            Identifier('tab2.column1')]),
    #                                    join_type=JoinType.INNER_JOIN
    #                                    ),
    #                    where=BinaryOperation('and',
    #                                          args=[
    #                                              BinaryOperation('and',
    #                                                              args=[
    #                                                                  BinaryOperation('=',
    #                                                                                  args=[Identifier('tab1.column1'),
    #                                                                                        Constant(1)]),
    #                                                                  BinaryOperation('=',
    #                                                                                  args=[Identifier('tab2.column1'),
    #                                                                                        Constant(0)]),
    #
    #                                                              ]
    #                                                              ),
    #                                              BinaryOperation('=',
    #                                                              args=[Identifier('column3'),
    #                                                                    Constant(0)]),
    #                                              # Ambigous column: no idea what table column3 comes from
    #                                          ]
    #                                          )
    #                    )
    #
    #     with pytest.raises(PlanningException) as e:
    #         plan_query(query, integrations=['int'])

    def test_join_tables_disambiguate_identifiers_in_condition(self):
        query = parse_sql(
            """
                SELECT tab1.column1, tab2.column1, tab2.column2
                FROM int.tab1
                INNER JOIN int.tab2 ON int.tab1.column1 = tab2.column1
            """
        )
        plan = plan_query(query, integrations=["int"])
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(integration="int", query=query),
                FetchDataframeStep(
                    integration="int",
                    query=Select(targets=[Star()], from_table=Identifier("tab2")),
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(
                            op="=",
                            args=[
                                Identifier("tab1.column1"),  # integration name gets stripped out
                                Identifier("tab2.column1"),
                            ],
                        ),
                        join_type=JoinType.INNER_JOIN,
                    ),
                ),
                ProjectStep(
                    dataframe=Result(2),
                    columns=[Identifier("tab1.column1"), Identifier("tab2.column1"), Identifier("tab2.column2")],
                ),
            ],
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def _disabled_test_join_tables_error_on_unspecified_table_in_condition(self):
        # disabled: identifier can be environment of system variable
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("tab2.column1"), Identifier("tab2.column2")],
            from_table=Join(
                left=Identifier("int.tab1"),
                right=Identifier("int.tab2"),
                condition=BinaryOperation(op="=", args=[Identifier("tab1.column1"), Identifier("column1")]),
                # Table name omitted
                join_type=JoinType.INNER_JOIN,
            ),
        )
        with pytest.raises(PlanningException):
            plan_query(query, integrations=["int"])

    def test_join_tables_error_on_wrong_table_in_condition(self):
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("tab2.column1"), Identifier("tab2.column2")],
            from_table=Join(
                left=Identifier("int.tab1"),
                right=Identifier("int2.tab2"),
                condition=BinaryOperation(op="=", args=[Identifier("tab1.column1"), Identifier("tab3.column1")]),
                # Wrong table name
                join_type=JoinType.INNER_JOIN,
            ),
        )
        with pytest.raises(PlanningException):
            plan_query(query, integrations=["int", "int2"])

    def test_join_tables_plan_default_namespace(self):
        query = parse_sql(
            """
              SELECT tab1.column1, tab2.column1, tab2.column2
               FROM tab1
               INNER JOIN tab2 ON tab1.column1 = tab2.column1
            """
        )

        expected_plan = QueryPlan(
            integrations=["int"],
            default_namespace="int",
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=parse_sql(
                        """
                             SELECT tab1.column1, tab2.column1, tab2.column2
                             FROM tab1
                             INNER JOIN tab2 ON tab1.column1 = tab2.column1
                        """
                    ),
                ),
            ],
        )
        plan = plan_query(query, integrations=["int"], default_namespace="int")

        assert plan.steps == expected_plan.steps

    def test_complex_join_tables(self):
        query = parse_sql(
            """
                    select * from int1.tbl1 t1
                    right join int2.tbl2 t2 on t1.id>t2.id
                    join pred m
                    left join tbl3 on tbl3.id=t1.id
                    where t1.a=1 and t2.b=2 and 1=1
                """
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None

        plan = plan_query(
            query,
            integrations=["int1", "int2", "proj"],
            default_namespace="proj",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int1", query=parse_sql("select * from tbl1 as t1 where a=1")),
                FetchDataframeStep(integration="int2", query=parse_sql("select * from tbl2 as t2 where b=2")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(op=">", args=[Identifier("t1.id"), Identifier("t2.id")]),
                        join_type=JoinType.RIGHT_JOIN,
                    ),
                ),
                ApplyPredictorStep(
                    namespace="proj", dataframe=Result(2), predictor=Identifier("pred", alias=Identifier("m"))
                ),
                JoinStep(
                    left=Result(2),
                    right=Result(3),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                # IN clause filter optimization is disabled - fetch full table
                FetchDataframeStep(integration="proj", query=parse_sql("select * from tbl3")),
                JoinStep(
                    left=Result(4),
                    right=Result(5),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(op="=", args=[Identifier("tbl3.id"), Identifier("t1.id")]),
                        join_type=JoinType.LEFT_JOIN,
                    ),
                ),
                QueryStep(subquery, from_table=Result(6), strict_where=False),
            ]
        )

        assert plan.steps == expected_plan.steps

    def test_complex_join_tables_subselect(self):
        query = parse_sql(
            """
                    select * from int1.tbl1 t1
                    join (
                        select * from int2.tbl3
                        join pred m
                    ) t2 on t1.id = t2.id
                """
        )

        plan = plan_query(
            query,
            integrations=["int1", "int2", "proj"],
            default_namespace="proj",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int1", query=parse_sql("select * from tbl1 as t1")),
                FetchDataframeStep(integration="int2", query=parse_sql("select * from tbl3")),
                ApplyPredictorStep(
                    namespace="proj", dataframe=Result(1), predictor=Identifier("pred", alias=Identifier("m"))
                ),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                SubSelectStep(dataframe=Result(3), query=Select(targets=[Star()]), table_name="t2"),
                JoinStep(
                    left=Result(0),
                    right=Result(4),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        join_type=JoinType.JOIN,
                        condition=BinaryOperation(op="=", args=[Identifier("t1.id"), Identifier("t2.id")]),
                    ),
                ),
            ]
        )

        assert plan.steps == expected_plan.steps

    def test_join_with_select_from_native_query(self):
        query = parse_sql(
            """
                    select * from (
                        select * from int1 (
                            select raw query
                        )
                    ) t1
                    join pred m
                """
        )

        plan = plan_query(
            query,
            integrations=["int1", "int2", "proj"],
            default_namespace="proj",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int1", raw_query="select raw query"),
                SubSelectStep(step_num=1, query=Select(targets=[Star()]), dataframe=Result(0), table_name="t1"),
                ApplyPredictorStep(
                    namespace="proj", dataframe=Result(1), predictor=Identifier("pred", alias=Identifier("m"))
                ),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # select from native query
        # has the same plan

        query = parse_sql(
            """
                    select * from int1 (
                        select raw query
                    ) t1
                    join pred m
                """
        )

        plan = plan_query(
            query,
            integrations=["int1", "int2", "proj"],
            default_namespace="proj",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_join_one_integration(self):
        query = parse_sql(
            """
              SELECT tab1.column1
               FROM int.tab1
               JOIN tab2 ON tab1.column1 = tab2.column1
            """
        )

        expected_plan = QueryPlan(
            integrations=["int"],
            default_namespace="int",
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=parse_sql(
                        """
                         SELECT tab1.column1
                         FROM tab1
                         JOIN tab2 ON tab1.column1 = tab2.column1
                       """
                    ),
                ),
            ],
        )
        plan = plan_query(query, integrations=["int"], default_namespace="int")

        assert plan.steps == expected_plan.steps

    def test_cte(self):
        query = parse_sql(
            """
                        with t1 as (
                           select * from int1.tbl1
                        )
                        select t1.id, t2.* from t1
                        join int2.tbl2 t2 on t1.id>t2.id
                    """
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None

        plan = plan_query(query, integrations=["int1", "int2"], default_namespace="mindsdb")

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int1", query=parse_sql("select * from tbl1")),
                SubSelectStep(
                    dataframe=Result(0), query=parse_sql("SELECT *"), table_name="t1"
                ),  # TODO: CTE column pruning optimization
                FetchDataframeStep(integration="int2", query=parse_sql("select * from tbl2 as t2")),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        condition=BinaryOperation(op=">", args=[Identifier("t1.id"), Identifier("t2.id")]),
                        join_type=JoinType.JOIN,
                    ),
                ),
                QueryStep(parse_sql("SELECT t1.`id`, t2.*"), from_table=Result(3), strict_where=False),
            ]
        )

        assert plan.steps == expected_plan.steps
