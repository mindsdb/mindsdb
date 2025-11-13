import copy

import pytest

from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Identifier, Select, Join, Constant, Star, Parameter, BinaryOperation
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
    QueryStep,
    SubSelectStep,
    ApplyPredictorRowStep,
    MapReduceStep,
)


class TestPlanJoinPredictor:
    def test_join_predictor_plan(self):
        sql = """
            select tab1.column1, pred.predicted
            from int.tab1, mindsdb.pred
        """
        query = parse_sql(sql)

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Star()], from_table=Identifier("tab1")
                    ),  # No column pruning with predictor joins (predictors may need all columns)
                ),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.INNER_JOIN),
                ),
                QueryStep(parse_sql("select tab1.column1, pred.predicted"), from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # test_predictor_namespace_is_case_insensitive
        plan = plan_query(query, integrations=["int"], predictor_namespace="MINDSDB", predictor_metadata={"pred": {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_join_predictor_plan_aliases(self):
        sql = """
            select ta.column1, tb.predicted
            from int.tab1 ta, mindsdb.pred tb
        """
        query = parse_sql(sql)

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Star()],
                        from_table=Identifier("tab1", alias=Identifier("ta")),
                    ),  # No column pruning with predictor joins
                ),
                ApplyPredictorStep(
                    namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred", alias=Identifier("tb"))
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.INNER_JOIN),
                ),
                QueryStep(parse_sql("select ta.column1, tb.predicted"), from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

    def test_join_predictor_plan_limit(self):
        sql = """
            select tab.column1, pred.predicted
            from int.tab, mindsdb.pred
            where tab.product_id = 'x' and tab.time between '2021-01-01' and '2021-01-31'
            order by tab.column2
            limit 10
            offset 1
        """
        query = parse_sql(sql)

        subquery = copy.deepcopy(query)
        subquery.from_table = None
        subquery.offset = None

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=parse_sql(
                        """
                            select * from tab
                            where product_id = 'x' and time between '2021-01-01' and '2021-01-31'
                            order by column2
                            limit 10
                            offset 1
                        """
                    ),  # No column pruning with predictor joins
                ),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.INNER_JOIN),
                ),
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

    # def test_join_predictor_error_when_filtering_on_predictions(self):
    #     """
    #     Query:
    #     SELECT rental_price_confidence
    #     FROM postgres_90.test_data.home_rentals AS ta
    #     JOIN mindsdb.hrp3 AS tb
    #     WHERE ta.sqft > 1000 AND tb.rental_price_confidence > 0.5
    #     LIMIT 5;
    #     """
    #
    #     query = Select(targets=[Identifier('rental_price_confidence')],
    #                    from_table=Join(left=Identifier('postgres_90.test_data.home_rentals', alias=Identifier('ta')),
    #                                    right=Identifier('mindsdb.hrp3', alias=Identifier('tb')),
    #                                    join_type=JoinType.INNER_JOIN,
    #                                    implicit=True),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('>', args=[Identifier('ta.sqft'), Constant(1000)]),
    #                        BinaryOperation('>', args=[Identifier('tb.rental_price_confidence'), Constant(0.5)]),
    #                    ]),
    #                    limit=5
    #                    )
    #
    #     with pytest.raises(PlanningException):
    #         plan_query(query, integrations=['postgres_90'], predictor_namespace='mindsdb', predictor_metadata={'hrp3': {}})

    def test_join_predictor_plan_complex_query(self):
        sql = """
            select t.asset, t.time, m.predicted
            from int.tab t, mindsdb.pred m
            where t.col1 = 'x'
            group by t.asset
            having t.asset = 'bitcoin'
            order by t.asset
            limit 1
            offset 2
        """
        query = parse_sql(sql)

        subquery = copy.deepcopy(query)
        subquery.from_table = None

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=parse_sql("select * from tab as t where col1 = 'x'"),
                ),  # No column pruning with predictor joins
                ApplyPredictorStep(
                    namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred", alias=Identifier("m"))
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.INNER_JOIN),
                ),
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

    def test_no_predictor_error(self):
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("pred.predicted")],
            from_table=Join(left=Identifier("int.tab1"), right=Identifier("pred"), join_type=None, implicit=True),
        )

        with pytest.raises(PlanningException):
            plan_query(query, integrations=["int"], predictor_metadata={"pred": {}})

    def test_join_predictor_plan_default_namespace_integration(self):
        sql = """
            select tab1.column1, pred.predicted
            from tab1, mindsdb.pred
        """
        query = parse_sql(sql)
        expected_plan = QueryPlan(
            default_namespace="int",
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Star()], from_table=Identifier("tab1")
                    ),  # No column pruning with predictor joins
                ),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.INNER_JOIN),
                ),
                QueryStep(parse_sql("select tab1.column1, pred.predicted"), from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="int",
            predictor_metadata={"pred": {}},
        )

        assert plan.steps == expected_plan.steps

    def test_join_predictor_plan_default_namespace_predictor(self):
        sql = """
                   select tab1.column1, pred.predicted
                   from int.tab1, pred
              """
        query = parse_sql(sql)

        expected_plan = QueryPlan(
            default_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Star()], from_table=Identifier("tab1")
                    ),  # No column pruning with predictor joins
                ),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.INNER_JOIN),
                ),
                QueryStep(parse_sql("select tab1.column1, pred.predicted"), from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="mindsdb",
            predictor_metadata={"pred": {}},
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_nested_select(self):
        # for tableau

        sql = """
            SELECT time
            FROM (
               select * from int.covid
               join mindsdb.pred
               limit 10
            ) `Custom SQL Query`
            limit 1
         """

        query = parse_sql(sql)

        expected_plan = QueryPlan(
            default_namespace="mindsdb",
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from covid limit 10")),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(Select(targets=[Star()], limit=Constant(10)), from_table=Result(2), strict_where=False),
                SubSelectStep(
                    dataframe=Result(3), query=parse_sql("SELECT time limit 1"), table_name="Custom SQL Query"
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="mindsdb",
            predictor_metadata={"pred": {}},
        )

        assert plan.steps == expected_plan.steps

        sql = """
                 SELECT `time`
                 FROM (
                   select * from int.covid
                   join mindsdb.pred
                 ) `Custom SQL Query`
                GROUP BY 1
            """

        query = parse_sql(sql)

        expected_plan = QueryPlan(
            default_namespace="mindsdb",
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from covid")),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                SubSelectStep(
                    dataframe=Result(2),
                    query=Select(targets=[Identifier("`time`")], group_by=[Constant(1)]),
                    table_name="Custom SQL Query",
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="mindsdb",
            predictor_metadata={"pred": {}},
        )

        assert plan.steps == expected_plan.steps

    def test_subselect(self):
        # nested limit is greater
        sql = """
               SELECT *
               FROM (
                  select col from int.covid
                  limit 10
               ) as t
               join mindsdb.pred
               limit 5
            """

        query = parse_sql(sql)

        expected_plan = QueryPlan(
            default_namespace="mindsdb",
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select col as col from covid limit 10")),
                SubSelectStep(query=Select(targets=[Star()]), dataframe=Result(0), table_name="t"),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(1), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(Select(targets=[Star()], limit=Constant(5)), from_table=Result(3), strict_where=False),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="mindsdb",
            predictor_metadata={"pred": {}},
        )
        assert plan.steps == expected_plan.steps

        # only nested select with limit
        sql = """
               SELECT *
               FROM (
                  select * from int.covid
                  join int.info
                  limit 5
               ) as t
               join mindsdb.pred
            """

        query = parse_sql(sql)

        expected_plan = QueryPlan(
            default_namespace="mindsdb",
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from covid join info limit 5")),
                SubSelectStep(query=Select(targets=[Star()]), dataframe=Result(0), table_name="t"),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(1), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="mindsdb",
            predictor_metadata={"pred": {}},
        )
        assert plan.steps == expected_plan.steps


class TestPredictorWithUsing:
    def test_using_join(self):
        sql = """
            select * from int.tab1
            join mindsdb.pred
            using a=1
        """

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1")),
                ApplyPredictorStep(
                    namespace="mindsdb", dataframe=Result(0), predictor=Identifier("pred"), params={"a": 1}
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                ProjectStep(dataframe=Result(2), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # with native query

        sql = """
                    select * from int (select * from tab1) t
                    join mindsdb.pred
                    using a=1
                """

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", raw_query="select * from tab1"),
                SubSelectStep(step_num=1, query=Select(targets=[Star()]), dataframe=Result(0), table_name="t"),
                ApplyPredictorStep(
                    namespace="mindsdb", dataframe=Result(1), predictor=Identifier("pred"), params={"a": 1}
                ),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_using_one_line(self):
        sql = """
            select * from mindsdb.pred where x=2 using a=1
        """

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                ApplyPredictorRowStep(
                    namespace="mindsdb", predictor=Identifier("pred"), row_dict={"x": 2}, params={"a": 1}
                ),
                ProjectStep(dataframe=Result(0), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


class TestPredictorVersion:
    def test_using_join(self):
        sql = """
            select * from int.tab1
            join proj.pred.1
            using a=1
        """

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1")),
                ApplyPredictorStep(
                    namespace="proj", dataframe=Result(0), predictor=Identifier("pred.1"), params={"a": 1}
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                ProjectStep(dataframe=Result(2), columns=[Star()]),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # default namespace

        sql = """
            select * from int.tab1
            join pred.1
            using a=1
        """
        query = parse_sql(sql)

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="proj",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_where_using(self):
        sql = """
            select * from int.tab1 a
            join proj.pred.1 p
            where a.x=1 and p.x=1 and p.ttt=2 and a.y=3 and p.y=''
        """

        subquery = parse_sql(
            """
                        select * from x
                        where a.x=1 and 0=0 and p.ttt=2 and a.y=3 and 0=0
                    """
        )
        subquery.from_table = None

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as a where x=1 and y=3")),
                ApplyPredictorStep(
                    namespace="proj",
                    dataframe=Result(0),
                    predictor=Identifier("pred.1", alias=Identifier("p")),
                    row_dict={"x": 1, "y": ""},
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            predictor_metadata=[{"name": "pred", "integration_name": "proj", "to_predict": ["ttt"]}],
        )

        assert plan.steps == expected_plan.steps

    def test_using_one_line(self):
        sql = """
            select * from proj.pred.1 where x=2 using a=1
        """

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                ApplyPredictorRowStep(
                    namespace="proj", predictor=Identifier("pred.1"), row_dict={"x": 2}, params={"a": 1}
                ),
                ProjectStep(dataframe=Result(0), columns=[Star()]),
            ],
        )
        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # default namespace

        sql = """
             select * from pred.1 where x=2 using a=1
        """
        query = parse_sql(sql)

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            default_namespace="proj",
            predictor_metadata=[{"name": "pred", "integration_name": "proj"}],
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


class TestPredictorParams:
    def test_model_param(self):
        sql = """
            select * from int.tab1 t
            join mindsdb.pred m
            where m.a=1 and t.b=2
        """

        query = parse_sql(sql)

        subquery = parse_sql(
            """
                        select * from x
                        where 0=0 and t.b=2
                    """
        )
        subquery.from_table = None

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as t where b=2")),
                ApplyPredictorStep(
                    namespace="mindsdb",
                    dataframe=Result(0),
                    predictor=Identifier("pred", alias=Identifier("m")),
                    row_dict={"a": 1},
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(subquery, from_table=Result(2), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

        # 3 table
        sql = """
            select * from int.tab1 t
            join int.tab2 t2
            join mindsdb.pred m
            where m.a=1
        """

        subquery = parse_sql(
            """
                        select * from x
                        where 0=0
                    """
        )
        subquery.from_table = None

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as t")),
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab2 as t2")),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                ApplyPredictorStep(
                    namespace="mindsdb",
                    dataframe=Result(2),
                    predictor=Identifier("pred", alias=Identifier("m")),
                    row_dict={"a": 1},
                ),
                JoinStep(
                    left=Result(2),
                    right=Result(3),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(subquery, from_table=Result(4), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

    def test_complex_subselect(self):
        sql = """
                select t2.x, m.id, (select a from int.tab0 where x=0) from int.tab1 t1
                join int.tab2 t2 on t1.x = t2.a
                join mindsdb.pred m
                where m.a=(select a from int.tab3 where x=3)
                  and t2.x=(select a from int.tab4 where x=4)
                  and t1.b=1 and t2.b=2 and t1.a = t2.a
        """

        subquery = parse_sql(
            """
                select t2.x, m.id, x
                from x
                where 0=0
                      and t2.x=x
                      and t1.b=1 and t2.b=2 and t1.a = t2.a
            """
        )
        subquery.from_table = None
        subquery.targets[2] = Parameter(Result(0))
        subquery.where.args[0].args[0].args[0].args[1].args[1] = Parameter(Result(2))

        query = parse_sql(sql)

        # Construct query for tab2 with Parameter manually since parse_sql doesn't support Parameter syntax
        q_table2 = parse_sql("select * from tab2 as t2 where x=0 and b=2")
        q_table2.where.args[0].args[1] = Parameter(Result(2))

        expected_plan = QueryPlan(
            steps=[
                # nested queries
                FetchDataframeStep(integration="int", query=parse_sql("select a as a from tab0 where x=0")),
                FetchDataframeStep(integration="int", query=parse_sql("select a as a from tab3 where x=3")),
                FetchDataframeStep(integration="int", query=parse_sql("select a as a from tab4 where x=4")),
                # tables (no column pruning with predictor joins)
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as t1 where b=1")),
                FetchDataframeStep(integration="int", query=q_table2),
                JoinStep(
                    left=Result(3),
                    right=Result(4),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        join_type=JoinType.JOIN,
                        condition=BinaryOperation(op="=", args=[Identifier("t1.x"), Identifier("t2.a")]),
                    ),
                ),
                # model
                ApplyPredictorStep(
                    namespace="mindsdb",
                    dataframe=Result(5),
                    predictor=Identifier("pred", alias=Identifier("m")),
                    row_dict={"a": Result(1)},
                ),
                JoinStep(
                    left=Result(5),
                    right=Result(6),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(subquery, from_table=Result(7), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

    def test_model_join_model(self):
        sql = """
                     select * from int.tab1 t
                      join mindsdb.pred m
                      join mindsdb.pred m2
                    where m.a = 2
                    using m.param1 = 'a',
                          m2.param2 = 'b',
                          param3 = 'c'
              """

        subquery = parse_sql(
            """
                        select * from x
                        where 0=0
                    """
        )
        subquery.from_table = None

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as t")),
                ApplyPredictorStep(
                    namespace="mindsdb",
                    dataframe=Result(0),
                    predictor=Identifier("pred", alias=Identifier("m")),
                    row_dict={"a": 2},
                    params={"param1": "a", "param3": "c"},
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                ApplyPredictorStep(
                    namespace="mindsdb",
                    dataframe=Result(2),
                    predictor=Identifier("pred", alias=Identifier("m2")),
                    params={"param2": "b", "param3": "c"},
                ),
                JoinStep(
                    left=Result(2),
                    right=Result(3),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(subquery, from_table=Result(4), strict_where=False),
            ],
        )
        plan = plan_query(query, integrations=["int"], predictor_namespace="mindsdb", predictor_metadata={"pred": {}})

        assert plan.steps == expected_plan.steps

    def test_model_column_map(self):
        sql = """
            select * from int.tab1 a
            join proj.pred.1 p on a.data1 = p.data2 and p.x = a.y
        """

        # subquery = parse_sql("""
        #     select * from x
        #     where a.x=1 and 0=0 and p.ttt=2 and a.y=3 and 0=0
        # """)
        # subquery.from_table = None

        query = parse_sql(sql)
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as a")),
                ApplyPredictorStep(
                    namespace="proj",
                    dataframe=Result(0),
                    predictor=Identifier("pred.1", alias=Identifier("p")),
                    columns_map={"data2": Identifier("a.data1"), "x": Identifier("a.y")},
                ),
                JoinStep(
                    left=Result(0),
                    right=Result(1),
                    query=Join(
                        left=Identifier("tab1"),
                        right=Identifier("tab2"),
                        join_type=JoinType.JOIN,
                        condition=BinaryOperation(
                            "and",
                            args=[
                                BinaryOperation("=", args=[Constant(0), Constant(0)]),
                                BinaryOperation("=", args=[Constant(0), Constant(0)]),
                            ],
                        ),
                    ),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            predictor_metadata=[{"name": "pred", "integration_name": "proj", "to_predict": ["ttt"]}],
        )

        assert plan.steps == expected_plan.steps

    def test_partition(self):
        sql = """
            select p1.* from int.tab1 a
            join proj.pred1 p1
            join proj.pred2 p2
            using partition_size=1000
        """

        query = parse_sql(sql)

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration="int", query=parse_sql("select * from tab1 as a")),
                MapReduceStep(
                    values=Result(0),
                    step=[
                        ApplyPredictorStep(
                            step_num="1_0",
                            namespace="proj",
                            dataframe=Result(0),
                            params={},
                            predictor=Identifier("pred1", alias=Identifier("p1")),
                        ),
                        JoinStep(
                            step_num="1_1",
                            left=Result(0),
                            right=Result("1_0"),
                            query=Join(
                                left=Identifier("tab1"),
                                right=Identifier("tab2"),
                                join_type=JoinType.JOIN,
                            ),
                        ),
                        ApplyPredictorStep(
                            step_num="1_2",
                            namespace="proj",
                            dataframe=Result("1_1"),
                            params={},
                            predictor=Identifier("pred2", alias=Identifier("p2")),
                        ),
                        JoinStep(
                            step_num="1_3",
                            left=Result("1_1"),
                            right=Result("1_2"),
                            query=Join(
                                left=Identifier("tab1"),
                                right=Identifier("tab2"),
                                join_type=JoinType.JOIN,
                            ),
                        ),
                    ],
                    partition=1000,
                ),
                QueryStep(parse_sql("select p1.*"), from_table=Result(1), strict_where=False),
            ],
        )

        plan = plan_query(
            query,
            integrations=["int"],
            predictor_namespace="mindsdb",
            predictor_metadata=[
                {"name": "pred1", "integration_name": "proj", "to_predict": ["ttt"]},
                {"name": "pred2", "integration_name": "proj", "to_predict": ["ttt"]},
            ],
        )

        assert plan.steps == expected_plan.steps
