import pytest

from mindsdb_sql_parser.ast import (
    Identifier,
    Select,
    NullConstant,
    Constant,
    Star,
    Parameter,
    BinaryOperation,
    Function,
    TableColumn,
    OrderBy,
)
from mindsdb_sql_parser import parse_sql

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.query_planner import MINDSDB_SQL_FUNCTIONS
from mindsdb.api.executor.planner.step_result import Result
from mindsdb.api.executor.planner.steps import (
    FetchDataframeStep,
    CreateTableStep,
    SubSelectStep,
    UpdateToTable,
    DeleteStep,
)


class TestPlanIntegrationSelect:
    def test_integration_select_plan(self):
        query = Select(
            targets=[Identifier("column1"), Constant(1), NullConstant(), Function("database", args=[])],
            from_table=Identifier("INT.tab"),
            where=BinaryOperation(
                "and",
                args=[
                    BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                    BinaryOperation(">", args=[Identifier("column3"), Constant(0)]),
                ],
            ),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Constant(1),
                            NullConstant(),
                            Function("database", args=[]),
                        ],
                        from_table=Identifier("tab"),
                        where=BinaryOperation(
                            "and",
                            args=[
                                BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                                BinaryOperation(">", args=[Identifier("column3"), Constant(0)]),
                            ],
                        ),
                    ),
                    step_num=0,
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_integration_name_is_case_insensitive(self):
        query = Select(
            targets=[Identifier("tab.column1")],
            from_table=Identifier("INT.tab"),
            where=BinaryOperation(
                "and",
                args=[
                    BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                    BinaryOperation(">", args=[Identifier("column3"), Constant(0)]),
                ],
            ),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="INT",
                    query=Select(
                        targets=[Identifier("tab.column1", alias=Identifier("column1"))],
                        from_table=Identifier("tab"),
                        where=BinaryOperation(
                            "and",
                            args=[
                                BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                                BinaryOperation(">", args=[Identifier("column3"), Constant(0)]),
                            ],
                        ),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["INT"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_limit_offset(self):
        query = Select(
            targets=[Identifier("column1")],
            from_table=Identifier("int.tab"),
            where=BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
            limit=Constant(10),
            offset=Constant(15),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],
                        from_table=Identifier("tab"),
                        where=BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                        limit=Constant(10),
                        offset=Constant(15),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_order_by(self):
        query = Select(
            targets=[Identifier("column1")],
            from_table=Identifier("int.tab"),
            where=BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
            limit=Constant(10),
            offset=Constant(15),
            order_by=[OrderBy(field=Identifier("tab.column1"))],
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],
                        from_table=Identifier("tab"),
                        where=BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                        limit=Constant(10),
                        offset=Constant(15),
                        order_by=[OrderBy(field=Identifier("tab.column1"))],
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_plan_star(self):
        query = Select(targets=[Star()], from_table=Identifier("int.tab"))
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(integration="int", query=Select(targets=[Star()], from_table=Identifier("tab"))),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_plan_complex_path(self):
        query = Select(
            targets=[Identifier(parts=["int", "tab", "a column with spaces"])], from_table=Identifier("int.tab")
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("tab.`a column with spaces`", alias=Identifier("a column with spaces"))],
                        from_table=Identifier("tab"),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_table_alias(self):
        query = Select(targets=[Identifier("alias.col1")], from_table=Identifier("int.tab", alias=Identifier("alias")))

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier(parts=["alias", "col1"], alias=Identifier("col1"))],
                        from_table=Identifier(parts=["tab"], alias=Identifier("alias")),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_column_alias(self):
        query = Select(targets=[Identifier("col1", alias=Identifier("column_alias"))], from_table=Identifier("int.tab"))

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier(parts=["col1"], alias=Identifier("column_alias"))],
                        from_table=Identifier(parts=["tab"]),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_table_alias_full_query(self):
        sql = "select ta.sqft from int.test_data.home_rentals as ta"

        query = parse_sql(sql)

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier(parts=["ta", "sqft"], alias=Identifier("sqft"))],
                        from_table=Identifier(parts=["test_data", "home_rentals"], alias=Identifier("ta")),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_plan_group_by(self):
        query = Select(
            targets=[
                Identifier("column1"),
                Identifier("column2"),
                Function(op="sum", args=[Identifier(parts=["column3"])], alias=Identifier("total")),
            ],
            from_table=Identifier("int.tab"),
            group_by=[Identifier("column1"), Identifier("column2")],
            having=BinaryOperation("=", args=[Identifier("column1"), Constant(0)]),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Identifier("column2", alias=Identifier("column2")),
                            Function(op="sum", args=[Identifier(parts=["column3"])], alias=Identifier("total")),
                        ],
                        from_table=Identifier("tab"),
                        group_by=[Identifier("column1"), Identifier("column2")],
                        having=BinaryOperation("=", args=[Identifier("column1"), Constant(0)]),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_no_integration_error(self):
        query = Select(
            targets=[Identifier("tab1.column1"), Identifier("pred.predicted")], from_table=Identifier("int.tab")
        )
        with pytest.raises(PlanningException):
            plan_query(query, integrations=[], predictor_namespace="mindsdb")

    def test_integration_select_subquery_in_target(self):
        query = Select(
            targets=[
                Identifier("column1"),
                Select(
                    targets=[Identifier("column2")],
                    from_table=Identifier("int.tab"),
                    limit=Constant(1),
                    alias=Identifier("subquery"),
                ),
            ],
            from_table=Identifier("int.tab"),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Select(
                                targets=[Identifier("column2", alias=Identifier("column2"))],
                                from_table=Identifier("tab"),
                                limit=Constant(1),
                                alias=Identifier("subquery"),
                            ),
                        ],
                        from_table=Identifier("tab"),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_subquery_in_from(self):
        query = Select(
            targets=[Identifier("column1")],
            from_table=Select(
                targets=[Identifier("column1")], from_table=Identifier("int.tab"), alias=Identifier("subquery")
            ),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Identifier("column1", alias=Identifier("column1"))],
                        from_table=Select(
                            targets=[Identifier("column1", alias=Identifier("column1"))],
                            from_table=Identifier("tab"),
                            alias=Identifier("subquery"),
                        ),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_subquery_in_where(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier("int.tab1"),
            where=BinaryOperation(
                op="in",
                args=(
                    Identifier(parts=["column1"]),
                    Select(targets=[Identifier("column2")], from_table=Identifier("int.tab2"), parentheses=True),
                ),
            ),
        )

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[Star()],
                        from_table=Identifier("tab1"),
                        where=BinaryOperation(
                            op="in",
                            args=[
                                Identifier("column1"),
                                Select(
                                    targets=[Identifier("column2", alias=Identifier("column2"))],
                                    from_table=Identifier("tab2"),
                                    parentheses=True,
                                ),
                            ],
                        ),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"])

        assert plan.steps == expected_plan.steps

    def test_integration_select_default_namespace(self):
        query = Select(
            targets=[Identifier("column1"), Constant(1), Function("database", args=[])],
            from_table=Identifier("tab"),
            where=BinaryOperation(
                "and",
                args=[
                    BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                    BinaryOperation(">", args=[Identifier("column3"), Constant(0)]),
                ],
            ),
        )

        expected_plan = QueryPlan(
            integrations=["int"],
            default_namespace="int",
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                            Constant(1),
                            Function("database", args=[]),
                        ],
                        from_table=Identifier("tab"),
                        where=BinaryOperation(
                            "and",
                            args=[
                                BinaryOperation("=", args=[Identifier("column1"), Identifier("column2")]),
                                BinaryOperation(">", args=[Identifier("column3"), Constant(0)]),
                            ],
                        ),
                    ),
                    step_num=0,
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"], default_namespace="int")

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_integration_select_default_namespace_subquery_in_from(self):
        query = Select(
            targets=[Identifier("column1")],
            from_table=Select(
                targets=[Identifier("column1")], from_table=Identifier("tab"), alias=Identifier("subquery")
            ),
        )
        expected_plan = QueryPlan(
            integrations=["int"],
            default_namespace="int",
            steps=[
                FetchDataframeStep(
                    integration="int",
                    query=Select(
                        targets=[
                            Identifier("column1", alias=Identifier("column1")),
                        ],
                        from_table=Select(
                            targets=[Identifier("column1", alias=Identifier("column1"))],
                            from_table=Identifier("tab"),
                            alias=Identifier("subquery"),
                        ),
                    ),
                ),
            ],
        )

        plan = plan_query(query, integrations=["int"], default_namespace="int")

        assert plan.steps == expected_plan.steps

    def test_integration_select_3_level(self):
        sql = "select * from xxx.yyy.zzz where x > 1"
        query = parse_sql(sql)

        expected_plan = QueryPlan(
            integrations=["int"],
            default_namespace="xxx",
            steps=[
                FetchDataframeStep(
                    integration="xxx",
                    query=Select(
                        targets=[Star()],
                        from_table=Identifier("yyy.zzz"),
                        where=BinaryOperation(op=">", args=[Identifier("x"), Constant(1)]),
                    ),
                )
            ],
        )

        plan = plan_query(query, integrations=["xxx"])

        assert plan.steps == expected_plan.steps

    def test_native_query_no_sub_select(self):
        # Just select to integration
        sql = "select * from integration1 (select * from task_items)"
        query = parse_sql(sql)

        plan = plan_query(query, integrations=["integration1"])

        expected_plan = QueryPlan(
            default_namespace="integration1",
            steps=[
                FetchDataframeStep(integration="integration1", raw_query="select * from task_items"),
            ],
        )
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_native_query(self):
        # select on results after select to integration

        sql = "select date_trunc('m', last_date) from integration1 (select * from task_items  )  a limit 1"
        query = parse_sql(sql)

        plan = plan_query(query, integrations=["integration1"])

        expected_plan = QueryPlan(
            default_namespace="integration1",
            steps=[
                FetchDataframeStep(integration="integration1", raw_query="select * from task_items"),
                SubSelectStep(
                    dataframe=Result(0), query=parse_sql("select date_trunc('m', last_date) limit 1"), table_name="a"
                ),
            ],
        )
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_update_table(self):
        # select on results after select to integration

        sql = "update integration1.direct_messages set a=1 where b=2"
        query = parse_sql(sql)

        plan = plan_query(query, integrations=["integration1"])

        expected_plan = QueryPlan(
            default_namespace="integration1",
            steps=[
                UpdateToTable(dataframe=None, table=Identifier("integration1.direct_messages"), update_command=query),
            ],
        )
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_select_from_table_subselect(self):
        query = parse_sql(
            """
                        select * from int2.tab1
                        where x1 in (select id from int1.tab1)
                    """
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int1",
                    query=parse_sql("select id as id from tab1"),
                ),
                FetchDataframeStep(
                    integration="int2",
                    query=Select(
                        targets=[Star()],
                        from_table=Identifier("tab1"),
                        where=BinaryOperation(op="in", args=[Identifier(parts=["x1"]), Parameter(Result(0))]),
                    ),
                ),
            ],
        )

        plan = plan_query(
            query, integrations=["int1", "int2"], predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}]
        )

        assert plan.steps == expected_plan.steps

    def test_select_from_table_subselect_api_integration(self):
        query = parse_sql(
            """
                        select x from int1.tab2
                        where x1 in (select id from int1.tab1)
                        limit 1
                    """
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int1",
                    query=parse_sql("select `id` AS `id` from tab1"),
                ),
                SubSelectStep(dataframe=Result(0), query=parse_sql("select id"), table_name="tab1"),
                FetchDataframeStep(
                    integration="int1",
                    query=Select(
                        targets=[Identifier("x", alias=Identifier("x"))],
                        from_table=Identifier("tab2"),
                        where=BinaryOperation(op="in", args=[Identifier(parts=["x1"]), Parameter(Result(1))]),
                        limit=Constant(1),
                    ),
                ),
                SubSelectStep(dataframe=Result(2), query=parse_sql("select x"), table_name="tab2"),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "api", "type": "data"}],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_select_from_table_subselect_sql_integration(self):
        query = parse_sql(
            """
                    select * from int1.tab1
                    where x1 in (select id from int1.tab1)
                """
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int1",
                    query=parse_sql("select * from tab1 where x1 in (select id as id from tab1)"),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "sql", "type": "data"}],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_select_from_single_integration(self):
        sql_parsed = """
            with tab2 as (
              select * from int1.tabl2
            )
            select a from (
                select x from tab2
                union
                select y from int1.tab1
                where x1 in (select id from int1.tab1)
                limit 1
            )
        """

        sql_integration = """
            with tab2 as (
              select * from tabl2
            )
            select a as a from (
                select x as x from tab2
                union
                select y as y from tab1
                where x1 in (select id as id from tab1)
                limit 1
            )
        """
        query = parse_sql(sql_parsed)

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int1",
                    query=parse_sql(sql_integration),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "sql", "type": "data"}],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
            default_namespace="mindsdb",
        )

        assert plan.steps == expected_plan.steps

    def test_delete_from_table_subselect_api_integration(self):
        query = parse_sql(
            """
                        delete from int1.tab1
                        where x1 in (select id from int1.tab1)
                    """
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int1",
                    query=parse_sql("select `id` AS `id` from tab1"),
                ),
                SubSelectStep(dataframe=Result(0), query=parse_sql("select id"), table_name="tab1"),
                DeleteStep(
                    table=Identifier("int1.tab1"),
                    where=BinaryOperation(op="in", args=[Identifier(parts=["x1"]), Parameter(Result(1))]),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "api", "type": "data"}],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_delete_from_table_subselect_sql_integration(self):
        query = parse_sql(
            """
                        delete from int1.tab1
                        where x1 in (select id from int1.tab1)
                    """
        )

        subselect = parse_sql("select id as id from tab1")
        subselect.parentheses = True
        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                DeleteStep(
                    table=Identifier("int1.tab1"),
                    where=BinaryOperation(op="in", args=[Identifier(parts=["x1"]), subselect]),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "sql", "type": "data"}],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_delete_from_table_subselect_sql_different_integration(self):
        query = parse_sql(
            """
                        delete from int1.tab1
                        where x1 in (select id from int2.tab1)
                    """
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int2",
                    query=parse_sql("select id as id from tab1"),
                ),
                DeleteStep(
                    table=Identifier("int1.tab1"),
                    where=BinaryOperation(op="in", args=[Identifier(parts=["x1"]), Parameter(Result(0))]),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "api", "type": "data"}, "int2"],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_create_table(self):
        query = parse_sql(
            """
              CREATE or replace table int2.tab1 (
                id int8,
                data varchar
              )
            """
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                CreateTableStep(
                    table=Identifier("int2.tab1"),
                    columns=[
                        TableColumn(name="id", type="int8"),
                        TableColumn(name="data", type="varchar"),
                    ],
                    is_replace=True,
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "api", "type": "data"}, "int2"],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_select_with_user_functions(self):
        query = parse_sql(
            """
                    select my.fnc(a, 1) from int1.tab1
                    where x1 > my.fnc2(b)
                    order by x
                    limit 2
                """
        )

        sub_query = parse_sql("select my.fnc(a, 1) from tab1 where x1 > my.fnc2(b)")
        sub_query.from_table = None

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                FetchDataframeStep(
                    integration="int1",
                    query=parse_sql("select * from tab1 where 0=0 order by x limit 2"),
                ),
                SubSelectStep(dataframe=Result(0), query=sub_query, table_name="tab1"),
            ],
        )

        plan = plan_query(
            query,
            integrations=[{"name": "int1", "class_type": "sql", "type": "data"}],
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        assert plan.steps == expected_plan.steps

    def test_select_with_mindsdb_functions(self):
        for function in MINDSDB_SQL_FUNCTIONS:
            query = parse_sql(
                f"""
                    select {function}(a) from int1.tab1
                    order by x
                    limit 2
                """
            )

            sub_query = parse_sql(f"select {function}(a) from tab1")
            sub_query.from_table = None

            expected_plan = QueryPlan(
                predictor_namespace="mindsdb",
                steps=[
                    FetchDataframeStep(
                        integration="int1",
                        query=parse_sql("select * from tab1 order by x limit 2"),
                    ),
                    SubSelectStep(dataframe=Result(0), query=sub_query, table_name="tab1"),
                ],
            )

            plan = plan_query(
                query,
                integrations=[{"name": "int1", "class_type": "sql", "type": "data"}],
                predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
            )

            assert plan.steps == expected_plan.steps
