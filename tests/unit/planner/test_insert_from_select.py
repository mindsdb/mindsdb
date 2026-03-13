from mindsdb_sql_parser import parse_sql, Join
from mindsdb_sql_parser.ast import (
    Identifier,
    Insert,
    Select,
    Constant,
    Star,
    BinaryOperation,
    Function,
)
import pandas as pd

from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.step_result import Result
from mindsdb.api.executor.planner.steps import (
    FetchDataframeStep,
    InsertToTable,
    QueryStep,
    FetchDataframeStepPartition,
    JoinStep,
    ApplyPredictorStep,
)
from mindsdb_sql_parser.utils import JoinType


class TestPlanInsertFromSelect:
    def test_insert_from_select_with_table_plan(self):
        query = Insert(
            table=Identifier("INT_1.table_1"),
            columns=None,
            from_select=Select(
                targets=[Star()],
                from_table=Identifier("INT_2.table_2"),
                where=None,
            ),
        )
        plan = plan_query(query, integrations=["INT_1", "INT_2"])

        step_1 = FetchDataframeStep(
            integration="INT_2",
            query=Select(
                targets=[Star()],
                from_table=Identifier("table_2"),
                where=None,
            ),
            step_num=0,
        )
        expected_plan = QueryPlan(
            steps=[step_1, InsertToTable(table=Identifier("INT_1.table_1"), step_num=1, dataframe=Result(0))]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_insert_from_select_with_table_and_columns_plan(self):
        query = Insert(
            table=Identifier("INT_1.table_1"),
            from_select=Select(
                targets=[Identifier("column_1"), Identifier("column_2")],
                from_table=Identifier("INT_2.table_2"),
                where=None,
            ),
        )
        plan = plan_query(query, integrations=["INT_1", "INT_2"])

        step_1 = FetchDataframeStep(
            integration="INT_2",
            query=Select(
                targets=[
                    Identifier("column_1", alias=Identifier("column_1")),
                    Identifier("column_2", alias=Identifier("column_2")),
                ],
                from_table=Identifier("table_2"),
                where=None,
            ),
            step_num=0,
        )
        expected_plan = QueryPlan(
            steps=[
                step_1,
                InsertToTable(
                    table=Identifier("INT_1.table_1"),
                    step_num=1,
                    dataframe=Result(0),
                ),
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_insert_from_select_with_table_and_columns_and_where_plan(self):
        query = Insert(
            table=Identifier("INT_1.table_1"),
            from_select=Select(
                targets=[Identifier("column_1"), Identifier("column_2")],
                from_table=Identifier("INT_2.table_2"),
                where=BinaryOperation(
                    op=">",
                    args=[
                        Identifier("column_3", alias=Identifier("column_3")),
                        Constant(10),
                    ],
                ),
            ),
        )
        plan = plan_query(query, integrations=["int_1", "int_2"])

        step_1 = FetchDataframeStep(
            integration="int_2",
            query=Select(
                targets=[
                    Identifier("column_1", alias=Identifier("column_1")),
                    Identifier("column_2", alias=Identifier("column_2")),
                ],
                from_table=Identifier("table_2"),
                where=BinaryOperation(
                    op=">",
                    args=[
                        Identifier("column_3", alias=Identifier("column_3")),
                        Constant(10),
                    ],
                ),
            ),
            step_num=0,
        )
        expected_plan = QueryPlan(
            steps=[
                step_1,
                InsertToTable(
                    table=Identifier("INT_1.table_1"),
                    step_num=1,
                    dataframe=Result(0),
                ),
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_insert_from_select_without_table_plan(self):
        select_query = Select(
            targets=[Function("function", args=[])],
            from_table=None,
            where=None,
        )
        query = Insert(table=Identifier("INT_1.table_1"), from_select=select_query)

        plan = plan_query(query, integrations=["INT_1"])

        step_1 = QueryStep(
            query=select_query,
            step_num=0,
            from_table=pd.DataFrame([None]),
        )
        expected_plan = QueryPlan(
            steps=[step_1, InsertToTable(table=Identifier("INT_1.table_1"), step_num=1, dataframe=Result(0))]
        )
        for i in range(len(plan.steps)):
            step = plan.steps[i]
            expected_step = expected_plan.steps[i]

            if (
                hasattr(step, "from_table")
                and isinstance(step.from_table, pd.DataFrame)
                and isinstance(expected_step.from_table, pd.DataFrame)
            ):
                assert step.from_table.equals(expected_step.from_table)
            else:
                assert step == expected_step


class TestPartitions:
    def test_insert_from_select(self):
        query = parse_sql("""
            insert into int2.table2
            select id from int1.table1
            using track_column = id, batch_size=100
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStepPartition(
                    step_num=0,
                    integration="int1",
                    query=parse_sql("select id as id from table1"),
                    params={"batch_size": 100, "track_column": "id"},
                    steps=[InsertToTable(table=Identifier("int2.table2"), step_num=1, dataframe=Result(0))],
                )
            ],
        )

        assert plan.steps == expected_plan.steps

    def test_insert_from_join(self):
        query = parse_sql("""
            insert into int2.table2
            ( select a, b from int1.table1
              join int1.table3 )
            using track_column = id, batch_size=100
        """)

        plan = plan_query(query, integrations=["int1", "int2"])

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStepPartition(
                    step_num=0,
                    integration="int1",
                    query=Select(
                        targets=[Identifier("a"), Identifier("b")],
                        from_table=Join(left=Identifier("table1"), right=Identifier("table3"), join_type="join"),
                        using={},
                    ),
                    params={"track_column": "id", "batch_size": 100},
                    steps=[InsertToTable(table=Identifier("int2.table2"), step_num=1, dataframe=Result(0))],
                )
            ],
        )

        assert plan.steps == expected_plan.steps

    def test_select_join_model(self):
        query = parse_sql("""
            select id from int1.table1
            join pred
            using track_column = id, batch_size=100
        """)

        plan = plan_query(
            query,
            integrations=["int1", "int2"],
            default_namespace="mindsdb",
            predictor_metadata=[
                {"name": "pred", "integration_name": "mindsdb", "to_predict": ["ttt"]},
            ],
        )

        expected_plan = QueryPlan(
            integrations=["int"],
            steps=[
                FetchDataframeStepPartition(
                    step_num=0,
                    integration="int1",
                    query=parse_sql("select * from table1"),
                    params={"batch_size": 100, "track_column": "id"},
                    steps=[
                        ApplyPredictorStep(
                            step_num=1,
                            namespace="mindsdb",
                            dataframe=Result(0),
                            params={},
                            predictor=Identifier("pred"),
                        ),
                        JoinStep(
                            step_num=2,
                            left=Result(0),
                            right=Result(1),
                            query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                        ),
                        QueryStep(step_num=3, query=parse_sql("select id"), from_table=Result(2), strict_where=False),
                    ],
                )
            ],
        )

        assert plan.steps == expected_plan.steps
