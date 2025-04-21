from mindsdb_sql_parser.ast import (
    Identifier, Insert, Select, Constant,
    Star, BinaryOperation, Function,
)
import pandas as pd

from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.steps import (
    FetchDataframeStep,
    InsertToTable,
    QueryStep
)


class TestPlanInsertFromSelect:
    def test_insert_from_select_with_table_plan(self):
        query = Insert(
            table=Identifier('INT_1.table_1'),
            columns=None,
            from_select=Select(
                targets=[Star()],
                from_table=Identifier('INT_2.table_2'),
                where=None,
            )
        )
        plan = plan_query(query, integrations=['INT_1', 'INT_2'])

        step_1 = FetchDataframeStep(
            integration='int_2',
            query=Select(
                targets=[Star()],
                from_table=Identifier('table_2'),
                where=None,
            ),
            step_num=0,
        )
        expected_plan = QueryPlan(
            steps=[
                step_1,
                InsertToTable(
                    table=Identifier('INT_1.table_1'),
                    step_num=1,
                    dataframe=step_1
                )
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_insert_from_select_with_table_and_columns_plan(self):
        query = Insert(
            table=Identifier('INT_1.table_1'),
            from_select=Select(
                targets=[Identifier('column_1'), Identifier('column_2')],
                from_table=Identifier('INT_2.table_2'),
                where=None,
            )
        )
        plan = plan_query(query, integrations=['INT_1', 'INT_2'])

        step_1 = FetchDataframeStep(
            integration='int_2',
            query=Select(
                targets=[Identifier('column_1', alias=Identifier('column_1')), Identifier('column_2', alias=Identifier('column_2'))],
                from_table=Identifier('table_2'),
                where=None,
            ),
            step_num=0,
        )
        expected_plan = QueryPlan(
            steps=[
                step_1,
                InsertToTable(
                    table=Identifier('INT_1.table_1'),
                    step_num=1,
                    dataframe=step_1,
                )
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_insert_from_select_with_table_and_columns_and_where_plan(self):
        query = Insert(
            table=Identifier('INT_1.table_1'),
            from_select=Select(
                targets=[Identifier('column_1'), Identifier('column_2')],
                from_table=Identifier('INT_2.table_2'),
                where=BinaryOperation(
                    op='>',
                    args=[
                        Identifier('column_3', alias=Identifier('column_3')),
                        Constant(10),
                    ],
                ),
            )
        )
        plan = plan_query(query, integrations=['INT_1', 'INT_2'])

        step_1 = FetchDataframeStep(
            integration='int_2',
            query=Select(
                targets=[Identifier('column_1', alias=Identifier('column_1')), Identifier('column_2', alias=Identifier('column_2'))],
                from_table=Identifier('table_2'),
                where=BinaryOperation(
                    op='>',
                    args=[
                        Identifier('column_3', alias=Identifier('column_3')),
                        Constant(10),
                    ],
                )
            ),
            step_num=0,
        )
        expected_plan = QueryPlan(
            steps=[
                step_1,
                InsertToTable(
                    table=Identifier('INT_1.table_1'),
                    step_num=1,
                    dataframe=step_1,
                )
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_insert_from_select_without_table_plan(self):
        select_query = Select(
            targets=[Function('function', args=[])],
            from_table=None,
            where=None,
        )
        query = Insert(
            table=Identifier('INT_1.table_1'),
            from_select=select_query
        )

        plan = plan_query(query, integrations=['INT_1'])

        step_1 = QueryStep(
            query=select_query,
            step_num=0,
            from_table=pd.DataFrame([None]),
        )
        expected_plan = QueryPlan(
            steps=[
                step_1,
                InsertToTable(
                    table=Identifier('INT_1.table_1'),
                    step_num=1,
                    dataframe=step_1
                )
            ]
        )
        for i in range(len(plan.steps)):
            step = plan.steps[i]
            expected_step = expected_plan.steps[i]

            if hasattr(step, 'from_table') and isinstance(step.from_table, pd.DataFrame) and isinstance(expected_step.from_table, pd.DataFrame):
                assert step.from_table.equals(expected_step.from_table)
            elif hasattr(step, 'dataframe') and isinstance(step.dataframe.from_table, pd.DataFrame) and isinstance(expected_step.dataframe.from_table, pd.DataFrame):
                assert step.dataframe.from_table.equals(expected_step.dataframe.from_table)
            else:
                assert step == expected_step
