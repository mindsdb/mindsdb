import copy

from mindsdb_sql_parser.ast import Identifier, Select, Join, Constant, Data, BinaryOperation, Star
from mindsdb_sql_parser.utils import JoinType

from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.step_result import Result
from mindsdb.api.executor.planner.steps import DataStep, JoinStep, ApplyPredictorStep, SubSelectStep, QueryStep


class TestInjectedData:
    def test_select_from_table(self):
        content = [
            {"a": 1},
            {"a": 2},
        ]

        query = Select(
            targets=[Identifier("int1.t")],
            from_table=Data(content),
            where=BinaryOperation(op="=", args=[Identifier("a"), Constant(1)]),
        )

        plan = plan_query(query, integrations=["int1"], default_namespace="mindsdb", predictor_metadata=[])

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                DataStep(data=content),
                SubSelectStep(
                    query=Select(
                        targets=[Identifier("int1.t")],
                        where=BinaryOperation(op="=", args=[Identifier("a"), Constant(1)]),
                    ),
                    dataframe=Result(0),
                    table_name=None,
                    add_absent_cols=True,
                ),
            ],
        )

        assert plan.steps == expected_plan.steps

    def test_join(self):
        content = [
            {"a": 1},
            {"a": 2},
        ]

        query = Select(
            targets=[Identifier("t.x")],
            from_table=Join(left=Data(content, alias=Identifier("t")), right=Identifier("pred"), join_type="JOIN"),
            where=BinaryOperation(op="=", args=[Identifier("t.a"), Constant(1)]),
        )

        subquery = copy.deepcopy(query)
        subquery.from_table = None

        plan = plan_query(
            query,
            integrations=["int1"],
            default_namespace="mindsdb",
            predictor_metadata=[{"name": "pred", "integration_name": "mindsdb"}],
        )

        expected_plan = QueryPlan(
            predictor_namespace="mindsdb",
            steps=[
                DataStep(data=content),
                SubSelectStep(
                    query=Select(
                        targets=[Star()],  # No column pruning with predictor joins
                        where=BinaryOperation(op="=", args=[Identifier("a"), Constant(1)]),
                    ),
                    dataframe=Result(0),
                    table_name="t",
                    add_absent_cols=True,
                ),
                ApplyPredictorStep(namespace="mindsdb", dataframe=Result(1), predictor=Identifier("pred")),
                JoinStep(
                    left=Result(1),
                    right=Result(2),
                    query=Join(left=Identifier("tab1"), right=Identifier("tab2"), join_type=JoinType.JOIN),
                ),
                QueryStep(subquery, from_table=Result(3), strict_where=False),
            ],
        )

        assert plan.steps == expected_plan.steps
