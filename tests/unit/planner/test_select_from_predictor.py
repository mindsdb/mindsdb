import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import (Identifier, Select, Constant, Star, Parameter, BinaryOperation)

from mindsdb.api.executor.planner.exceptions import PlanningException
from mindsdb.api.executor.planner import plan_query
from mindsdb.api.executor.planner.query_plan import QueryPlan
from mindsdb.api.executor.planner.step_result import Result
from mindsdb.api.executor.planner.steps import (
    ProjectStep, ApplyPredictorRowStep, GetPredictorColumns, FetchDataframeStep
)


class TestPlanSelectFromPredictor:
    def test_select_from_predictor_plan(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('mindsdb.pred'),
            where=BinaryOperation(
                op='and',
                args=[BinaryOperation(op='=', args=[Identifier('x1'), Constant(1)]),
                      BinaryOperation(op='=', args=[Identifier('x2'), Constant('2')])],
            )
        )
        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mindsdb', predictor=Identifier('pred'),
                    row_dict={'x1': 1, 'x2': '2'}
                ),
            ],

        )

        plan = plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_negative_constant(self):
        query = parse_sql(
            '''
                        select * from mindsdb.pred
                        where x1 = -1
                    '''
        )

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(namespace='mindsdb', predictor=Identifier('pred'), row_dict={'x1': -1, }),
            ],
        )

        plan = plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_plan_other_ml(self):
        query = parse_sql(
            '''
                        select * from mlflow.pred
                        where x1 = 1 and x2 = '2'
                    '''
        )

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mlflow', predictor=Identifier('pred'),
                    row_dict={'x1': 1, 'x2': '2'}
                ),
            ],

        )

        plan = plan_query(query, predictor_metadata=[{'name': 'pred', 'integration_name': 'mlflow'}])

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_aliases_in_project(self):
        query = Select(
            targets=[Identifier('tb.x1', alias=Identifier('col1')),
                     Identifier('tb.x2', alias=Identifier('col2')),
                     Identifier('tb.y', alias=Identifier('predicted'))],
            from_table=Identifier('mindsdb.pred', alias=Identifier('tb')),
            where=BinaryOperation(
                op='and',
                args=[
                    BinaryOperation(op='=', args=[Identifier('tb.x1'), Constant(1)]),
                    BinaryOperation(op='=', args=[Identifier('tb.x2'), Constant('2')]),
                ],
            )
        )
        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mindsdb',
                    predictor=Identifier('pred', alias=Identifier('tb')),
                    row_dict={'x1': 1, 'x2': '2'}
                ),
                ProjectStep(
                    dataframe=Result(0),
                    columns=[Identifier('tb.x1', alias=Identifier('col1')),
                             Identifier('tb.x2', alias=Identifier('col2')),
                             Identifier('tb.y', alias=Identifier('predicted'))]
                ),
            ],

        )

        plan = plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_plan_predictor_alias(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('mindsdb.pred', alias=Identifier('pred_alias')),
            where=BinaryOperation(
                op='and',
                args=[
                    BinaryOperation(op='=', args=[Identifier('pred_alias.x1'), Constant(1)]),
                    BinaryOperation(
                        op='=',
                        args=[Identifier('pred_alias.x2'), Constant('2')]
                    )
                ],
            )
        )
        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mindsdb', predictor=Identifier('pred', alias=Identifier('pred_alias')),
                    row_dict={'x1': 1, 'x2': '2'}
                ),
            ],
        )

        plan = plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_plan_verbose_col_names(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('mindsdb.pred'),
            where=BinaryOperation(
                op='and',
                args=[BinaryOperation(op='=', args=[Identifier('pred.x1'), Constant(1)]),
                      BinaryOperation(op='=', args=[Identifier('pred.x2'), Constant('2')])],
            )
        )
        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mindsdb', predictor=Identifier('pred'),
                    row_dict={'x1': 1, 'x2': '2'}
                ),
                ProjectStep(dataframe=Result(0), columns=[Star()]),
            ],
        )

        plan = plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_select_from_predictor_plan_group_by_error(self):
        query = Select(
            targets=[Identifier('x1'), Identifier('x2'), Identifier('pred.y')],
            from_table=Identifier('mindsdb.pred'),
            group_by=[Identifier('x1')]
        )
        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

    def test_select_from_predictor_wrong_where_op_error(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('mindsdb.pred'),
            where=BinaryOperation(
                op='and',
                args=[BinaryOperation(op='>', args=[Identifier('x1'), Constant(1)]),
                      BinaryOperation(op='=', args=[Identifier('x2'), Constant('2')])],
            )
        )

        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

    def test_select_from_predictor_multiple_values_error(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('mindsdb.pred'),
            where=BinaryOperation(
                op='and',
                args=[BinaryOperation(op='=', args=[Identifier('x1'), Constant(1)]),
                      BinaryOperation(op='=', args=[Identifier('x1'), Constant('2')])],
            )
        )

        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

    def test_select_from_predictor_no_where_error(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('mindsdb.pred')
        )

        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

    def test_select_from_predictor_default_namespace(self):
        query = Select(
            targets=[Star()],
            from_table=Identifier('pred'),
            where=BinaryOperation(
                op='and',
                args=[BinaryOperation(op='=', args=[Identifier('x1'), Constant(1)]),
                      BinaryOperation(op='=', args=[Identifier('x2'), Constant('2')])],
            )
        )
        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            default_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mindsdb', predictor=Identifier('pred'),
                    row_dict={'x1': 1, 'x2': '2'}
                ),
            ],
        )

        plan = plan_query(
            query, predictor_namespace='mindsdb', default_namespace='mindsdb', predictor_metadata={'pred': {}}
        )

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_get_columns(self):
        sql = 'SELECT GDP_per_capita_USD FROM hdi_predictor_external WHERE 1 = 0'
        query = parse_sql(sql, dialect='mindsdb')

        expected_query = Select(
            targets=[Identifier('GDP_per_capita_USD')],
            from_table=Identifier('hdi_predictor_external'),
            where=BinaryOperation(
                op="=",
                args=[Constant(1), Constant(0)]
            )
        )
        assert query.to_tree() == expected_query.to_tree()

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            default_namespace='mindsdb',
            steps=[
                GetPredictorColumns(
                    namespace='mindsdb',
                    predictor=Identifier('hdi_predictor_external')
                ),
                ProjectStep(dataframe=Result(0), columns=[Identifier('GDP_per_capita_USD')]),
            ],
        )

        plan = plan_query(
            query, predictor_namespace='mindsdb', default_namespace='mindsdb',
            predictor_metadata={'hdi_predictor_external': {}}
        )

        assert plan.steps == expected_plan.steps

    def test_using_predictor_version(self):
        query = parse_sql(
            '''
                        select * from mindsdb.pred.21
                        where x1 = 1
                    ''', dialect='mindsdb'
        )

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                ApplyPredictorRowStep(
                    namespace='mindsdb', predictor=Identifier(parts=['pred', '21']),
                    row_dict={'x1': 1}
                )
            ],
        )

        plan = plan_query(query, predictor_metadata=[{'name': 'pred', 'integration_name': 'mindsdb'}])

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_subselect(self):
        query = parse_sql(
            '''
                        select * from mindsdb.pred.21
                        where x1 = (select id from int1.t1)
                    ''', dialect='mindsdb'
        )

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                FetchDataframeStep(
                    integration='int1',
                    query=parse_sql('select id as id from t1'),
                ),
                ApplyPredictorRowStep(
                    namespace='mindsdb',
                    predictor=Identifier(parts=['pred', '21']),
                    row_dict={'x1': Parameter(Result(0))}
                )
            ],
        )

        plan = plan_query(
            query,
            integrations=['int1'],
            predictor_metadata=[{'name': 'pred', 'integration_name': 'mindsdb'}]
        )

        assert plan.steps == expected_plan.steps

    def test_select_from_view_subselect(self):
        query = parse_sql(
            '''
                        select * from v1
                        where x1 in (select id from int1.tab1)
                    ''', dialect='mindsdb'
        )

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                FetchDataframeStep(
                    integration='int1',
                    query=parse_sql('select id as id from tab1'),
                ),
                FetchDataframeStep(
                    integration='mindsdb',
                    query=Select(
                        targets=[Star()],
                        from_table=Identifier('v1'),
                        where=BinaryOperation(
                            op='in',
                            args=[
                                Identifier(parts=['x1']),
                                Parameter(Result(0))
                            ]
                        )
                    ),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=['int1'],
            default_namespace='mindsdb',
            predictor_metadata=[{'name': 'pred', 'integration_name': 'mindsdb'}]
        )

        assert plan.steps == expected_plan.steps

    def test_select_from_view_subselect_view(self):
        query = parse_sql(
            '''
                        select * from v1
                        where x1 in (select v2.id from v2)
                    ''', dialect='mindsdb'
        )

        expected_plan = QueryPlan(
            predictor_namespace='mindsdb',
            steps=[
                FetchDataframeStep(
                    integration='mindsdb',
                    query=parse_sql('select v2.id as id from v2'),
                ),
                FetchDataframeStep(
                    integration='mindsdb',
                    query=Select(
                        targets=[Star()],
                        from_table=Identifier('v1'),
                        where=BinaryOperation(
                            op='in',
                            args=[
                                Identifier(parts=['x1']),
                                Parameter(Result(0))
                            ]
                        )
                    ),
                ),
            ],
        )

        plan = plan_query(
            query,
            integrations=[],
            default_namespace='mindsdb',
            predictor_metadata=[]
        )

        assert plan.steps == expected_plan.steps


class TestMLSelect:

    def test_select_from_predictor_plan_other_ml(self):
        # sends to integrations
        query = parse_sql(''' select * from mlflow.predictors ''')

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(step_num=0, integration='mlflow', query=parse_sql('SELECT * FROM predictors'))
            ],
        )

        plan = plan_query(query, predictor_metadata=[], integrations=['mlflow'])

        assert plan.steps == expected_plan.steps


class TestNestedSelect:

    def test_using_predictor_in_subselect(self):
        """
        Use predictor in subselect when selecting from integration
        """
        sql = """
        SELECT *
        FROM chromadb.test_tabl
        WHERE
            search_vector = (
                SELECT emebddings
                FROM mindsdb.embedding_model
                WHERE
                    content = 'some text'
            )
        """
        ast_tree = parse_sql(sql)
        plan = plan_query(
            ast_tree,
            integrations=['chromadb'],
            predictor_metadata=[
                {'name': 'embedding_model', 'integration_name': 'mindsdb'}
            ]
        )

        expected_plan = [
            ApplyPredictorRowStep(
                step_num=0,
                namespace='mindsdb',
                predictor=Identifier(parts=['embedding_model']),
                row_dict={'content': 'some text'}
            ),
            ProjectStep(
                step_num=1,
                dataframe=Result(0),
                columns=[Identifier(parts=['emebddings'])]
            ),
            FetchDataframeStep(
                step_num=2,
                integration='chromadb',
                query=Select(
                    targets=[Star()],
                    from_table=Identifier(parts=['test_tabl']),
                    where=BinaryOperation(
                        op='=',
                        args=[
                            Identifier(parts=['search_vector']),
                            Parameter(Result(1))
                        ]
                    )
                ),
            ),
        ]

        assert plan.steps == expected_plan

    def test_using_integration_in_subselect(self):
        """
        Use integration in subselect when selecting from predictor
        """
        sql = """

        SELECT *
        FROM mindsdb.embedding_model
        WHERE
            content = (
                SELECT content
                FROM chromadb.test_tabl
                LIMIT 1
            )
        """
        ast_tree = parse_sql(sql)
        plan = plan_query(
            ast_tree,
            integrations=['chromadb'],
            predictor_metadata=[
                {'name': 'embedding_model', 'integration_name': 'mindsdb'}
            ]
        )

        expected_plan = [
            FetchDataframeStep(
                step_num=0,
                integration='chromadb',
                query=parse_sql('SELECT content AS content FROM test_tabl LIMIT 1')
            ),
            ApplyPredictorRowStep(
                step_num=1,
                namespace='mindsdb',
                predictor=Identifier(parts=['embedding_model']),
                row_dict={'content': Parameter(Result(0))}
            )
        ]

        assert plan.steps == expected_plan
