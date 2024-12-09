import inspect

from mindsdb.api.executor.planner import query_planner
from mindsdb.api.executor.planner import steps

from tests.unit.planner import test_integration_select
from tests.unit.planner import test_join_predictor
from tests.unit.planner import test_join_tables
from tests.unit.planner import test_plan_union
from tests.unit.planner import test_select_from_predictor
from tests.unit.planner import test_ts_predictor


class FakeExecutor:

    def list_cols_return(self, table_name, columns):

        table_alias = ('int', table_name, table_name)
        data = {
            'values': [],
            'columns': {
                table_alias: columns
            },
            'tables': [table_alias]
        }
        return data

    def execute(self, step):

        if (
                isinstance(step, steps.ProjectStep)
                or isinstance(step, steps.FetchDataframeStep)
                or isinstance(step, steps.UnionStep)
        ):
            return [
                {'id': 1, 'name': 'asdf'},
                {'id': 2, 'name': 'jkl;'}
            ]
        if isinstance(step, steps.GetTableColumns):
            if step.table in ('tab', 'tab1', 'tab2', 'tab3', 'data.ny_output', 'data', 'yyy.zzz', 'sweat', 'schem.sweat', 'predictors', 'v1'):
                cols = [
                    {'name': 'id', 'type': 'int'},
                    {'name': 'name', 'type': 'str'},
                    {'name': 'a column with spaces', 'type': 'str'},
                    {'name': 'column1', 'type': 'str'},
                    {'name': 'column2', 'type': 'str'},
                    {'name': 'column3', 'type': 'str'},
                    {'name': 'col1', 'type': 'str'},
                    {'name': 'asset', 'type': 'float'},
                    {'name': 'time', 'type': 'datetime'},
                    {'name': 'predicted', 'type': 'float'},
                    {'name': 'target', 'type': 'float'},
                    {'name': 'sqft', 'type': 'float'},
                    {'name': 'x', 'type': 'int'},
                ]
                return self.list_cols_return(step.table, cols)
            return None
        if isinstance(step, steps.GetPredictorColumns):
            name = step.predictor.parts[-1]
            if name.isdigit():
                name = step.predictor.parts[-2]

            if name in ('pred', 'tp3', 'pr', 'embedding_model'):
                cols = [
                    {'name': 'id', 'type': 'int'},
                    {'name': 'value', 'type': 'str'},
                    {'name': 'predicted', 'type': 'int'},
                    {'name': 'x1', 'type': 'int'},
                    {'name': 'x2', 'type': 'int'},
                    {'name': 'y', 'type': 'int'},
                    {'name': 'time', 'type': 'datetime'},
                    {'name': 'price', 'type': 'float'},
                    {'name': 'target', 'type': 'float'},
                ]
                name = step.predictor.parts[-1]
                return self.list_cols_return(name, cols)
        else:
            return None


executor = FakeExecutor()


def plan_query_patch(query, **kwargs):

    plan = query_planner.QueryPlanner(**kwargs)

    steps = []
    # get prepared statement
    for step in plan.prepare_steps(query):
        result = executor.execute(step)
        step.set_result(result)

        # not include prepared steps yet
        # steps.append(step)

    # print(plan.get_statement_info()) # raises if prepare_steps doesn't execute

    params = []
    for step in plan.execute_steps(params):
        result = executor.execute(step)
        step.set_result(result)
        steps.append(step)

    # plan.fetch(10)
    plan.steps = steps
    return plan


test_integration_select.plan_query = plan_query_patch
test_join_predictor.plan_query = plan_query_patch
test_join_tables.plan_query = plan_query_patch
test_plan_union.plan_query = plan_query_patch
test_select_from_predictor.plan_query = plan_query_patch
test_ts_predictor.plan_query = plan_query_patch


class TestPreparedStatement:

    def test_from_planner_tests(self):

        for module in (test_integration_select, test_join_predictor, test_join_tables,
                       test_plan_union, test_select_from_predictor, test_ts_predictor):

            for class_name, klass in inspect.getmembers(module, predicate=inspect.isclass):
                if not class_name.startswith('Test'):
                    continue

                tests = klass()
                for test_name, test_method in inspect.getmembers(tests, predicate=inspect.ismethod):
                    if not test_name.startswith('test_') or test_name.endswith('_error'):
                        continue
                    if test_name in ('test_native_query_no_sub_select',):
                        # skipped tests
                        continue
                    try:
                        test_method()
                    except query_planner.PlanningException as e:

                        if str(e) == 'Predictor must be last table in query':
                            # TODO replace tables in tests: to predictor as left table
                            pass
                        elif str(e) == 'Predictor is not at first level':
                            # TODO make prepared statement planner more sophisticated
                            pass
                        else:
                            raise e
