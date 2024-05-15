import datetime
import pandas as pd
from unit.executor_test_base import BaseExecutorDummyML


class TestHandlerMetrics(BaseExecutorDummyML):
    def test_handler_query_time(self):
        self.set_data('tasks', pd.DataFrame([
            {'a': 1, 'b': datetime.datetime(2020, 1, 1)},
            {'a': 2, 'b': datetime.datetime(2020, 1, 2)},
            {'a': 1, 'b': datetime.datetime(2020, 1, 3)},
        ]))
        # Create & predict a simple model.
        self.run_sql('create database proj')
        self.run_sql(
            '''
                CREATE model proj.task_model
                from dummy_data (select * from tasks)
                PREDICT a
                using engine='dummy_ml',
                tag = 'first',
                join_learn_process=true
            '''
        )
        self.wait_predictor('proj', 'task_model')
        self.run_sql('''
             SELECT m.*
               FROM dummy_data.tasks as t
               JOIN proj.task_model as m
        ''')
        # Import here so we don't reuse registry across test functions.
        from mindsdb.metrics import metrics
        query_time_metric = list(metrics.INTEGRATION_HANDLER_QUERY_TIME.collect())[0]
        query_size_metric = list(metrics.INTEGRATION_HANDLER_RESPONSE_SIZE.collect())[0]
        assert len(query_time_metric.samples) == 3
        assert len(query_size_metric.samples) == 3
        for sample in query_time_metric.samples:
            assert sample.name.startswith('mindsdb_integration_handler_query_seconds')
            if sample.name.endswith('count'):
                assert sample.value == 1.0
            elif sample.name.endswith('sum'):
                assert sample.value > 0.0
            elif sample.name.endswith('created'):
                assert sample.value > 0.0
        for sample in query_size_metric.samples:
            assert sample.name.startswith('mindsdb_integration_handler_response_size')
            if sample.name.endswith('count'):
                assert sample.value == 1.0
            elif sample.name.endswith('sum'):
                assert sample.value > 0.0
            elif sample.name.endswith('created'):
                assert sample.value > 0.0
