from unittest.mock import patch

import pandas as pd
from type_infer.infer import dtype

from mindsdb_sql import parse_sql


from .executor_test_base import BaseExecutorMockPredictor


class Test(BaseExecutorMockPredictor):

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_use_predictor_params(self, mock_handler):
        # set integration data

        df = pd.DataFrame([
            {'a': 1, 'b': 'one'},
            {'a': 2, 'b': 'two'},
            {'a': 1, 'b': 'three'},
        ])
        self.set_handler(mock_handler, name='pg', tables={'tasks': df})

        # --- use predictor ---
        predicted_value = 3.14
        predictor = {
            'name': 'task_model',
            'predict': 'p',
            'dtypes': {
                'p': dtype.float,
                'a': dtype.integer,
                'b': dtype.categorical
            },
            'predicted_value': predicted_value
        }
        self.set_predictor(predictor)

        # --- join table ---

        ret = self.command_executor.execute_command(parse_sql('''
           select m.p, v.a
           from pg.tasks v
           join mindsdb.task_model m
           where v.a = 2
           using p1='a', p2={'x':1, 'y':2}
        ''', dialect='mindsdb'))
        assert ret.error_code is None

        # check predictor input
        predict_args = self.mock_predict.call_args[0][1]
        assert predict_args['predict_params'] == {'p1': 'a', 'p2': {'x': 1, 'y': 2}}

        # --- inline prediction ---
        self.mock_predict.reset_mock()

        ret = self.command_executor.execute_command(parse_sql(f'''
            select * from mindsdb.task_model where a = 2
            using p1=1, p2=[1,2] 
        ''', dialect='mindsdb'))

        predict_args = self.mock_predict.call_args[0][1]
        assert predict_args['predict_params'] == {'p1': 1, 'p2': [1,2]}
