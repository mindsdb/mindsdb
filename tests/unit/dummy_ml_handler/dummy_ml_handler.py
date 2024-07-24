import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine


class DummyHandler(BaseMLEngine):
    name = 'dummy_ml'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if args is not None:
            args['target'] = target
        if 'error' in args.get('using', {}):
            raise RuntimeError()

    def create(self, target, args=None, **kwargs):
        self.model_storage.json_set('args', args['using'])

    def predict(self, df, args=None):
        df['predicted'] = 42
        df['predictor_id'] = self.model_storage.predictor_id
        df['row_id'] = self.model_storage.predictor_id * 100 + df.reset_index().index

        output_columns = ['predicted', 'predictor_id', 'row_id', 'engine_args']

        if 'engine_args' in df.columns:
            # could exist from previous model
            df = df.drop('engine_args', axis=1)
            print(1)

        model_args = self.model_storage.json_get('args')
        engine_args = self.engine_storage.json_get('engine_args')

        # check input
        if 'output' in model_args:
            df['output'] = model_args['output']
            output_columns.append('output')
        if 'input' in df.columns:
            df['output'] = df['input']
            output_columns.append('output')

        df.insert(len(df.columns), 'engine_args', [engine_args] * len(df))

        return df[output_columns]

    def _get_model_verison(self):
        return self.model_storage._get_model_record(
            self.model_storage.predictor_id
        ).version

    def describe(self, attribute=None):
        if attribute == 'info':
            return pd.DataFrame(
                [['dummy', self._get_model_verison()]],
                columns=['type', 'version']
            )
        elif isinstance(attribute, list):
            return pd.DataFrame(
                [['.'.join(attribute), self._get_model_verison()]],
                columns=['attribute', 'version']
            )
        else:
            tables = ['info']
            return pd.DataFrame(tables, columns=['tables'])

    def create_engine(self, connection_args):
        self.engine_storage.json_set('engine_args', connection_args)
