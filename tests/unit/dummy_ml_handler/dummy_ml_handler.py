from mindsdb.integrations.libs.base import BaseMLEngine


class DummyHandler(BaseMLEngine):
    name = 'dummy_ml'

    def create(self, target, args=None, **kwargs):
        pass

    def predict(self, df, args=None):
        df['predicted'] = 42
        df['predictor_id'] = self.model_storage.predictor_id
        return df[['predicted', 'predictor_id']]
