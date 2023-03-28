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
        pass

    def predict(self, df, args=None):
        df['predicted'] = 42
        df['predictor_id'] = self.model_storage.predictor_id
        return df[['predicted', 'predictor_id']]
