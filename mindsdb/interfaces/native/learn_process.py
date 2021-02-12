import torch.multiprocessing as mp
from mindsdb.interfaces.database.database import DatabaseWrapper


ctx = mp.get_context('spawn')


class LearnProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(LearnProcess, self).__init__(args=args)

    def run(self):
        '''
        running at subprocess due to
        ValueError: signal only works in main thread

        this is work for celery worker here?
        '''
        import mindsdb_native

        name, from_data, to_predict, kwargs, config = self._args
        mdb = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})

        to_predict = to_predict if isinstance(to_predict, list) else [to_predict]
        data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
        mdb.learn(
            from_data=data_source,
            to_predict=to_predict,
            **kwargs
        )

        model_data = mindsdb_native.F.get_model_data(name)

        DatabaseWrapper(config).register_predictors([model_data])
