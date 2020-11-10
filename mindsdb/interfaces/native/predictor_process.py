import torch.multiprocessing as mp
from mindsdb.interfaces.database.database import DatabaseWrapper


ctx = mp.get_context('spawn')


class PredictorProcess(ctx.Process):
    daemon = True

    def __init__(self, *args):
        super(PredictorProcess, self).__init__(args=args)

    def run(self):
        '''
        running at subprocess due to
        ValueError: signal only works in main thread

        this is work for celery worker here?
        '''
        import mindsdb_native

        name, from_data, to_predict, kwargs, config, trx_type = self._args

        mdb = mindsdb_native.Predictor(name=name)

        if trx_type == 'learn':
            to_predict = to_predict if isinstance(to_predict, list) else [to_predict]
            data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
            mdb.learn(
                from_data=data_source,
                to_predict=to_predict,
                **kwargs
            )

            stats = mindsdb_native.F.get_model_data(name)['data_analysis_v2']

            DatabaseWrapper(config).register_predictors([{
                'name': name,
                'predict': to_predict,
                'data_analysis': stats
            }], setup=False)

        if trx_type == 'predict':
            if isinstance(from_data, dict):
                when_data = from_data
            else:
                when_data = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])

            predictions = mdb.predict(
                when_data=when_data,
                **kwargs
            )

            # @TODO Figure out a way to recover this since we are using `spawn` here... simple Queue or instiating a Multiprocessing manager and registering a value in a dict using that. Or using map from a multiprocessing pool with 1x process (though using a custom process there might be it's own bucket of annoying)
            return predictions
