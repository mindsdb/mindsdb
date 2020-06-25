import torch.multiprocessing as mp


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
        import sys
        import mindsdb_native

        from mindsdb.utilities.config import Config

        name, from_data, to_predict, kwargs, config = self._args
        config = Config(config)

        mdb = mindsdb_native.Predictor(name=name)

        data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])

        mdb.learn(
            from_data=data_source,
            to_predict=to_predict,
            use_gpu=config.get('use_gpu', False),
            **kwargs
        )

        stats = mdb.get_model_data()['data_analysis_v2']

        try:
            assert(config['integrations']['default_clickhouse']['enabled'] == True)
            from mindsdb.interfaces.clickhouse.clickhouse import Clickhouse
            clickhouse = Clickhouse(config)
            clickhouse.register_predictor(name, stats)
        except:
            pass

        try:
            assert(config['integrations']['default_mariadb']['enabled'] == True)
            from mindsdb.interfaces.mariadb.mariadb import Mariadb
            mariadb = Mariadb(config)
            mariadb.register_predictor(name, stats)
        except:
            pass
