import json
import torch.multiprocessing as mp
from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.interfaces.state.state import State
from mindsdb.interfaces.state.config import Config

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
        print('\n\n0\n\n')
        name, from_data, to_predict, kwargs, trx_type = self._args

        print('\n\nCREATING THE CONFIG !\n\n')
        config = Config()
        print('\n\CREATED THE CONFIG !\n\n')
        state = State(config)
        print('\n\CREATED THE STATE !\n\n')
        print('\n\n2\n\n')
        mdb = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})
        print('\n\n3\n\n')
        if trx_type == 'learn':
            to_predict = to_predict if isinstance(to_predict, list) else [to_predict]
            state.make_predicotr(name, None, to_predict)
            data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
            print('\n\n3\n\n')
            mdb.learn(
                from_data=data_source,
                to_predict=to_predict,
                **kwargs
            )
            print('\n\n4\n\n')
            analysis = mindsdb_native.F.get_model_data(name)
            stats = analysis['data_analysis_v2']
            status = analysis['status']
            print('\n\n5\n\n')
            state.update_predictor(name=name, status=status, original_path=None, data=json.dumps(stats))
            print('\n\n6\n\n')
        print('\n\n7\n\n')

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
