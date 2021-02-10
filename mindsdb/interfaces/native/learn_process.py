import torch.multiprocessing as mp

from mindsdb.interfaces.database.database import DatabaseWrapper
from mindsdb.utilities.os_specific import get_mp_context
from mindsdb.interfaces.storage.db import session, Predictor
from mindsdb.interfaces.storage.fs import FsSotre
from mindsdb.interfaces.datastore.datastore import DataStore

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

        fs_store = FsSotre()
        datastore = DataStore()
        company_id = os.environ.get('MINDSDB_COMPANY_ID', None)

        mdb = mindsdb_native.Predictor(name=name, run_env={'trigger': 'mindsdb'})

        predictor_record = Predictor.query.filter_by(company_id=company_id, name=name)
        name, from_data, to_predict, kwargs, _ = self._args
        predictor_record.to_predict = to_predict
        predictor_record.version = mindsdb_native.__version__
        predictor_record.data = mindsdb_native.F.get_model_data(name)
        #predictor_record.datasource_id = ... <-- can be done once `learn` is passed a datasource name
        session.commit()

        to_predict = to_predict if isinstance(to_predict, list) else [to_predict]
        data_source = getattr(mindsdb_native, from_data['class'])(*from_data['args'], **from_data['kwargs'])
        mdb.learn(
            from_data=data_source,
            to_predict=to_predict,
            **kwargs
        )
        self.fs_store.put(name, f'predictor_{company_id}_{name}', config['paths']['predictors'])

        model_data = mindsdb_native.F.get_model_data(name)

        predictor_record = Predictor.query.filter_by(company_id=company_id, name=name)
        predictor_record.data = model_data
        session.commit()

        DatabaseWrapper().register_predictors([model_data])
