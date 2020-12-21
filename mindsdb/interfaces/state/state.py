from mindsdb.interfaces.state.schemas import session, Integration, Datasource, Predictor, Configuration, Semaphor, Registration
from mindsdb.interfaces.state.storage import StorageEngine
from mindsdb.interfaces.database.database import DatabaseWrapper
import mindsdb_native
import json


class State():
    def __init__(self, config):
        self.storage = StorageEngine()
        self.config = config
        self.company_id = self.config['company_id']
        self.dbw = DatabaseWrapper()


    def populate_registrations(self):
        register_predictors = []
        for predictor in self.list_predictors():
            predictor_id = predictor.id
            if predictor.data is not None:
                register_predictors.append({
                    'name': predictor.name,
                    'predict': predictor.to_predict.split(','),
                    'data_analysis': json.loads(predictor.data)
                })
        self.dbw.register_predictors(register_predictors, True)


    def make_predictor(self, name, datasource_id, to_predict):
        predictor = Predictor(name=name, datasource_id=datasource_id, native_version=mindsdb_native.__version__, to_predict=','.join(to_predict), company_id=self.company_id, status='training')
        session.add(predictor)
        session.commit()

    def update_predictor(self, name, status, original_path, data):
        predictor = Predictor.query.filter_by(name=name, company_id=self.company_id).first()

        predictor.status = status
        predictor.data = data

        if self.storage.location != 'local':
            storage_path = f'predictor_{predictor.company_id}_{predictor.name}'
            predictor.storage_path = storage_path
            self.storage.put_fs_node(storage_path, original_path)
        session.commit()

        self.dbw.register_predictors([{
            'name': predictor.name,
            'predict': predictor.to_predict.split(','),
            'data_analysis': json.loads(predictor.data)
        }], False)

    def delete_predictor(self, name):
        predictor = Predictor.query.filter_by(name=name, company_id=self.company_id).first()
        storage_path = predictor.storage_path
        session.delete(predictor)
        session.commit()
        #self.populate_registrations()
        self.dbw.unregister_predictor(name) #<--- broken, but this should be the way we do it

        if self.storage.location != 'local':
            self.storage.del_fs_node(storage_path)

    def get_predictor(self, name):
        predictor = Predictor.query.filter_by(name=name, company_id=self.company_id).first()
        return {
            'status': predictor['status']
            ,'stats': predictor['stats']
        }

    def load_predictor(self, name):
        predictor = Predictor.query.filter_by(name=name, company_id=self.company_id).first()
        if self.storage.location != 'local':
            pass

    def list_predictors(self):
        return Predictor.query.filter_by(company_id=self.company_id)

    def list_integrations(self):
        return Integration.query.filter_by(company_id=self.company_id)
