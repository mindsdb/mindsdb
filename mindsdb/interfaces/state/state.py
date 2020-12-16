from mindsdb.interfaces.state.schemas import session, Integration, Datasource, Predictor, Configuration, Semaphor, Registration
from mindsdb.interfaces.state.storage import StorageEngine
from mindsdb.interfaces.state.config import Config
from mindsdb.interfaces.database.database import DatabaseWrapper
import mindsdb_native

class State():
    def __init__(self, config, company_id=None):
        self.storage = StorageEngine()
        self.config = Config()
        self.company_id = self.config['company_id']
        self.dbw = DatabaseWrapper()
        self.update_registrations(setup=True)


    def update_registrations(self, setup=False):
        register_predictors = []
        for predictor in self.list_predictors():
            predictor_id = predictor.id
            for integration_id in [x.id for x in self.list_integrations()]:
                if len(Registration.query.filter_by(company_id=self.company_id,integration_id=integration_id,predictor_id=predictor_id)) < 1:
                    if predictor.data is not None:
                        register_predictors.append({
                            'name': predictor.name,
                            'predict': predictor.to_predict.split(','),
                            'data_analysis': predictor.data
                        })
                        # We re-register with every integration for now, so no need to keep itterating
                        break

        self.dbw.register_predictors(register_predictors,setup=setup)


    def make_predicotr(self, name, datasource_id, to_predict):
        predictor = Predictor(name=name, datasource_id=datasource_id, native_version=mindsdb_native.__version__, to_predict=','.join(to_predict), company_id=self.company_id, status='training')
        session.add(predictor)
        session.commit()

    def update_predictor(self, name, status, path, data):
        predictor = Predictor.query.filter_by(name=name, data=data, company_id=self.company_id).first()

        predictor.status = status

        if self.storage.location != 'local':
            storage_path = f'predictor_{predictor.company_id}_{predictor.name}'
            predictor.storage_path = storage_path
            self.storage.put_fs_node(storage_path, path)

        session.commit()
        self.update_registrations()

    def delete_predictor(self, name):
        predictor = Predictor.query.filter_by(name=name, company_id=self.company_id).first()
        self.dbw.unregister_predictor(name)
        storage_path = predictor.storage_path
        predictor.delete()

        if self.storage.location != 'local':
            self.storage.del_fs_node(storage_path)

    def load_predictor(self, name):
        if self.storage.location != 'local':
            pass

    def list_predictors(self):
        return Predictor.query.filter_by(company_id=self.company_id)


    def list_integrations(self):
        return Integration.query.filter_by(company_id=self.company_id)
