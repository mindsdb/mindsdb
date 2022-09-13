import os
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import Config

from mindsdb.integrations.libs.const import PREDICTOR_STATUS


class ModelStorage:
    def __init__(self, fs_store, company_id, integration_id, predictor_id):
        config = Config()

        self.fs_store = fs_store
        self.base_dir = config['paths']['predictors']
        self.company_id = company_id
        self.predictor_id = predictor_id
        self.integration_id = integration_id

    # -- fields --

    def get_info(self):
        rec = db.Predictor.query.get(self.predictor_id)
        return dict(status=rec.status, to_predict=rec.to_predict)

    def status_set(self, status, status_info=None):
        rec = db.Predictor.query.get(self.predictor_id)
        rec.status = status
        if status == PREDICTOR_STATUS.ERROR and status_info is not None:
            rec.data = status_info
        db.session.commit()

    def columns_get(self):
        rec = db.Predictor.query.get(self.predictor_id)
        return rec.dtype_dict

    def columns_set(self, columns):
        # columns: {name: dtype}

        rec = db.Predictor.query.get(self.predictor_id)
        rec.dtype_dict = columns
        db.session.commit()

    # files

    def _get_file_name(self, name):
        return f'predictor_{self.company_id}_{self.predictor_id}_{name}'

    def file_get(self, name):
        file_name = self._get_file_name(name)

        self.fs_store.get(file_name, self.base_dir)
        with open(os.path.join(self.base_dir, file_name), 'rb') as fd:
            return fd.read()

    def file_set(self, name, content):
        file_name = self._get_file_name(name)

        with open(os.path.join(self.base_dir, file_name), 'wb') as fd:
            fd.write(content)

        self.fs_store.put(file_name, self.base_dir)

    def file_list(self):
        ...
    def file_del(self, name):
        ...

    # jsons

    def json_set(self, name, content):
        ...
    def json_get(self, name):
        ...
    def json_list(self):
        ...
    def json_del(self, name):
        ...


class HandlerStorage:
    def __init__(self, fs_store, company_id, integration_id):
        config = Config()

        self.fs_store = fs_store
        self.base_dir = config['paths']['predictors']
        self.company_id = company_id
        self.integration_id = integration_id

    def get_connection_args(self):
        rec = db.Integration.query.get(self.integration_id)
        return rec.data

    # files
    def _get_file_name(self, name):
        return f'predictor_{self.company_id}_int_{self.integration_id}_{name}'

    def file_get(self, name):
        file_name = self._get_file_name(name)

        self.fs_store.get(file_name, self.base_dir)
        with open(os.path.join(self.base_dir, file_name), 'rb') as fd:
            return fd.read()

    def file_set(self, name, content):
        file_name = self._get_file_name(name)

        with open(os.path.join(self.base_dir, file_name), 'wb') as fd:
            fd.write(content)

        self.fs_store.put(file_name, self.base_dir)

    def file_list(self):
        ...
    def file_del(self, name):
        ...

    # jsons

    def json_set(self, name, content):
        ...
    def json_get(self, name):
        ...
    def json_list(self):
        ...
    def json_del(self, name):
        ...
