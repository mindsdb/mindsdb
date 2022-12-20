import re

import mindsdb.interfaces.storage.db as db

from .fs import RESOURCE_GROUP, FileStorageFactory
from .json import get_json_storage


class ModelStorage:
    """
    This class deals with all model-related storage requirements, from setting status to storing artifacts.
    """
    def __init__(self, predictor_id):
        storageFactory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.PREDICTOR,
            sync=True
        )
        self.fileStorage = storageFactory(predictor_id)
        self.predictor_id = predictor_id

    # -- fields --

    def get_info(self):
        rec = db.Predictor.query.get(self.predictor_id)
        return dict(status=rec.status,
                    to_predict=rec.to_predict,
                    data=rec.data)

    def status_set(self, status, status_info=None):
        rec = db.Predictor.query.get(self.predictor_id)
        rec.status = status
        if status_info is not None:
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

    def file_get(self, name):
        return self.fileStorage.file_get(name)

    def file_set(self, name, content):
        self.fileStorage.file_set(name, content)

    def file_list(self):
        ...

    def file_del(self, name):
        ...

    # jsons

    def json_set(self, name, data):
        json_storage = get_json_storage(
            resource_id=self.predictor_id,
            resource_group=RESOURCE_GROUP.PREDICTOR
        )
        return json_storage.set(name, data)

    def json_get(self, name):
        json_storage = get_json_storage(
            resource_id=self.predictor_id,
            resource_group=RESOURCE_GROUP.PREDICTOR
        )
        return json_storage.get(name)

    def json_list(self):
        ...

    def json_del(self, name):
        ...

    def delete(self):
        self.fileStorage.delete()
        # TODO delete json


class HandlerStorage:
    """
    This class deals with all handler-related storage requirements, from storing metadata to synchronizing folders
    across instances.
    """
    def __init__(self, integration_id):
        storageFactory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            sync=False
        )
        self.fileStorage = storageFactory(integration_id)
        self.integration_id = integration_id

    def get_connection_args(self):
        rec = db.Integration.query.get(self.integration_id)
        return rec.data

    # files

    def file_get(self, name):
        self.fileStorage.pull_path(name)
        return self.fileStorage.file_get(name)

    def file_set(self, name, content):
        self.fileStorage.file_set(name, content)
        self.fileStorage.push_path(name)

    def file_list(self):
        ...

    def file_del(self, name):
        ...

    # folder

    def folder_get(self, name, update=True):
        # pull folder and return path
        name = name.lower().replace(' ', '_')
        name = re.sub(r'([^a-z^A-Z^_\d]+)', '_', name)

        self.fileStorage.pull_path(name, update=update)
        return str(self.fileStorage.get_path(name))

    def folder_sync(self, name):
        # sync abs path
        name = name.lower().replace(' ', '_')
        name = re.sub(r'([^a-z^A-Z^_\d]+)', '_', name)

        self.fileStorage.push_path(name)

    # jsons

    def json_set(self, name, content):
        ...

    def json_get(self, name):
        ...

    def json_list(self):
        ...

    def json_del(self, name):
        ...
