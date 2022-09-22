import mindsdb.interfaces.storage.db as db

from mindsdb.integrations.libs.const import PREDICTOR_STATUS

from mindsdb.interfaces.storage.fs import FileStorageFactory, RESOURCE_GROUP


class ModelStorage:
    def __init__(self, company_id, predictor_id):

        storageFactory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.PREDICTOR,
            company_id=company_id,
            sync=True
        )

        self.fileStorage = storageFactory(predictor_id)

        self.company_id = company_id
        self.predictor_id = predictor_id

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

    def file_get(self, name):
        return self.fileStorage.file_get(name)

    def file_set(self, name, content):
        self.fileStorage.file_set(name, content)

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
    def __init__(self, company_id, integration_id):
        storageFactory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            company_id=company_id,
            sync=True
        )
        self.fileStorage = storageFactory(integration_id)

        self.company_id = company_id
        self.integration_id = integration_id

    def get_connection_args(self):
        rec = db.Integration.query.get(self.integration_id)
        return rec.data

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

    def json_set(self, name, content):
        ...
    def json_get(self, name):
        ...
    def json_list(self):
        ...
    def json_del(self, name):
        ...
