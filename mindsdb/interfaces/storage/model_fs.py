import os
import zipfile
import re
import io

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

    def training_state_set(self, current_state_num=None, total_states=None, state_name=None):
        rec = db.Predictor.query.get(self.predictor_id)
        if current_state_num is not None:
            rec.training_phase_current = current_state_num
        if total_states is not None:
            rec.training_phase_total = total_states
        if state_name is not None:
            rec.training_phase_name = state_name
        db.session.commit()

    def training_state_get(self):
        rec = db.Predictor.query.get(self.predictor_id)
        return [rec.training_phase_current, rec.training_phase_total, rec.training_phase_name]

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
        json_storage = get_json_storage(
            resource_id=self.predictor_id,
            resource_group=RESOURCE_GROUP.PREDICTOR
        )
        json_storage.clean()


class HandlerStorage:
    """
    This class deals with all handler-related storage requirements, from storing metadata to synchronizing folders
    across instances.
    """
    def __init__(self, integration_id: int, root_dir: str = None, is_temporal=False):
        args = {}
        if root_dir is not None:
            args['root_dir'] = root_dir
        storageFactory = FileStorageFactory(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            sync=False,
            **args
        )
        self.fileStorage = storageFactory(integration_id)
        self.integration_id = integration_id
        self.is_temporal = is_temporal
        # do not sync with remote storage

    def __convert_name(self, name):
        name = name.lower().replace(' ', '_')
        return re.sub(r'([^a-z^A-Z^_\d]+)', '_', name)

    def get_connection_args(self):
        rec = db.Integration.query.get(self.integration_id)
        return rec.data

    # files

    def file_get(self, name):
        self.fileStorage.pull_path(name)
        return self.fileStorage.file_get(name)

    def file_set(self, name, content):
        self.fileStorage.file_set(name, content)
        if not self.is_temporal:
            self.fileStorage.push_path(name)

    def file_list(self):
        ...

    def file_del(self, name):
        ...

    # folder

    def folder_get(self, name, update=True, not_empty=False):
        '''
        Copies folder from remote to local file system and returns its path

        :param name: name of the folder
        :param update: update from source even folder exists in content folder
        :param not_empty: return None if folder is empty
        :return: path to local folder
        '''

        name = self.__convert_name(name)

        self.fileStorage.pull_path(name, update=update)
        path = str(self.fileStorage.get_path(name))
        if not_empty:
            files = os.listdir(path)
            # remove lock
            if 'dir.lock' in files:
                files.remove('dir.lock')
            if len(files) == 0:
                return None
        return path

    def folder_sync(self, name):
        # sync abs path
        if self.is_temporal:
            return
        name = self.__convert_name(name)
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

    def export_files(self) -> bytes:
        folder_path = self.folder_get('', not_empty=True)
        if folder_path is None:
            return None

        # parent_folder = os.path.dirname(folder_path)

        zip_fd = io.BytesIO()

        with zipfile.ZipFile(zip_fd, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    if file == 'dir.lock':
                        continue
                    abs_path = os.path.join(root, file)
                    zipf.write(abs_path, os.path.relpath(abs_path, folder_path))

        zip_fd.seek(0)
        return zip_fd.read()

    def import_files(self, content: bytes):

        folder_path = self.folder_get('')

        zip_fd = io.BytesIO()
        zip_fd.write(content)
        zip_fd.seek(0)

        with zipfile.ZipFile(zip_fd, 'r') as zip_ref:
            zip_ref.extractall(folder_path)

        self.folder_sync('')
