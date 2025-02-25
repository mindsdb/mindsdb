import os
import re
import json
import io
import zipfile
from typing import Union

import mindsdb.interfaces.storage.db as db

from .fs import RESOURCE_GROUP, FileStorageFactory, SERVICE_FILES_NAMES
from .json import get_json_storage, get_encrypted_json_storage


JSON_STORAGE_FILE = 'json_storage.json'


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

    def _get_model_record(self, model_id: int, check_exists: bool = False) -> Union[db.Predictor, None]:
        """Get model record by id

        Args:
            model_id (int): model id
            check_exists (bool): true if need to check that model exists

        Returns:
            Union[db.Predictor, None]: model record

        Raises:
            KeyError: if `check_exists` is True and model does not exists
        """
        model_record = db.Predictor.query.get(self.predictor_id)
        if check_exists is True and model_record is None:
            raise KeyError('Model does not exists')
        return model_record

    def get_info(self):
        rec = self._get_model_record(self.predictor_id)
        return dict(status=rec.status,
                    to_predict=rec.to_predict,
                    data=rec.data,
                    learn_args=rec.learn_args)

    def status_set(self, status, status_info=None):
        rec = self._get_model_record(self.predictor_id)
        rec.status = status
        if status_info is not None:
            rec.data = status_info
        db.session.commit()

    def training_state_set(self, current_state_num=None, total_states=None, state_name=None):
        rec = self._get_model_record(self.predictor_id)
        if current_state_num is not None:
            rec.training_phase_current = current_state_num
        if total_states is not None:
            rec.training_phase_total = total_states
        if state_name is not None:
            rec.training_phase_name = state_name
        db.session.commit()

    def training_state_get(self):
        rec = self._get_model_record(self.predictor_id)
        return [rec.training_phase_current, rec.training_phase_total, rec.training_phase_name]

    def columns_get(self):
        rec = self._get_model_record(self.predictor_id)
        return rec.dtype_dict

    def columns_set(self, columns):
        # columns: {name: dtype}

        rec = self._get_model_record(self.predictor_id)
        rec.dtype_dict = columns
        db.session.commit()

    # files

    def file_get(self, name):
        return self.fileStorage.file_get(name)

    def file_set(self, name, content):
        self.fileStorage.file_set(name, content)

    def folder_get(self, name):
        # pull folder and return path
        name = name.lower().replace(' ', '_')
        name = re.sub(r'([^a-z^A-Z^_\d]+)', '_', name)

        self.fileStorage.pull_path(name)
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

    def encrypted_json_set(self, name: str, data: dict) -> None:
        json_storage = get_encrypted_json_storage(
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

    def encrypted_json_get(self, name: str) -> dict:
        json_storage = get_encrypted_json_storage(
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

    def is_empty(self):
        """ check if storage directory is empty

            Returns:
                bool: true if dir is empty
        """
        for path in self.fileStorage.folder_path.iterdir():
            if path.is_file() and path.name in SERVICE_FILES_NAMES:
                continue
            return False
        return True

    def get_connection_args(self):
        rec = db.Integration.query.get(self.integration_id)
        return rec.data

    def update_connection_args(self, connection_args: dict) -> None:
        """update integration connection args

        Args:
            connection_args (dict): new connection args
        """
        rec = db.Integration.query.get(self.integration_id)
        if rec is None:
            raise KeyError("Can't find integration")
        rec.data = connection_args
        db.session.commit()

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

    def folder_get(self, name):
        ''' Copies folder from remote to local file system and returns its path

        :param name: name of the folder
        '''
        name = self.__convert_name(name)

        self.fileStorage.pull_path(name)
        return str(self.fileStorage.get_path(name))

    def folder_sync(self, name):
        # sync abs path
        if self.is_temporal:
            return
        name = self.__convert_name(name)
        self.fileStorage.push_path(name)

    # jsons

    def json_set(self, name, content):
        json_storage = get_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )
        return json_storage.set(name, content)

    def encrypted_json_set(self, name: str, content: dict) -> None:
        json_storage = get_encrypted_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )
        return json_storage.set(name, content)

    def json_get(self, name):
        json_storage = get_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )
        return json_storage.get(name)

    def encrypted_json_get(self, name: str) -> dict:
        json_storage = get_encrypted_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )
        return json_storage.get(name)

    def json_list(self):
        ...

    def json_del(self, name):
        ...

    def export_files(self) -> bytes:
        json_storage = self.export_json_storage()

        if self.is_empty() and not json_storage:
            return None

        folder_path = self.folder_get('')

        zip_fd = io.BytesIO()

        with zipfile.ZipFile(zip_fd, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    if file_name in SERVICE_FILES_NAMES:
                        continue
                    abs_path = os.path.join(root, file_name)
                    zipf.write(abs_path, os.path.relpath(abs_path, folder_path))

            # If JSON storage is not empty, add it to the zip file.
            if json_storage:
                json_str = json.dumps(json_storage)
                zipf.writestr(JSON_STORAGE_FILE, json_str)

        zip_fd.seek(0)
        return zip_fd.read()

    def import_files(self, content: bytes):

        folder_path = self.folder_get('')

        zip_fd = io.BytesIO()
        zip_fd.write(content)
        zip_fd.seek(0)

        with zipfile.ZipFile(zip_fd, 'r') as zip_ref:
            for name in zip_ref.namelist():
                # If JSON storage file is in the zip file, import the content to the JSON storage.
                # Thereafter, remove the file from the folder.
                if name == JSON_STORAGE_FILE:
                    json_storage = zip_ref.read(JSON_STORAGE_FILE)
                    self.import_json_storage(json_storage)

                else:
                    zip_ref.extract(name, folder_path)

        self.folder_sync('')

    def export_json_storage(self) -> list[dict]:
        json_storage = get_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )

        records = []
        for record in json_storage.get_all_records():
            record_dict = record.to_dict()
            if record_dict.get('encrypted_content'):
                record_dict['encrypted_content'] = record_dict['encrypted_content'].decode()
            records.append(record_dict)

        return records

    def import_json_storage(self, records: bytes) -> None:
        json_storage = get_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )

        encrypted_json_storage = get_encrypted_json_storage(
            resource_id=self.integration_id,
            resource_group=RESOURCE_GROUP.INTEGRATION
        )

        records = json.loads(records.decode())

        for record in records:
            if record['encrypted_content']:
                encrypted_json_storage.set_str(record['name'], record['encrypted_content'])
            else:
                json_storage.set(record['name'], record['content'])
