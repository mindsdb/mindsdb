from mindsdb.libs.data_types.persistent_object import PersistentObject
from mindsdb.config import *
import os
import logging

class PersistentMlModelInfo(PersistentObject):

    _entity_name = 'model_metrics'
    _pkey = ['model_name', 'ml_model_name', 'config_serialized']

    def setup(self):

        self.model_name = None
        self.ml_model_name = None
        self.config_serialized = None

        self.status = None

        self.r_squared = None
        self.error_msg = None
        self.fs_file_ids = None

        self.loss_y = []
        self.loss_x = []
        self.error_y = []
        self.error_x = []

        self.confussion_matrices = None

        self.lowest_error = None
        self.predicted_targets = None
        self.real_targets = None

        self.accuracy = None

        self.stop_training = False
        self.kill_training = False


    def deleteFiles(self):
        """
        This deletes the model files from storage
        :return:
        """
        if self.fs_file_ids is None:
            return

        files = self.fs_file_ids
        if type(files) != type([]):
            files = [files]
        for file in files:
            filename = '{path}/{filename}.pt'.format(path=MINDSDB_STORAGE_PATH, filename=file)
            try:
                os.remove(filename)
            except OSError:
                logging.error('could not delete file {file}'.format(filename))