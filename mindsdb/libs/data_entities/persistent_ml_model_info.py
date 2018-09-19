from mindsdb.libs.data_types.persistent_object import PersistentObject

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


