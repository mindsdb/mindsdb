from mindsdb.libs.data_types.persistent_object import PersistentObject

class PersistentModelMetadata(PersistentObject):

    _entity_name = 'model_metadata'
    _pkey = ['model_name']

    def setup(self):

        self.model_name = None
        self.train_metadata = None
        self.predict_columns = None

        self.columns = None
        self.current_phase = None
        self.column_stats = None
        self.start_time = None
        self.end_time = None
        self.error_msg = None
        self.max_group_by_count = 0
        self.total_row_count = None
        self.test_row_count = None
        self.train_row_count= None
        self.validation_row_count = None

        self.stop_training = False
        self.kill_training = False

