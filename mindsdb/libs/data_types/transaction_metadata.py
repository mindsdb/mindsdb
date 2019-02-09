from mindsdb.libs.data_types.object_dict import ObjectDict
from mindsdb.config import CONFIG
from mindsdb.libs.constants.mindsdb import MODEL_GROUP_BY_DEAFAULT_LIMIT

class TransactionMetadata(ObjectDict):

    def __init__(self):

        self._ignore_keys = ['test_from_data', 'from_data']

        self.model_name = None
        self.model_predict_columns = None
        self.model_columns_map = {}
        self.model_when_conditions = None
        self.model_group_by = None
        self.model_order_by = []
        self.model_is_time_series = False
        self.from_data_dropout = 0
        self.window_size = MODEL_GROUP_BY_DEAFAULT_LIMIT
        self.model_ignore_null_targets = True
        self.from_data = None
        self.test_from_data = None
        self.type = None
        self.ignore_columns = []
        self.sample_margin_of_error = CONFIG.DEFAULT_MARGIN_OF_ERROR
        self.sample_confidence_level = CONFIG.DEFAULT_CONFIDENCE_LEVEL



# only run the test if this file is called from debugger
if __name__ == "__main__":

    a = TransactionMetadata()
    a.getAsDict()
