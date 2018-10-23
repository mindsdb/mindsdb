from mindsdb.libs.data_types.object_dict import ObjectDict
import mindsdb.config as CONFIG
from mindsdb.libs.constants.mindsdb import MODEL_GROUP_BY_DEAFAULT_LIMIT

class TransactionMetadata(ObjectDict):

    def __init__(self):

        self._ignore_keys = ['test_from_data', 'from_data']

        self.model_name = None
        self.model_predict_columns = None
        self.model_when_conditions = None
        self.model_group_by = None
        self.model_order_by = []
        self.model_order_by_type = []
        self.window_size = MODEL_GROUP_BY_DEAFAULT_LIMIT
        self.model_ignore_null_targets = True
        self.storage_file = CONFIG.SQLITE_FILE
        self.from_data = None
        self.test_from_data = None
        self.type = None
        self.ignore_columns = []



# only run the test if this file is called from debugger
if __name__ == "__main__":

    a = TransactionMetadata()
    a.getAsDict()
