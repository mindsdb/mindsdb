from mindsdb.libs.data_types.object_dict import ObjectDict
import mindsdb.config as CONFIG
from mindsdb.libs.constants.mindsdb import MODEL_GROUP_BY_DEAFAULT_LIMIT

class TransactionMetadata(ObjectDict):

    def __init__(self):
        self.model_name = None
        self.model_query = None
        self.model_predict_columns = None
        self.model_test_query = None
        self.model_when_conditions = None
        self.model_group_by = None
        self.model_order_by = []
        self.model_order_by_type = []
        self.model_group_by_limit = MODEL_GROUP_BY_DEAFAULT_LIMIT
        self.model_ignore_null_targets = True
        self.storage_file = CONFIG.SQLITE_FILE
        self.type = None



# only run the test if this file is called from debugger
if __name__ == "__main__":

    a = TransactionMetadata()
    a.getAsDict()
