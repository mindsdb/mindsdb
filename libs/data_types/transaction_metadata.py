from libs.data_types.object_dict import ObjectDict

class TransactionMetadata(ObjectDict):

    def __init__(self):
        self.model_name = None
        self.model_query = None
        self.model_predict_columns = None
        self.model_test_query = None
        self.model_when_conditions = None
        self.model_group_by = None
        self.model_order_by = []
        self.type = None



# only run the test if this file is called from debugger
if __name__ == "__main__":

    a = TransactionMetadata()
    a.getAsDict()
