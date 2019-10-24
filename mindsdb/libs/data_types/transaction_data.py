
class TransactionData():

    def __init__(self):
        self.data_frame = None
        self.train_df = None
        self.test_df = None
        self.validation_df = None

        self.train_indexes = {}
        self.test_indexes = {}
        self.validation_indexes = {}
        self.all_indexes = {}

        self.columns = []
