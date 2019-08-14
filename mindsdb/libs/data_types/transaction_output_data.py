from mindsdb.libs.constants.mindsdb import *

from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.data_types.transaction_output_row import TransactionOutputRow

class TrainTransactionOutputData():
    def __init__(self):
        self.data_frame = None
        self.columns = None


class PredictTransactionOutputData():
    def __init__(self, transaction, data, evaluations):
        self.data = data
        self.evaluations = evaluations
        self.transaction = transaction

    def __iter__(self):
        for i, value in enumerate(self.data[self.transaction.lmd['columns'][0]]):
            yield TransactionOutputRow(self, i)

    def __getitem__(self, item):
        return TransactionOutputRow(self, item)

    def __str__(self):
        return str(self.data)

    def __len__(self):
        return len(self.data[self.transaction.lmd['columns'][0]])
