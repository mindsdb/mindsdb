from mindsdb.libs.constants.mindsdb import *

from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.data_types.transaction_output_row import TransactionOutputRow
class TransactionOutputData():

    def __init__(self, transaction = None, data = {}, evaluations = {}, ):
        self.data = data
        self.columns = None
        self.evaluations = evaluations
        self.transaction = transaction



    def __iter__(self):
        self.columns = self.transaction.persistent_model_metadata.columns
        first_col = self.columns[0]

        for i, cell in enumerate(self.data[first_col]):

            yield TransactionOutputRow(self, i)

    def __getitem__(self, item):

        return TransactionOutputRow(self, item)


    def __str__(self):

        return str(self.data)

    @property
    def predicted_values(self):
        """
        Legacy method, we should remove but for now so that things work
        :return:
        """
        self.transaction.log.error('predict_values method will be removed from Predictor.predict response, just make sure you use the outout as an iterator')
        self.columns = self.transaction.persistent_model_metadata.columns
        first_column = self.columns[0]

        ret = []
        for row, v in enumerate(self.data[first_column]):
            ret_arr = {col: self.data[col][row] for col in self.evaluations}
            ret_arr['prediction_confidence'] = 0 # we no longer support
            ret += [ret_arr]
        return ret