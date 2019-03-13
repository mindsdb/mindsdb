from mindsdb.libs.constants.mindsdb import *

from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.data_types.transaction_output_row import TransactionOutputRow

class TrainTransactionOutputData():
    def __init__(self):
        self.data_array = None
        self.columns = None


class PredictTransactionOutputData():

    def __init__(self, transaction = None, data = {}, evaluations = {}, ):
        self.data = data
        self.columns = None
        self.evaluations = evaluations
        self.transaction = transaction

    def __iter__(self):
        self.columns = self.transaction.persistent_model_metadata.columns
        first_col = self.columns[0]

        for i, cell in enumerate(self.data[first_col]):
            yield TransactionOutputRow(self, i).as_dict()

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


"""

    def _getOrigColum(self, col):
        for orig_col in self.columns_map:
            if self.columns_map[orig_col] == col:
                return orig_col

    def __iter__(self):
        self.columns = self.transaction.persistent_model_metadata.columns
        first_col = self.columns[0]

    def __iter__(self):
        self.iter_col_n = 0
        return self

    def __next__(self):
        if self.iter_col_n < len(self.data_array[0]):
            predictions_map = {}
            for i in range(len(self.predicted_columns)):
                pred_col = self.predicted_columns[i]
                confidence_col = self.confidence_columns[i]
                predictions_map[self._getOrigColum(pred_col)] = self.data_array[self.iter_col_n][self.columns.index(pred_col)]
                predictions_map[confidence_col] = self.data_array[self.iter_col_n][self.columns.index(confidence_col)]

            self.iter_col_n += 1
            return predictions_map
        else:
            raise StopIteration

    def __iter__(self):
        self.iter_col_n = 0
        return self

    def __next__(self):
        if self.iter_col_n < len(self.data_array[0]):
            predictions_map = {}
            for i in range(len(self.predicted_columns)):
                pred_col = self.predicted_columns[i]
                confidence_col = self.confidence_columns[i]
                predictions_map[self._getOrigColum(pred_col)] = self.data_array[self.iter_col_n][self.columns.index(pred_col)]
                predictions_map[confidence_col] = self.data_array[self.iter_col_n][self.columns.index(confidence_col)]

            self.iter_col_n += 1
            return predictions_map
        else:
            raise StopIteration

    @property
    def predicted_values(self, as_list=False, add_header = False):

        ret = []


    for i in range(len(self.predicted_columns)):
        pred_col = self.predicted_columns[i]
        confidence_col = self.confidence_columns[i]

    if as_list:
        ret_row += [row[pred_col_index]]
        ret_row += [row[pred_col_confidence_index]]
    else:
        ret_row[self._getOrigColum(pred_col)] = row[pred_col_index]
        ret_row[confidence_col] = row[pred_col_confidence_index]


    if as_list and add_header:
        header = []
        for i in range(len(self.predicted_columns)):
            pred_col = self.predicted_columns[i]
            confidence_col = self.confidence_columns[i]
            header.append(pred_col)
            header.append(confidence_col)
        ret = [header] + ret
"""
