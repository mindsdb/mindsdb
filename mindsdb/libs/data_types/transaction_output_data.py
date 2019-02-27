from mindsdb.libs.constants.mindsdb import *

from mindsdb.libs.data_types.mindsdb_logger import log

class TrainTransactionOutputData():
    def __init__(self):
        self.data_array = None
        self.columns = None


class PredictTransactionOutputData():

    def __init__(self, predicted_columns, data_array, columns, columns_map = {}):
        self.data_array = data_array
        self.columns = columns
        self.predicted_columns = predicted_columns
        self.columns_map = columns_map
        self.confidence_columns = []

    def _getOrigColum(self, col):
        for orig_col in self.columns_map:
            if self.columns_map[orig_col] == col:
                return orig_col

        return col

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
        """
        Get an array of dictionaries (unless as_list=True) for predicted values
        :return: predicted_values
        """
        ret = []

        # foreach row in the result extract only the predicted values
        for row in self.data_array:

            # prepare the result, either a dict or a list
            if as_list:
                ret_row = []
            else:
                ret_row = {}

            # append predicted values and confidences
            for i in range(len(self.predicted_columns)):
                pred_col = self.predicted_columns[i]
                confidence_col = self.confidence_columns[i]

                pred_col_index = self.columns.index(pred_col)
                pred_col_confidence_index = self.columns.index(confidence_col)

                if as_list:
                    ret_row += [row[pred_col_index]]
                    ret_row += [row[pred_col_confidence_index]]
                else:
                    ret_row[self._getOrigColum(pred_col)] = row[pred_col_index]
                    ret_row[confidence_col] = row[pred_col_confidence_index]

            # append row to result
            ret += [ret_row]

        # if add_header and as_list True, add the header to the result
        if as_list and add_header:
            header = []
            for i in range(len(self.predicted_columns)):
                pred_col = self.predicted_columns[i]
                confidence_col = self.confidence_columns[i]
                header.append(pred_col)
                header.append(confidence_col)
            ret = [header] + ret

        return ret
