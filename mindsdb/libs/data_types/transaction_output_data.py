from mindsdb.libs.constants.mindsdb import *

class TransactionOutputData():

    def __init__(self, predicted_columns=[]):
        self.data_array = []
        self.columns = []
        self.predicted_columns = []

    @property
    def predicted_values(self):
        """
        Get an array of dictionaries for predicted values
        :return:
        """
        ret = []
        for row in self.data_array:
            ret_row = {}
            for col in self.predicted_columns:
                col_index = self.columns.index(col)
                ret_row[col] = row[col_index]
            col_index = self.columns.index(KEY_CONFIDENCE)
            ret_row[KEY_CONFIDENCE] = row[col_index]

            ret += [ret_row]

        return ret
