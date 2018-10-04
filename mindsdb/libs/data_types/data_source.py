

class DataSource:

    def __init__(self, df):
        self._col_map = {} # you can store here if there were some columns renamed
        self._df = df

    @property
    def df(self):
        return self._df

    def setDF(self, df):
        self._df = df

    def dropColumns(self, column_list):
        """
        Drop columns by original names
        :param column_list: a list of columns that you want to drop
        :return:
        """
        cols = [col if col not in self._col_map else self._col_map[col] for col in column_list]
        self._df = self._df.drop(columns=cols)

    def applyFunctionToColumn(self, column, function):

        self._df[column] = self._df[column].apply(lambda col: function(col))
