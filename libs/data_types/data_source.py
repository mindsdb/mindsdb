

class DataSource:

    def __init__(self, df):
        self._df = df

    @property
    def df(self):
        return self._df

    def setDF(self, df):
        self._df = df

    def applyFunctionToColumn(self, column, function):

        self._df[column] = self._df[column].apply(lambda col: function(col))
