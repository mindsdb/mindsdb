

class DataSource:

    def __init__(self, df):
        self._df = df

    @property
    def df(self):
        return self._df

    def setDF(self, df):
        self._df = df