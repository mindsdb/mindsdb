from mindsdb.libs.data_types.mindsdb_logger import log

class DataSource:

    def __init__(self, *args, **kwargs):
        self.log = log
        df, col_map = self._setup(*args, **kwargs)
        self.setDF(df, col_map)
        self._cleanup()

    def _setup(self, df):
        col_map = {}
        for col in df.columns:
            col_map[col] = col
        return df, col_map

    def _cleanup(self):
        pass

    @property
    def df(self):
        return self._df

    def setDF(self, df, col_map):
        self._df = df
        self._col_map = col_map

    def dropColumns(self, column_list):
        """
        Drop columns by original names

        :param column_list: a list of columns that you want to drop
        :return: None
        """

        cols = [col if col not in self._col_map else self._col_map[col] for col in column_list]
        self._df = self._df.drop(columns=cols)

    def __getattr__(self, item):
        """
        Map all other functions to the DataFrame

        :param item: the attribute to get
        :return: the dataframe attribute
        """

        return getattr(self._df, item)


    def __getitem__(self, key):
        """
        Map all other items to the DataFrame

        :param key: the key to get
        :return: the dataframe attribute
        """
        return self._df[key]


    def __setitem__(self, key, value):
        """
        Support item assignment, mapped ot dataframe
        :param key:
        :param value:
        :return:
        """
        self._df[key] = value
