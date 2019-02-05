from mindsdb.libs.data_types.mindsdb_logger import log

class DataSource:

    def __init__(self, *args, **kwargs):
        self._col_map = {} # you can store here if there were some columns renamed
        self._setup(*args, **kwargs)
        self.log = log

    def _setup(self, df):
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
        :return: None
        """

        cols = [col if col not in self._col_map else self._col_map[col] for col in column_list]
        self._df = self._df.drop(columns=cols)

    def applyFunctionToColumn(self, column, function):
        """
        This applies a function for all rows in a column

        :param column: column to affect
        :param function: a lambda or function to call
        :return: None
        """

        # make sure that we use the column name in the data source
        column = self.getColNameAsInDF(column)

        self._df[column] = self._df[column].apply(lambda col: function(col))


    def getColNameAsInDF(self, column):
        """
        Since Columns can be mapped at somepoint in the DF then you can use this to get the DF column name by passing the original value

        :param column:
        :return: None
        """

        return column if column not in self._col_map else self._col_map[column]


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