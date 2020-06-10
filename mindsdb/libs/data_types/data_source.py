from mindsdb.libs.constants.mindsdb import DATA_TYPES_SUBTYPES, DATA_TYPES, DATA_SUBTYPES
from mindsdb.libs.data_types.mindsdb_logger import log


class DataSource:

    def __init__(self, *args, **kwargs):
        self.data_types = {}
        self.data_subtypes = {}
        df, col_map = self._setup(*args, **kwargs)
        self._set_df(df, col_map)
        self._cleanup()

    def _setup(self, df, **kwargs):
        col_map = {}

        for col in df.columns:
            col_map[col] = col

        return df, col_map

    def _cleanup(self):
        pass

    @property
    def df(self):
        return self._df

    def set_subtypes(self, data_subtypes):
        if data_subtypes is not None:
            for col in data_subtypes:
                if col not in self._col_map:
                    del data_subtypes[col]
                    log.warning(f'Column {col} not present in your data, ignoring the "{data_subtypes[col]}" subtype you specified for it')

            self.data_subtypes = data_subtypes
            for col in self.data_subtypes:
                col_subtype = self.data_subtypes[col]
                if col_subtype not in [getattr(DATA_SUBTYPES,x) for x in DATA_SUBTYPES.__dict__ if '__' not in x]:
                    raise Exception(f'Invalid data subtype: {col_subtype}')

                for col_type in DATA_TYPES_SUBTYPES.subtypes:
                    if col_subtype in DATA_TYPES_SUBTYPES.subtypes[col_type]:
                        self.data_types[col] = col_type

    def _set_df(self, df, col_map):
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
