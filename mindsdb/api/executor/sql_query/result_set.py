import copy
from typing import List, Optional

import numpy as np
import pandas as pd

from mindsdb.api.executor.exceptions import WrongArgumentError


class Column:
    def __init__(self, name=None, alias=None,
                 table_name=None, table_alias=None,
                 type=None, database=None, flags=None,
                 charset=None):
        if alias is None:
            alias = name
        if table_alias is None:
            table_alias = table_name
        self.name = name
        self.alias = alias
        self.table_name = table_name
        self.table_alias = table_alias
        self.type = type
        self.database = database
        self.flags = flags
        self.charset = charset

    def get_hash_name(self, prefix):
        table_name = self.table_name if self.table_alias is None else self.table_alias
        name = self.name if self.alias is None else self.alias

        name = f'{prefix}_{table_name}_{name}'
        return name

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'


def rename_df_columns(df: pd.DataFrame, names: Optional[List] = None) -> None:
    """Inplace rename of dataframe columns

    Args:
        df (pd.DataFrame): dataframe
        names (Optional[List]): columns names to set
    """
    if names is not None:
        df.columns = names
    else:
        df.columns = list(range(len(df.columns)))


class ResultSet:
    def __init__(self, columns=None, values: List[List] = None, df: pd.DataFrame = None):
        '''

        :param columns: list of Columns
        :param values: data of resultSet, have to be list of lists with length equal to column
        :param df: injected dataframe, have to have enumerated columns and length equal to columns
        '''
        if columns is None:
            columns = []
        self._columns = columns

        if values is None:
            df = None
        elif df is None:
            df = pd.DataFrame(values)
        self._df = df

        self.is_prediction = False

    def __repr__(self):
        col_names = ', '.join([col.name for col in self._columns])

        return f'{self.__class__.__name__}({self.length()} rows, cols: {col_names})'

    def __len__(self) -> int:
        if self._df is None:
            return 0
        return len(self._df)

    def __getitem__(self, slice_val):
        # return resultSet with sliced dataframe
        df = self._df[slice_val]
        return ResultSet(columns=self.columns, df=df)

    # --- converters ---

    def from_df(self, df, database=None, table_name=None, table_alias=None):
        self._columns = [
            Column(
                name=column_name,
                table_name=table_name,
                table_alias=table_alias,
                database=database,
                type=column_dtype
            ) for column_name, column_dtype
            in zip(df.columns, df.dtypes)
        ]

        rename_df_columns(df)
        self._df = df

        return self

    def from_df_cols(self, df, col_names, strict=True):
        # find column by alias
        alias_idx = {}
        for col in col_names.values():
            if col.alias is not None:
                alias_idx[col.alias] = col

        for col in df.columns:
            if col in col_names or strict:
                column = col_names[col]
            elif col in alias_idx:
                column = alias_idx[col]
            else:
                column = Column(col)
            self._columns.append(column)

        rename_df_columns(df)
        self._df = df

        return self

    def to_df(self):
        columns_names = self.get_column_names()
        df = self.get_raw_df()
        rename_df_columns(df, columns_names)
        return df

    def to_df_cols(self, prefix=''):
        # returns dataframe and dict of columns
        #   can be restored to ResultSet by from_df_cols method

        columns = []
        col_names = {}
        for col in self._columns:
            name = col.get_hash_name(prefix)
            columns.append(name)
            col_names[name] = col

        df = self.get_raw_df()
        rename_df_columns(df, columns)
        return df, col_names

    # --- tables ---

    def get_tables(self):
        tables_idx = []
        tables = []
        cols = ['database', 'table_name', 'table_alias']
        for col in self._columns:
            table = (col.database, col.table_name, col.table_alias)
            if table not in tables_idx:
                tables_idx.append(table)
                tables.append(dict(zip(cols, table)))
        return tables

    # --- columns ---

    def get_col_index(self, col):
        """
        Get column index
        :param col: column object
        :return: index of column
        """

        col_idx = None
        for i, col0 in enumerate(self._columns):
            if col0 is col:
                col_idx = i
                break
        if col_idx is None:
            raise WrongArgumentError(f'Column is not found: {col}')
        return col_idx

    def add_column(self, col, values=None):
        self._columns.append(col)

        col_idx = len(self._columns) - 1
        if self._df is not None:
            self._df[col_idx] = values
        return col_idx

    def del_column(self, col):
        idx = self.get_col_index(col)
        self._columns.pop(idx)

        self._df.drop(idx, axis=1, inplace=True)
        rename_df_columns(self._df)

    @property
    def columns(self):
        return self._columns

    def get_column_names(self):
        columns = [
            col.name if col.alias is None else col.alias
            for col in self._columns
        ]
        return columns

    def find_columns(self, alias=None, table_alias=None):
        col_list = []
        for col in self.columns:
            if alias is not None and col.alias.lower() != alias.lower():
                continue
            if table_alias is not None and col.table_alias.lower() != table_alias.lower():
                continue
            col_list.append(col)

        return col_list

    def copy_column_to(self, col, result_set2):
        # copy with values
        idx = self.get_col_index(col)

        values = [row[idx] for row in self._records]

        col2 = copy.deepcopy(col)

        result_set2.add_column(col2, values)
        return col2

    def set_col_type(self, col_idx, type_name):
        self.columns[col_idx].type = type_name
        if self._df is not None:
            self._df[col_idx] = self._df[col_idx].astype(type_name)

    # --- records ---

    def get_raw_df(self):
        if self._df is None:
            names = range(len(self._columns))
            return pd.DataFrame([], columns=names)
        return self._df

    def add_raw_df(self, df):
        if len(df.columns) != len(self._columns):
            raise WrongArgumentError(f'Record length mismatch columns length: {len(df.columns)} != {len(self.columns)}')

        rename_df_columns(df)

        if self._df is None:
            self._df = df
        else:
            self._df = pd.concat([self._df, df], ignore_index=True)

    def add_raw_values(self, values):
        # If some values are None, the DataFrame could have incorrect integer types, since 'NaN' is technically a float, so it will convert ints to floats automatically.
        df = pd.DataFrame(values).convert_dtypes(
            convert_integer=True,
            convert_floating=True,
            infer_objects=False,
            convert_string=False,
            convert_boolean=False
        )
        self.add_raw_df(df)

    def to_lists(self, json_types=False):
        """
        :param type_cast: cast numpy types
            array->list, datetime64->str
        :return: list of lists
        """

        if len(self.get_raw_df()) == 0:
            return []
        # output for APIs. simplify types
        if json_types:
            df = self.get_raw_df().copy()
            for name, dtype in df.dtypes.to_dict().items():
                if pd.api.types.is_datetime64_any_dtype(dtype):
                    df[name] = df[name].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            df = df.replace({np.nan: None})
            return df.to_records(index=False).tolist()

        # slower but keep timestamp type
        df = self._df.replace({np.nan: None})
        return df.to_dict('split')['data']

    def get_column_values(self, col_idx):
        # get by column index
        df = self.get_raw_df()
        return list(df[df.columns[col_idx]])

    def set_column_values(self, col_name, values):
        # values is one value or list of values
        cols = self.find_columns(col_name)
        if len(cols) == 0:
            col_idx = self.add_column(Column(name=col_name))
        else:
            col_idx = self.get_col_index(cols[0])

        if self._df is not None:
            self._df[col_idx] = values

    def add_from_result_set(self, rs):

        source_names = rs.get_column_names()

        col_sequence = []
        for name in self.get_column_names():
            col_sequence.append(
                source_names.index(name)
            )

        raw_df = rs.get_raw_df()[col_sequence]

        self.add_raw_df(raw_df)

    @property
    def records(self):
        return list(self.get_records())

    def get_records(self):
        # get records as dicts.
        # !!! Attention: !!!
        # if resultSet contents duplicate column name: only one of them will be in output
        names = self.get_column_names()
        for row in self.to_lists():
            yield dict(zip(names, row))

    def length(self):
        return len(self)
