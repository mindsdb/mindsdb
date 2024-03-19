import copy
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


class ResultSet:
    def __init__(self, length=0):
        self._columns = []
        # records is list of lists with the same length as columns
        self._records = []
        for i in range(length):
            self._records.append([])

        self.is_prediction = False

    def __repr__(self):
        col_names = ', '.join([col.name for col in self._columns])
        data = '\n'.join([str(rec) for rec in self._records[:20]])

        if len(self._records) > 20:
            data += '\n...'

        return f'{self.__class__.__name__}({self.length()} rows, cols: {col_names})\n {data}'

    def __len__(self) -> int:
        return len(self._records)

    # --- converters ---

    def from_df(self, df, database, table_name, table_alias=None):

        resp_dict = df.to_dict(orient='split')

        self._records = resp_dict['data']

        for col in resp_dict['columns']:
            self._columns.append(Column(
                name=col,
                table_name=table_name,
                table_alias=table_alias,
                database=database,
                type=df.dtypes[col]
            ))
        return self

    def from_df_cols(self, df, col_names, strict=True):
        # find column by alias
        alias_idx = {}
        for col in col_names.values():
            if col.alias is not None:
                alias_idx[col.alias] = col

        resp_dict = df.to_dict(orient='split')

        self._records = resp_dict['data']

        for col in resp_dict['columns']:
            if col in col_names or strict:
                column = col_names[col]
            elif col in alias_idx:
                column = alias_idx[col]
            else:
                column = Column(col)
            self._columns.append(column)
        return self

    def to_df(self):
        columns = self.get_column_names()
        return pd.DataFrame(self._records, columns=columns)

    def to_df_cols(self, prefix=''):
        # returns dataframe and dict of columns
        #   can be restored to ResultSet by from_df_cols method

        columns = []
        col_names = {}
        for col in self._columns:
            name = col.get_hash_name(prefix)
            columns.append(name)
            col_names[name] = col

        return pd.DataFrame(self._records, columns=columns), col_names

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

    def _locate_column(self, col):
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

        if values is None:
            values = []
        # update records
        if len(self._records) > 0:
            for rec in self._records:
                if len(values) > 0:
                    value = values.pop(0)
                else:
                    value = None
                rec.append(value)

    def del_column(self, col):
        idx = self._locate_column(col)
        self._columns.pop(idx)
        for row in self._records:
            row.pop(idx)

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
        idx = self._locate_column(col)

        values = [row[idx] for row in self._records]

        col2 = copy.deepcopy(col)

        result_set2.add_column(col2, values)
        return col2

    # --- records ---

    def add_records(self, data):
        names = self.get_column_names()
        for rec in data:
            # if len(rec) != len(self._columns):
            #     raise ErSqlWrongArguments(f'Record length mismatch columns length: {len(rec)} != {len(self._columns)}')

            record = [
                rec[name]
                for name in names
            ]
            self._records.append(record)

    def get_records_raw(self):
        return self._records

    def add_record_raw(self, rec):
        if len(rec) != len(self._columns):
            raise WrongArgumentError(f'Record length mismatch columns length: {len(rec)} != {len(self.columns)}')
        self._records.append(rec)

    @property
    def records(self):
        return self.get_records()

    def get_records(self):
        # get records as dicts.
        # !!! Attention: !!!
        # if resultSet contents duplicate column name: only one of them will be in output
        names = self.get_column_names()
        records = []
        for row in self._records:
            records.append(dict(zip(names, row)))
        return records

    # def clear_records(self):
    #     self._records = []

    def length(self):
        return len(self._records)
