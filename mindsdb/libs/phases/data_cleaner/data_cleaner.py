from mindsdb.libs.phases.base_module import BaseModule
from mindsdb.libs.data_types.mindsdb_logger import log


class DataCleaner(BaseModule):
    def _cleanup_w_missing_targets(self, df):
        initial_len = len(df)
        df = df.dropna(subset=self.transaction.lmd['predict_columns'])
        no_dropped = len(df) - initial_len
        if no_dropped > 0:
            self.log.warning(f'Dropped {no_dropped} rows because they had null values in one or more of the columns that we are trying to predict. Please always provide non-null values in the columns you want to predict !')
        return df

    def _cleanup_ignored(self, df):
        for col_name in df.columns.values:
            if len(df[col_name].dropna()) < 1:
                self.transaction.lmd['columns_to_ignore'].append(col_name)
                self.transaction.lmd['empty_columns'].append(col_name)
                self.log.warning(f'Column "{col_name}" is empty ! We\'ll go ahead and ignore it, please make sure you gave mindsdb the correct data.')

        df = df.drop(columns=self.transaction.lmd['columns_to_ignore'])
        return df

    def run(self, stage):
        if stage == 0:
            self.transaction.input_data.data_frame = self._cleanup_w_missing_targets(self.transaction.input_data.data_frame)
            self.transaction.input_data.data_frame = self._cleanup_ignored(self.transaction.input_data.data_frame)
