from mindsdb.libs.constants.mindsdb import *
from dateutil.parser import parse as parse_datetime

from ludwig import LudwigModel
import pandas as pd

# @TODO: Define generci interface, similar to 'base_module' in the phases
class LudwigBackend():

    def __init__(self, transaction):
        self.transaction = transaction

    def _create_ludwig_dataframe(self, mode):
        if mode == 'train':
            indexes = self.transaction.input_data.train_indexes[KEY_NO_GROUP_BY]
            columns = self.transaction.persistent_model_metadata.columns
        elif mode == 'predict':
            indexes = self.transaction.input_data.all_indexes[KEY_NO_GROUP_BY]
            columns = [col for col in self.transaction.persistent_model_metadata.columns if col not in self.transaction.persistent_model_metadata.predict_columns]
        elif mode == 'validate':
            indexes = self.transaction.input_data.validation_indexes[KEY_NO_GROUP_BY]
            columns = self.transaction.persistent_model_metadata.columns
        else:
            raise Exception(f'Unknown mode specified: "{mode}"')
        model_definition = {'input_features': [], 'output_features': []}
        data = {}

        if self.transaction.persistent_model_metadata.model_order_by is None:
            timeseries_cols = []
        else:
            timeseries_cols = list(map(lambda x: x[0], self.transaction.persistent_model_metadata.model_order_by))

        for col_ind, col in enumerate(columns):
            data[col] = []

            col_stats = self.transaction.persistent_model_metadata.column_stats[col]
            data_subtype = col_stats['data_subtype']

            ludwig_dtype = None
            encoder = None

            if col in timeseries_cols:
                ludwig_dtype = 'timeseries'

            elif data_subtype in (DATA_SUBTYPES.INT, DATA_SUBTYPES.FLOAT):
                ludwig_dtype = 'numerical'

            elif data_subtype in (DATA_SUBTYPES.BINARY):
                ludwig_dtype = 'binary'

            elif data_subtype in (DATA_SUBTYPES.DATE, DATA_SUBTYPES.TIMESTAMP):
                ludwig_dtype = 'category'
                encoder = 'stacked_cnn'

            elif data_subtype in (DATA_SUBTYPES.SINGLE, DATA_SUBTYPES.MULTIPLE):
                ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.IMAGE):
                ludwig_dtype = 'image'
                encoder = 'stacked_cnn'

            elif data_subtype in (DATA_SUBTYPES.TEXT):
                ludwig_dtype = 'text'

            else:
                # @TODO Maybe regress to some other similar subtype or use the principal data type for certain values
                self.transaction.log.error(f'The Ludwig backend doesn\'t support the "{data_subtype}" data type !')
                raise Exception(f'Data type "{data_subtype}" no supported by Ludwig model backend')

            for row_ind in indexes:
                if ludwig_dtype == 'timeseries':
                    ts_data_point = self.transaction.input_data.data_array[row_ind][col_ind]
                    try:
                        ts_data_point = float(ts_data_point)
                    except:
                        ts_data_point = parse_datetime(ts_data_point).timestamp()
                    data[col].append(ts_data_point)
                else:
                    data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])

            if col not in self.transaction.persistent_model_metadata.predict_columns:
                input_def = {
                    'name': col
                    ,'type': ludwig_dtype
                }
                if encoder is not None:
                    input_def['encoder'] = encoder
                model_definition['input_features'].append(input_def)
            else:
                output_def = {
                    'name': col
                    ,'type': ludwig_dtype
                }
                model_definition['output_features'].append(output_def)

        df = pd.DataFrame(data=data)
        if len(timeseries_cols) > 0:
            df.sort_values(timeseries_cols)
        return df, model_definition

    def train(self):
        training_dataframe, model_definition = self._create_ludwig_dataframe('train')

        print(model_definition)

        model = LudwigModel(model_definition)

        # Figure out how to pass `model_load_path`
        train_stats = model.train(training_dataframe, model_name=self.transaction.metadata.model_name)

        ludwig_model_savepath = model.model.weights_save_path.rstrip('/model_weights_progress') + '/model'

        model.save(ludwig_model_savepath)
        model.close()

        self.transaction.persistent_model_metadata.ludwig_data = {'ludwig_save_path': ludwig_model_savepath}


    def predict(self, mode='predict', ignore_columns=[]):
        predict_dataframe, model_definition = self._create_ludwig_dataframe(mode)
        model = LudwigModel.load(self.transaction.persistent_model_metadata.ludwig_data['ludwig_save_path'])

        for ignore_col in ignore_columns:
            predict_dataframe[ignore_col] = [None] * len(predict_dataframe[ignore_col])

        predictions = model.predict(data_df=predict_dataframe)
        for col_name in predictions:
            col_name_normalized = col_name.replace('_predictions', '')
            predictions = predictions.rename(columns = {col_name: col_name_normalized})

        return predictions
