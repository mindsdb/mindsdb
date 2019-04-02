from mindsdb.libs.constants.mindsdb import *
from mindsdb.config import *
from dateutil.parser import parse as parse_datetime
from scipy.misc import imread

from ludwig import LudwigModel
import pandas as pd

# @TODO: Define generci interface, similar to 'base_module' in the phases
class LudwigBackend():

    def __init__(self, transaction):
        self.transaction = transaction

    def _translate_df_to_timeseries_format(self, df, model_definition, timeseries_cols):
        input_features = model_definition['input_features']

        other_col_names = []
        timeseries_col_name = timeseries_cols[0]
        for feature_def in input_features:
            if feature_def['name'] not in self.transaction.persistent_model_metadata.model_group_by:
                feature_def['type'] = 'sequence'
                if feature_def['name'] not in timeseries_cols:
                    other_col_names.append(feature_def['name'])

        new_cols = {}
        for col in [*other_col_names,timeseries_col_name]:
            new_cols[col] = []

        nr_ele = len(df[timeseries_col_name])

        if self.transaction.persistent_model_metadata.window_size_seconds is not None:
            window_size_seconds = self.transaction.persistent_model_metadata.window_size_seconds
            for i in range(nr_ele):
                current_window = 0
                new_row = {}

                timeseries_row = [df[timeseries_col_name][i]]

                for col in other_col_names:
                    new_row[col] = [df[col][i]]

                inverted_index_range = list(range(i))
                inverted_index_range.reverse()
                for ii in inverted_index_range:
                    if window_size_seconds < current_window + (timeseries_row[-1] - df[timeseries_col_name][ii]):
                        break
                    current_window += (timeseries_row[-1] - df[timeseries_col_name][ii])
                    timeseries_row.append(df[timeseries_col_name][ii])
                    for col in other_col_names:
                        new_row[col].append(df[col][ii])

                new_row[timeseries_col_name] = timeseries_row

                for col in new_row:
                    new_row[col].reverse()
                    new_cols[col].append(new_row[col])
        else:
            window_size_samples = self.transaction.persistent_model_metadata.window_size_samples
            for i in range(nr_ele):
                new_row = {}

                timeseries_row = [df[timeseries_col_name][i]]

                for col in other_col_names:
                    new_row[col] = [df[col][i]]

                inverted_index_range = list(range(i))
                inverted_index_range.reverse()
                for ii in inverted_index_range:
                    if (i - ii) > window_size_samples:
                        break
                    timeseries_row.append(df[timeseries_col_name][ii])
                    for col in other_col_names:
                        new_row[col].append(df[col][ii])

                new_row[timeseries_col_name] = timeseries_row

                for col in new_row:
                    new_row[col].reverse()
                    new_cols[col].append(new_row[col])


        for col in new_cols:
            df[col] = new_cols[col]
        return df, model_definition

    def _create_ludwig_dataframe(self, mode):
        if mode == 'train':
            indexes = self.transaction.input_data.train_indexes[KEY_NO_GROUP_BY]
            columns = self.transaction.persistent_model_metadata.columns
        elif mode == 'predict':
            indexes = self.transaction.input_data.all_indexes[KEY_NO_GROUP_BY]
            columns = [col for col in self.transaction.persistent_model_metadata.columns if col not in self.transaction.persistent_model_metadata.predict_columns]
        elif mode == 'validate':
            indexes = self.transaction.input_data.validation_indexes[KEY_NO_GROUP_BY]
            columns = [col for col in self.transaction.persistent_model_metadata.columns if col not in self.transaction.persistent_model_metadata.predict_columns]
        elif mode == 'test':
            indexes = self.transaction.input_data.test_indexes[KEY_NO_GROUP_BY]
            columns = [col for col in self.transaction.persistent_model_metadata.columns if col not in self.transaction.persistent_model_metadata.predict_columns]
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
            cell_type = None
            in_memory = None
            height = None
            width = None

            if col in timeseries_cols:
                encoder = 'rnn'
                cell_type = 'gru_cudnn'
                ludwig_dtype = 'order_by_col'

            if data_subtype in DATA_SUBTYPES.ARRAY:
                encoder = 'rnn'
                cell_type = 'gru_cudnn'
                ludwig_dtype = 'sequence'

            elif data_subtype in (DATA_SUBTYPES.INT, DATA_SUBTYPES.FLOAT):
                ludwig_dtype = 'numerical'

            elif data_subtype in (DATA_SUBTYPES.BINARY):
                ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.DATE):
                if col not in self.transaction.persistent_model_metadata.predict_columns:
                    ludwig_dtype = 'date'
                else:
                    ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.TIMESTAMP):
                ludwig_dtype = 'numerical'

            elif data_subtype in (DATA_SUBTYPES.SINGLE, DATA_SUBTYPES.MULTIPLE):
                ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.IMAGE):
                ludwig_dtype = 'image'
                encoder = 'stacked_cnn'
                in_memory = True
                height = 256
                width = 256

            elif data_subtype in (DATA_SUBTYPES.TEXT):
                ludwig_dtype = 'text'

            else:
                # @TODO Maybe regress to some other similar subtype or use the principal data type for certain values
                self.transaction.log.error(f'The Ludwig backend doesn\'t support the "{data_subtype}" data type !')
                estr = f'Data subtype "{data_subtype}" no supported by Ludwig model backend'
                raise Exception(estr)

            custom_logic_continue = False

            for row_ind in indexes:
                if ludwig_dtype == 'order_by_col':
                    ts_data_point = self.transaction.input_data.data_array[row_ind][col_ind]

                    try:
                        ts_data_point = float(ts_data_point)
                    except:
                        ts_data_point = parse_datetime(ts_data_point).timestamp()
                    data[col].append(ts_data_point)

                elif ludwig_dtype == 'sequence':
                    arr_str = self.transaction.input_data.data_array[row_ind][col_ind]
                    arr = list(map(float,arr_str.rstrip(']').lstrip('[').split(self.transaction.persistent_model_metadata.column_stats[col]['separator'])))
                    data[col].append(arr)

                # Date isn't supported yet, so we hack around it
                elif ludwig_dtype == 'date':
                    if col in data:
                        data.pop(col)
                        data[col + '_year'] = []
                        data[col + '_month'] = []
                        data[col + '_day'] = []

                        model_definition['input_features'].append({
                            'name': col + '_year'
                            ,'type': 'numerical'
                        })
                        model_definition['input_features'].append({
                            'name': col + '_month'
                            ,'type': 'numerical'
                        })
                        model_definition['input_features'].append({
                            'name': col + '_day'
                            ,'type': 'numerical'
                        })

                    date = parse_datetime(self.transaction.input_data.data_array[row_ind][col_ind])

                    data[col + '_year'].append(date.year)
                    data[col + '_month'].append(date.month)
                    data[col + '_day'].append(date.day)

                    custom_logic_continue = True

                elif data_subtype in (DATA_SUBTYPES.TIMESTAMP):
                    unix_ts = parse_datetime(self.transaction.input_data.data_array[row_ind][col_ind]).timestamp()
                    data[col].append(unix_ts)

                elif data_subtype in (DATA_SUBTYPES.FLOAT):
                    if type(self.transaction.input_data.data_array[row_ind][col_ind]) == str:
                        data[col].append(float(self.transaction.input_data.data_array[row_ind][col_ind].replace(',','.')))
                    else:
                        data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])

                elif data_subtype in (DATA_SUBTYPES.INT):
                    if type(self.transaction.input_data.data_array[row_ind][col_ind]) == str:
                        data[col].append(round(float(self.transaction.input_data.data_array[row_ind][col_ind].replace(',','.'))))
                    else:
                        data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])

                else:
                    data[col].append(self.transaction.input_data.data_array[row_ind][col_ind])

            if custom_logic_continue:
                continue

            if col not in self.transaction.persistent_model_metadata.predict_columns:
                input_def = {
                    'name': col
                    ,'type': ludwig_dtype
                }
                if encoder is not None:
                    input_def['encoder'] = encoder
                if cell_type is not None:
                    input_def['cell_type'] = cell_type
                if in_memory is not None:
                    input_def['in_memory'] = in_memory

                if height is not None and width is not None:
                    input_def['height'] = height
                    input_def['width'] = width
                    input_def['resize_image'] = True
                    input_def['resize_method'] = 'crop_or_pad'
                    model_definition['preprocessing'] = {
                        'image': {
                            'height': height
                            ,'width': width
                            ,'resize_image': True
                            ,'resize_method': 'crop_or_pad'
                        }
                    }

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
        if self.transaction.persistent_model_metadata.model_order_by is None:
            timeseries_cols = []
        else:
            timeseries_cols = list(map(lambda x: x[0], self.transaction.persistent_model_metadata.model_order_by))

        if len(timeseries_cols) > 0:
            training_dataframe, model_definition =  self._translate_df_to_timeseries_format(training_dataframe, model_definition, timeseries_cols)

        model = LudwigModel(model_definition)

        # Figure out how to pass `model_load_path`
        train_stats = model.train(data_df=training_dataframe, model_name=self.transaction.metadata.model_name)

        #model.model.weights_save_path.rstrip('/model_weights_progress') + '/model'
        ludwig_model_savepath = Config.LOCALSTORE_PATH.rstrip('local_jsondb_store') + self.transaction.metadata.model_name

        model.save(ludwig_model_savepath)
        model.close()

        self.transaction.persistent_model_metadata.ludwig_data = {'ludwig_save_path': ludwig_model_savepath, 'model_definition': model_definition}


    def predict(self, mode='predict', ignore_columns=[]):
        predict_dataframe, model_definition = self._create_ludwig_dataframe(mode)
        model = LudwigModel.load(self.transaction.persistent_model_metadata.ludwig_data['ludwig_save_path'])

        if self.transaction.persistent_model_metadata.model_order_by is None:
            timeseries_cols = []
        else:
            timeseries_cols = list(map(lambda x: x[0], self.transaction.persistent_model_metadata.model_order_by))

        if len(timeseries_cols) > 0:
            predict_dataframe, model_definition =  self._translate_df_to_timeseries_format(predict_dataframe, model_definition, timeseries_cols)

        for ignore_col in ignore_columns:
            predict_dataframe[ignore_col] = [None] * len(predict_dataframe[ignore_col])

        predictions = model.predict(data_df=predict_dataframe)
        print(predictions)
        exit()
        for col_name in predictions:
            col_name_normalized = col_name.replace('_predictions', '')
            predictions = predictions.rename(columns = {col_name: col_name_normalized})

        return predictions
