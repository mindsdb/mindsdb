from dateutil.parser import parse as parse_datetime
import os, sys
import shutil
import subprocess

from mindsdb.libs.constants.mindsdb import *
from mindsdb.config import *
from mindsdb.libs.helpers.general_helpers import disable_console_output, get_tensorflow_colname

from tensorflow.python.client import device_lib
from ludwig.api import LudwigModel
from ludwig.data.preprocessing import build_metadata
import pandas as pd
from imageio import imread


class LudwigBackend():

    def __init__(self, transaction):
        try:
            subprocess.call(['python3','-m','spacy','download','en_core_web_sm'])
        except:
            try:
                subprocess.call(['python','-m','spacy','download','en_core_web_sm'])
            except:
                print('Can\'t download spacy vocabulary, ludwig backend may fail when processing text input')
        try:
            subprocess.call(['python3','-m','spacy','download','en'])
        except:
            try:
                subprocess.call(['python','-m','spacy','download','en'])
            except:
                print('Can\'t download spacy vocabulary, ludwig backend may fail when processing text input')

        self.transaction = transaction

    def _translate_df_to_timeseries_format(self, df, model_definition, timeseries_cols, mode='predict'):
        timeseries_col_name = timeseries_cols[0]

        previous_predict_col_names = []
        predict_col_names = []
        for feature_def in model_definition['output_features']:
            if mode == 'train':
                predict_col_names.append(feature_def['name'])
            else:
                predict_col_names = []
            previous_predict_col_name = 'previous_' + feature_def['name']

            previous_predict_col_already_in = False

            for definition in model_definition['input_features']:
                if definition['name'] == previous_predict_col_name:
                     previous_predict_col_already_in = True

            if not previous_predict_col_already_in:
                model_definition['input_features'].append({
                    'name': previous_predict_col_name
                    ,'type': 'sequence'
                })

        other_col_names = []
        for feature_def in model_definition['input_features']:
            if feature_def['name'] not in self.transaction.lmd['model_group_by'] and feature_def['name'] not in previous_predict_col_names:
                feature_def['type'] = 'sequence'
                if feature_def['name'] not in [timeseries_col_name]:
                    other_col_names.append(feature_def['name'])


            previous_predict_col_names.append(previous_predict_col_name)

        new_cols = {}
        for col in [*other_col_names,*previous_predict_col_names,timeseries_col_name,*predict_col_names,*self.transaction.lmd['model_group_by']]:
            new_cols[col] = []

        nr_ele = len(df[timeseries_col_name])

        i = 0
        while i < nr_ele:
            new_row = {}

            timeseries_row = [df[timeseries_col_name][i]]

            for col in other_col_names:
                new_row[col] = [df[col][i]]
            for col in previous_predict_col_names:
                new_row[col] = []
            for col in predict_col_names:
                new_row[col] = df[col][i]
            for col in self.transaction.lmd['model_group_by']:
                new_row[col] = df[col][i]

            inverted_index_range = list(range(i))
            inverted_index_range.reverse()
            ii = 0
            for ii in inverted_index_range:
                if (i - ii) > self.transaction.lmd['window_size']:
                    break
                timeseries_row.append(df[timeseries_col_name][ii])

                for col in other_col_names:
                    new_row[col].append(df[col][ii])
                for col in previous_predict_col_names:
                    try:
                        new_row[col].append(df[col.replace('previous_', '')][ii])
                    except:
                        try:
                            new_row[col].append(df[col][ii])
                        except:
                            self.transaction.log.warning('Missing previous predicted values for output column: {}, these should be included in your input under the name: {}'.format(col.replace('previous_', ''), col))

            if mode == 'train':
                i = max(i + 1, (i + round((i - ii)/2)))
            else:
                i = i + 1

            new_row[timeseries_col_name] = timeseries_row

            for col in new_row:
                if col not in predict_col_names and col not in self.transaction.lmd['model_group_by']:
                    new_row[col].reverse()
                new_cols[col].append(new_row[col])

        new_df = pd.DataFrame(data=new_cols)
        df = new_df
        return df, model_definition

    def _create_ludwig_dataframe(self, mode):
        has_heavy_data = False
        col_map = {}

        if mode == 'train':
            df = self.transaction.input_data.train_df
        elif mode == 'predict':
            df = self.transaction.input_data.data_frame
        elif mode == 'validate':
            df = self.transaction.input_data.validation_df
        elif mode == 'test':
            df = self.transaction.input_data.test_df
        else:
            raise Exception(f'Unknown mode specified: "{mode}"')
        model_definition = {'input_features': [], 'output_features': []}
        data = {}

        if self.transaction.lmd['model_order_by'] is None:
            timeseries_cols = []
        else:
            timeseries_cols = list(map(lambda x: x[0], self.transaction.lmd['model_order_by']))

        for col in df.columns:
            tf_col = get_tensorflow_colname(col)
            col_map[tf_col] = col

            # Handle malformed columns
            if col in self.transaction.lmd['columns_to_ignore']:
                continue

            data[tf_col] = []

            col_stats = self.transaction.lmd['column_stats'][col]
            data_subtype = col_stats['data_subtype']

            ludwig_dtype = None
            encoder = None
            cell_type = None
            in_memory = None
            height = None
            width = None

            if col in timeseries_cols:
                encoder = 'rnn'
                cell_type = 'rnn'
                ludwig_dtype = 'order_by_col'

            if data_subtype in DATA_SUBTYPES.ARRAY:
                encoder = 'rnn'
                cell_type = 'rnn'
                ludwig_dtype = 'sequence'

            elif data_subtype in (DATA_SUBTYPES.INT, DATA_SUBTYPES.FLOAT):
                ludwig_dtype = 'numerical'

            elif data_subtype in (DATA_SUBTYPES.BINARY):
                ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.DATE):
                if col not in self.transaction.lmd['predict_columns']:
                    ludwig_dtype = 'date'
                else:
                    ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.TIMESTAMP):
                ludwig_dtype = 'numerical'

            elif data_subtype in (DATA_SUBTYPES.SINGLE, DATA_SUBTYPES.MULTIPLE):
                ludwig_dtype = 'category'

            elif data_subtype in (DATA_SUBTYPES.IMAGE):
                has_heavy_data = True
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

            for index, row in df.iterrows():
                if ludwig_dtype == 'order_by_col':
                    ts_data_point = row[col]

                    try:
                        ts_data_point = float(ts_data_point)
                    except:
                        ts_data_point = parse_datetime(ts_data_point).timestamp()
                    data[tf_col].append(ts_data_point)

                elif ludwig_dtype == 'sequence':
                    arr_str = row[col]
                    if arr_str is not None:
                        arr = list(map(float,arr_str.rstrip(']').lstrip('[').split(self.transaction.lmd['column_stats'][col]['separator'])))
                    else:
                        arr = ''
                    data[tf_col].append(arr)

                # Date isn't supported yet, so we hack around it
                elif ludwig_dtype == 'date':
                    if col in data:
                        data.pop(col)
                        data[tf_col + '_year'] = []
                        data[tf_col + '_month'] = []
                        data[tf_col + '_day'] = []

                        model_definition['input_features'].append({
                            'name': col + '_year'
                            ,'type': 'category'
                        })
                        model_definition['input_features'].append({
                            'name': col + '_month'
                            ,'type': 'category'
                        })
                        model_definition['input_features'].append({
                            'name': col + '_day'
                            ,'type': 'numerical'
                        })

                    date = parse_datetime(row[col])

                    data[tf_col + '_year'].append(date.year)
                    data[tf_col + '_month'].append(date.month)
                    data[tf_col + '_day'].append(date.day)

                    custom_logic_continue = True

                    if col in timeseries_cols:
                        timeseries_cols.remove(col)
                        timeseries_cols.append(col + '_day')
                        timeseries_cols.append(col + '_month')
                        timeseries_cols.append(col + '_year')

                elif data_subtype in (DATA_SUBTYPES.TIMESTAMP):
                    if row[col] is None:
                        unix_ts = 0
                    else:
                        unix_ts = parse_datetime(row[col]).timestamp()

                    data[tf_col].append(unix_ts)

                elif data_subtype in (DATA_SUBTYPES.FLOAT):
                    if isinstance(row[col], str):
                        data[tf_col].append(float(str(row[col]).replace(',', '.')))
                    else:
                        data[tf_col].append(row[col])

                elif data_subtype in (DATA_SUBTYPES.INT):
                    if isinstance(row[col], str):
                        data[tf_col].append(round(float(str(row[col]).replace(',', '.'))))
                    else:
                        data[tf_col].append(row[col])

                elif data_subtype in (DATA_SUBTYPES.IMAGE):
                    if os.path.isabs(row[col]):
                        data[tf_col].append(row[col])
                    else:
                        data[tf_col].append(os.path.join(os.getcwd(), row[col]))
                else:
                    data[tf_col].append(row[col])

            if custom_logic_continue:
                continue

            if col not in self.transaction.lmd['predict_columns']:
                input_def = {
                    'name': tf_col
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
                            ,'num_channels': 3
                        }
                    }

                model_definition['input_features'].append(input_def)
            else:
                output_def = {
                    'name': tf_col
                    ,'type': ludwig_dtype
                }
                model_definition['output_features'].append(output_def)

        df = pd.DataFrame(data=data)
        if len(timeseries_cols) > 0:
            df.sort_values(timeseries_cols)

        return df, model_definition, timeseries_cols, has_heavy_data, col_map

    def _get_model_dir(self):
        model_dir = None
        for thing in os.listdir(self.transaction.lmd['ludwig_data']['ludwig_save_path']):
            if 'api_experiment' in thing:
                model_dir = os.path.join(self.transaction.lmd['ludwig_data']['ludwig_save_path'],thing,'model')
        if model_dir is None:
            model_dir = os.path.join(self.transaction.lmd['ludwig_data']['ludwig_save_path'],'model')
        return model_dir

    def _get_useable_gpus(self):
        if self.transaction.lmd['use_gpu'] == False:
            return []
        local_device_protos = device_lib.list_local_devices()
        gpus = [x for x in local_device_protos if x.device_type == 'GPU']
        #bus_ids = [x.locality.bus_id for x in gpus]
        gpu_indices = [i for i in range(len(gpus))]
        if len(gpu_indices) == 0:
            return None
        else:
            return gpu_indices

    def train(self):
        training_dataframe, model_definition, timeseries_cols, has_heavy_data, self.transaction.lmd['ludwig_tf_self_col_map'] = self._create_ludwig_dataframe('train')

        if len(timeseries_cols) > 0:
            training_dataframe, model_definition =  self._translate_df_to_timeseries_format(training_dataframe, model_definition, timeseries_cols, 'train')

        with disable_console_output(True):
            # <---- Ludwig currently broken, since mode can't be initialized without train_set_metadata and train_set_metadata can't be obtained without running train... see this issue for any updates on the matter: https://github.com/uber/ludwig/issues/295
            #model.initialize_model(train_set_metadata={})
            #train_stats = model.train_online(data_df=training_dataframe) # ??Where to add model_name?? ----> model_name=self.transaction.lmd['name']

            ludwig_save_is_working = False

            if not ludwig_save_is_working:
                shutil.rmtree('results',ignore_errors=True)

            if self.transaction.lmd['rebuild_model'] is True:
                model = LudwigModel(model_definition)
                merged_model_definition = model.model_definition
                train_set_metadata = build_metadata(
                    training_dataframe,
                    (merged_model_definition['input_features'] +
                    merged_model_definition['output_features']),
                    merged_model_definition['preprocessing']
                )
                model.initialize_model(train_set_metadata=train_set_metadata, gpus=self._get_useable_gpus())
            else:
                model = LudwigModel.load(model_dir=self._get_model_dir())


            split_by = int(20 * pow(10,6))
            if has_heavy_data:
                split_by = 40
            df_len = len(training_dataframe[training_dataframe.columns[0]])
            if df_len > split_by:
                i = 0
                while i < df_len:
                    end = i + split_by
                    self.transaction.log.info(f'Training with batch from index {i} to index {end}')
                    training_sample = training_dataframe.iloc[i:end]
                    training_sample = training_sample.reset_index()

                    if len(training_sample) < 1:
                        continue

                    train_stats = model.train(data_df=training_sample, model_name=self.transaction.lmd['name'], skip_save_model=ludwig_save_is_working, skip_save_progress=True, gpus=self._get_useable_gpus())
                    i = end
            else:
                train_stats = model.train(data_df=training_dataframe, model_name=self.transaction.lmd['name'], skip_save_model=ludwig_save_is_working, skip_save_progress=True, gpus=self._get_useable_gpus())

            for k in train_stats['train']:
                if k not in self.transaction.lmd['model_accuracy']['train']:
                    self.transaction.lmd['model_accuracy']['train'][k] = []
                    self.transaction.lmd['model_accuracy']['test'][k] = []
                elif k != 'combined':
                    # We should be adding the accuracy here but we only have it for combined, so, for now use that, will only affect multi-output scenarios anyway
                    pass
                else:
                    self.transaction.lmd['model_accuracy']['train'][k].extend(train_stats['train'][k]['accuracy'])
                    self.transaction.lmd['model_accuracy']['test'][k].extend(train_stats['test'][k]['accuracy'])

            '''
            @ TRAIN ONLINE BIT That's not working
            model = LudwigModel.load(self.transaction.lmd['ludwig_data']['ludwig_save_path'])
            for i in range(0,100):
                train_stats = model.train_online(data_df=training_dataframe)
                # The resulting train_stats are "None"... wonderful -_-
            '''

        ludwig_model_savepath = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.transaction.lmd['name'] + '_ludwig_data')
        if ludwig_save_is_working:
            model.save(ludwig_model_savepath)
            model.close()
        else:
            shutil.rmtree(ludwig_model_savepath,ignore_errors=True)
            shutil.move(os.path.join('results',os.listdir('results')[0]),ludwig_model_savepath)
        self.transaction.lmd['ludwig_data'] = {'ludwig_save_path': ludwig_model_savepath}
        self.transaction.hmd['ludwig_data'] = {'model_definition': model_definition}

    def predict(self, mode='predict', ignore_columns=None):
        if ignore_columns is None:
            ignore_columns = []

        predict_dataframe, model_definition, timeseries_cols, has_heavy_data, _ = self._create_ludwig_dataframe(mode)
        model_definition = self.transaction.hmd['ludwig_data']['model_definition']

        if len(timeseries_cols) > 0:
            predict_dataframe, model_definition =  self._translate_df_to_timeseries_format(predict_dataframe, model_definition, timeseries_cols)

        for ignore_col in ignore_columns:
            for tf_col in self.transaction.lmd['ludwig_tf_self_col_map']:
                if ignore_col == self.transaction.lmd['ludwig_tf_self_col_map'][tf_col]:
                    ignore_col = tf_col
            try:
                predict_dataframe[ignore_col] = [None] * len(predict_dataframe[ignore_col])
            except:
                for date_appendage in ['_year', '_month','_day']:
                    predict_dataframe[ignore_col + date_appendage] = [None] * len(predict_dataframe[ignore_col + date_appendage])

        with disable_console_output(True):
            model_dir = self._get_model_dir()
            model = LudwigModel.load(model_dir=model_dir)
            predictions = model.predict(data_df=predict_dataframe, gpus=self._get_useable_gpus())

        for col_name in predictions:
            col_name_normalized = col_name.replace('_predictions', '')
            if col_name_normalized in self.transaction.lmd['ludwig_tf_self_col_map']:
                col_name_normalized = self.transaction.lmd['ludwig_tf_self_col_map'][col_name_normalized]
            predictions = predictions.rename(columns = {col_name: col_name_normalized})

        return predictions
