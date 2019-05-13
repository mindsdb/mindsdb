import shutil
import zipfile
import os
import _thread
import uuid
import traceback
import pickle

from mindsdb.libs.data_types.mindsdb_logger import MindsdbLogger
from mindsdb.libs.helpers.multi_data_source import getDS
from mindsdb.libs.helpers.general_helpers import check_for_updates
from mindsdb.__about__ import __version__

from mindsdb.config import CONFIG
from mindsdb.libs.controllers.transaction import Transaction
from mindsdb.libs.constants.mindsdb import *


from pathlib import Path

class Predictor:

    def __init__(self, name, root_folder=CONFIG.MINDSDB_STORAGE_PATH, log_level=CONFIG.DEFAULT_LOG_LEVEL, log_server=CONFIG.MINDSDB_SERVER_URL):
        """
        This controller defines the API to a MindsDB 'mind', a mind is an object that can learn and predict from data

        :param name: the namespace you want to identify this mind instance with
        :param root_folder: the folder where you want to store this mind or load from
        :param log_level: the desired log level
        :param log_server: the url for a server that can accept log streams

        """

        # initialize variables
        self.name = name
        self.root_folder = root_folder
        self.uuid = str(uuid.uuid1())
        # initialize log
        self.log = MindsdbLogger(log_level=log_level, send_logs=False, log_url=log_server, uuid=self.uuid)

        # check for updates
        _thread.start_new_thread(check_for_updates, ())

        # set the mindsdb storage folder
        storage_ok = True  # default state

        # if it does not exist try to create it
        if not os.path.exists(CONFIG.MINDSDB_STORAGE_PATH):
            try:
                self.log.info('{folder} does not exist, creating it now'.format(folder=CONFIG.MINDSDB_STORAGE_PATH))
                path = Path(CONFIG.MINDSDB_STORAGE_PATH)
                path.mkdir(exist_ok=True, parents=True)
            except:
                self.log.info(traceback.format_exc())
                storage_ok = False
                self.log.error('MindsDB storage foldler: {folder} does not exist and could not be created'.format(
                    folder=CONFIG.MINDSDB_STORAGE_PATH))

        # If storage path is not writable, raise an exception as this can no longer be
        if not os.access(CONFIG.MINDSDB_STORAGE_PATH, os.W_OK) or storage_ok == False:
            error_message = '''Cannot write into storage path, please either set the config variable mindsdb.config.set('MINDSDB_STORAGE_PATH',<path>) or give write access to {folder}'''
            raise ValueError(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))

    def get_models(self):
        models = []
        for fn in os.listdir(CONFIG.MINDSDB_STORAGE_PATH):
            if '_light_model_metadata.pickle' in fn:
                model_name = fn.replace('_light_model_metadata.pickle','')
                lmd = self.get_model_data(model_name)
                model = {}
                for k in ['name', 'version', 'is_active', 'data_source', 'predict', 'accuracy',
                'status', 'train_end_at', 'updated_at', 'created_at']:
                    if k in lmd:
                        model[k] = lmd[k]
                    else:
                        model[k] = None
                        print(f'Key {k} not found in the light model metadata !')
                models.append(model)
        return models

    def _adapt_column(self, col_stats, col):
        icm = {}
        icm['column_name'] = col
        icm['data_type'] = col_stats['data_type']
        icm['data_subtype'] = col_stats['data_subtype']

        icm['data_type_distribution'] = {
            'type': "categorical"
            ,'x': []
            ,'y': []
        }
        for k in col_stats['data_type_dist']:
            icm['data_type_distribution']['x'].append(k)
            icm['data_type_distribution']['y'].append(col_stats['data_type_dist'][k])

        icm['data_subtype_distribution'] = {
            'type': "categorical"
            ,'x': []
            ,'y': []
        }
        for k in col_stats['data_subtype_dist']:
            icm['data_subtype_distribution']['x'].append(k)
            icm['data_subtype_distribution']['y'].append(col_stats['data_subtype_dist'][k])

        icm['data_distribution'] = {}
        icm['data_distribution']['data_histogram'] = {
            "type": "categorical",
            'x': [],
            'y': []
        }
        icm['data_distribution']['clusters'] =  [
             {
                 "group": [],
                 "members": []
             }
         ]


        for i in range(len(col_stats['histogram']['x'])):
            icm['data_distribution']['data_histogram']['x'].append(col_stats['histogram']['x'][i])
            icm['data_distribution']['data_histogram']['y'].append(col_stats['histogram']['y'][i])

        scores = ['consistency_score', 'redundancy_score', 'variability_score']
        for score in scores:
            metrics = []
            if score == 'consistency_score':
                metrics.append({
                      "type": "score",
                      "score": col_stats['data_type_distribution_score'],
                      "description": col_stats['data_type_distribution_score_description'],
                      "warning": col_stats['data_type_distribution_score_warning']
                })
                metrics.append({
                      "type": "score",
                      "score": col_stats['empty_cells_score'],
                      "description": col_stats['empty_cells_score_description'],
                      "warning": col_stats['empty_cells_score_warning']
                })
                if 'duplicates_score' in col_stats:
                    metrics.append({
                          "type": "score",
                          "score": col_stats['duplicates_score'],
                          "description": col_stats['duplicates_score_description'],
                          "warning": col_stats['duplicates_score_warning']
                    })

            if score == 'variability_score':
                if 'lof_based_outlier_score' in col_stats and 'z_test_based_outlier_score' in col_stats:
                    metrics.append({
                          "type": "score",
                          "score": col_stats['lof_based_outlier_score'],
                          "description": col_stats['lof_based_outlier_score_description'],
                          "warning": col_stats['lof_based_outlier_score_warning']
                    })
                    metrics.append({
                          "type": "score",
                          "score": col_stats['z_test_based_outlier_score'],
                          "description": col_stats['z_test_based_outlier_score_description'],
                          "warning": col_stats['z_test_based_outlier_score_warning']
                    })
                    metrics.append({
                          "type": "score",
                          "score": col_stats['value_distribution_score'],
                          "description": col_stats['value_distribution_score_description'],
                          "warning": col_stats['value_distribution_score_warning']
                    })
                else:
                    metrics.append({
                          "type": "score",
                          "score": col_stats['value_distribution_score'],
                          "description": col_stats['value_distribution_score_description'],
                          "warning": col_stats['value_distribution_score_warning']
                    })

            if score == 'redundancy_score':
                metrics.append({
                      "type": "score",
                      "score": col_stats['similarity_score'],
                      "description": col_stats['similarity_score_description'],
                      "warning": col_stats['similarity_score_warning']
                })


            icm[score.replace('_score','')] = {
                'score': col_stats[score],
                'metrics': metrics,
                "description": col_stats[f'{score}_description'],
                "warning": col_stats[f'{score}_warning']
            }

        return icm

    def get_model_data(self, model_name):
        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, f'{model_name}_light_model_metadata.pickle'), 'rb') as fp:
            lmd = pickle.load(fp)
        # ADAPTOR CODE
        amd = {}

        # Shared keys
        for k in ['name', 'version', 'is_active', 'data_source', 'predict', 'accuracy',
        'status', 'train_end_at', 'updated_at', 'created_at','data_preparation']:
            if k == 'predict':
                amd[k] = lmd['predict_columns']
            elif k in lmd:
                amd[k] = lmd[k]
            else:
                amd[k] = None
                print(f'Key {k} not found in the light model metadata !')

        amd['data_analysis'] = {
            'target_columns_metadata': []
            ,'input_columns_metadata': []
        }

        amd['model_analysis'] = []

        for col in lmd['model_columns_map'].keys():
            if col in lmd['malformed_columns']['names']:
                continue

            try:
                icm = self._adapt_column(lmd['column_stats'][col],col)
            except:
                print(f'Issue processing column: {col} !')
                continue

            amd['force_vectors'] = {}
            if col in lmd['predict_columns']:
                # Histograms for plotting the force vectors
                amd['force_vectors'][col] = {}
                amd['force_vectors'][col]['normal_data_distribution'] = lmd['all_columns_prediction_distribution'][col]
                amd['force_vectors'][col]['normal_data_distribution']['type'] = 'categorical'

                amd['force_vectors'][col]['missing_data_distribution'] = {}
                for missing_column in lmd['columnless_prediction_distribution'][col]:
                    amd['force_vectors'][col]['missing_data_distribution'][missing_column] = lmd['columnless_prediction_distribution'][col][missing_column]
                    amd['force_vectors'][col]['missing_data_distribution'][missing_column]['type'] = 'categorical'

                icm['importance_score'] = None
                amd['data_analysis']['target_columns_metadata'].append(icm)

                # Model analysis building for each of the predict columns
                mao = {
                    'column_name': col
                    ,'overall_input_importance': {
                        "type": "categorical"
                        ,"x": []
                        ,"y": []
                    }
                  ,"train_accuracy_over_time": {
                    "type": "categorical",
                    "x": [],
                    "y": []
                  }
                  ,"test_accuracy_over_time": {
                    "type": "categorical",
                    "x": [],
                    "y": []
                  }
                  ,"accuracy_histogram": {
                        "x": []
                        ,"y": []
                        ,'x_explained': []
                  }
                }

                train_acc = lmd['model_accuracy']['train']['combined']
                test_acc = lmd['model_accuracy']['test']['combined']

                for i in range(0,len(train_acc)):
                    mao['train_accuracy_over_time']['x'].append(i)
                    mao['train_accuracy_over_time']['y'].append(train_acc[i])

                for i in range(0,len(test_acc)):
                    mao['test_accuracy_over_time']['x'].append(i)
                    mao['test_accuracy_over_time']['y'].append([i])

                mao['accuracy_histogram']['x'] = []
                mao['accuracy_histogram']['y'] = []

                bucket_importance_keys = list(lmd['unusual_columns_buckets_importances'].keys())
                for incol in lmd['column_importances']:
                    incol_bucket_importance_keys = list(filter(lambda x: incol in x, bucket_importance_keys))

                    mao['accuracy_histogram']['x'].append(incol)
                    mao['accuracy_histogram']['y'].append(lmd['column_importances'][incol])

                    if len(incol_bucket_importance_keys) > 0:
                        sub_group_stats = []
                        for sub_incol in incol_bucket_importance_keys:
                            sub_group_stats.append(self._adapt_column(lmd['unusual_columns_buckets_importances'][sub_incol], sub_incol))
                    else:
                        sub_group_stats = [None]
                    mao['accuracy_histogram']['x_explained'].append(sub_group_stats)

                for icol in lmd['model_columns_map'].keys():
                    if icol in lmd['malformed_columns']['names']:
                        continue
                    if icol not in lmd['predict_columns']:
                        try:
                            mao['overall_input_importance']['x'].append(icol)
                            mao['overall_input_importance']['y'].append(lmd['column_importances'][icol])
                        except:
                            print(f'No column importances found for {icol} !')

                amd['model_analysis'].append(mao)
            else:
                icm['importance_score'] = lmd['column_importances'][col]
                amd['data_analysis']['input_columns_metadata'].append(icm)



        # ADAPTOR CODE

        return amd

    def export(self, mindsdb_storage_dir='mindsdb_storage'):
        """
        If you want to export this mindsdb's instance storage to a file

        :param mindsdb_storage_dir: this is the full_path where you want to store a mind to, it will be a zip file
        :return: bool (True/False) True if mind was exported successfully
        """
        try:
            shutil.make_archive(base_name=mindsdb_storage_dir, format='zip', root_dir=CONFIG.MINDSDB_STORAGE_PATH)
            print(f'Exported mindsdb storage to {mindsdb_storage_dir}.zip')
            return True
        except:
            return False

    def export_model(self, model_name):
        """
        If you want to export a model to a file

        :param model_name: this is the name of the model you wish to export (defaults to the name of the current Predictor)
        :return: bool (True/False) True if mind was exported successfully
        """
        if model_name is None:
            model_name = self.name
        try:
            storage_file = model_name + '.zip'
            with zipfile.ZipFile(storage_file, 'w') as zip_fp:
                for file_name in [model_name + '_heavy_model_metadata.pickle', model_name + '_light_model_metadata.pickle']:
                    full_path = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, file_name)
                    zip_fp.write(full_path, os.path.basename(full_path))
            print(f'Exported model to {storage_file}')
            return True
        except Exception as e:
            print(e)
            return False

    def load(self, mindsdb_storage_dir='mindsdb_storage.zip'):
        """
        If you want to import a mindsdb instance storage from a file

        :param mindsdb_storage_dir: this is the full_path that contains your mind
        :return: bool (True/False) True if mind was importerd successfully
        """
        shutil.unpack_archive(mindsdb_storage_dir, extract_dir=CONFIG.MINDSDB_STORAGE_PATH)


    def load_model(self, model_archive_path=None):
        """
        If you want to load a model to a file

        :param model_archive_path: this is the path to the archive where your model resides
        :return: bool (True/False) True if mind was importerd successfully
        """
        shutil.unpack_archive(model_archive_path, extract_dir=CONFIG.MINDSDB_STORAGE_PATH)


    def learn(self, to_predict, from_data = None, test_from_data=None, group_by = None, window_size_samples = None, window_size_seconds = None,
    window_size = None, order_by = [], sample_margin_of_error = CONFIG.DEFAULT_MARGIN_OF_ERROR, ignore_columns = [], rename_strange_columns = False,
    stop_training_in_x_seconds = None, stop_training_in_accuracy = None,  send_logs=CONFIG.SEND_LOGS, backend='ludwig', rebuild_model=True):
        """
        Tells the mind to learn to predict a column or columns from the data in 'from_data'

        Mandatory arguments:
        :param to_predict: what column or columns you want to predict
        :param from_data: the data that you want to learn from, this can be either a file, a pandas data frame, or url to a file

        Optional arguments:
        :param test_from_data: If you would like to test this learning from a different data set

        Optional Time series arguments:
        :param order_by: this order by defines the time series, it can be a list. By default it sorts each sort by column in ascending manner, if you want to change this pass a touple ('column_name', 'boolean_for_ascending <default=true>')
        :param group_by: This argument tells the time series that it should learn by grouping rows by a given id
        :param window_size: The number of samples to learn from in the time series

        Optional data transformation arguments:
        :param ignore_columns: it simply removes the columns from the data sources
        :param rename_strange_columns: this tells mindsDB that if columns have special characters, it should try to rename them, this is a legacy argument, as now mindsdb supports any column name

        Optional sampling parameters:
        :param sample_margin_error (DEFAULT 0): Maximum expected difference between the true population parameter, such as the mean, and the sample estimate.

        Optional debug arguments:
        :param send_logs: If you want to stream these logs to a server
        :param stop_training_in_x_seconds: (default None), if set, you want training to finish in a given number of seconds

        :return:
        """

        # Backwards compatibility of interface
        if window_size is not None:
            window_size_samples = window_size
        #

        from_ds = getDS(from_data)
        test_from_ds = test_from_data if test_from_data is None else getDS(test_from_data)
        breakpoint = CONFIG.DEBUG_BREAK_POINT
        transaction_type = TRANSACTION_LEARN
        sample_confidence_level = 1 - sample_margin_of_error
        predict_columns_map = {}

        # lets turn into lists: predict, order_by and group by
        predict_columns = [to_predict] if type(to_predict) != type([]) else to_predict
        group_by = group_by if type(group_by) == type([]) else [group_by] if group_by else []
        order_by = order_by if type(order_by) == type([]) else [order_by] if order_by else []

        if len(predict_columns) == 0:
            error = 'You need to specify a column to predict'
            self.log.error(error)
            raise ValueError(error)

        # lets turn order by into tuples if not already
        # each element ('column_name', 'boolean_for_ascending <default=true>')
        order_by = [(col_name, True) if type(col_name) != type(()) else col_name for col_name in order_by]

        is_time_series = True if len(order_by) > 0 else False

        if rename_strange_columns is False:
            for predict_col in predict_columns:
                predict_col_as_in_df = from_ds.getColNameAsInDF(predict_col)
                predict_columns_map[predict_col_as_in_df]=predict_col

            predict_columns = list(predict_columns_map.keys())
        else:
            self.log.warning('Note that after version 1.0, the default value for argument rename_strange_columns in MindsDB().learn, will be flipped from True to False, this means that if your data has columns with special characters, MindsDB will not try to rename them by default.')

        heavy_transaction_metadata = {}
        heavy_transaction_metadata['name'] = self.name
        heavy_transaction_metadata['from_data'] = from_ds
        heavy_transaction_metadata['test_from_data'] = test_from_ds
        heavy_transaction_metadata['bucketing_algorithms'] = {}

        light_transaction_metadata = {}
        light_transaction_metadata['version'] = str(__version__)
        light_transaction_metadata['name'] = self.name
        light_transaction_metadata['data_preparation'] = {}
        light_transaction_metadata['model_backend'] = backend
        light_transaction_metadata['predict_columns'] = predict_columns
        light_transaction_metadata['model_columns_map'] = {} if rename_strange_columns else from_ds._col_map
        light_transaction_metadata['model_group_by'] = group_by
        light_transaction_metadata['model_order_by'] = order_by
        light_transaction_metadata['window_size_samples'] = window_size_samples
        light_transaction_metadata['window_size_seconds'] = window_size_seconds
        light_transaction_metadata['model_is_time_series'] = is_time_series
        light_transaction_metadata['data_source'] = from_data
        light_transaction_metadata['type'] = transaction_type
        light_transaction_metadata['ignore_columns'] = ignore_columns
        light_transaction_metadata['sample_margin_of_error'] = sample_margin_of_error
        light_transaction_metadata['sample_confidence_level'] = sample_confidence_level
        light_transaction_metadata['stop_training_in_x_seconds'] = stop_training_in_x_seconds
        light_transaction_metadata['stop_training_in_accuracy'] = stop_training_in_accuracy
        light_transaction_metadata['rebuild_model'] = rebuild_model
        light_transaction_metadata['model_accuracy'] = {'train': {}, 'test': {}}
        light_transaction_metadata['column_importances'] = None
        light_transaction_metadata['unusual_columns_buckets_importances'] = None
        light_transaction_metadata['columnless_prediction_distribution'] = None
        light_transaction_metadata['all_columns_prediction_distribution'] = None
        light_transaction_metadata['malformed_columns'] = {'names': [], 'indices': []}


        if rebuild_model is False:
            old_lmd = {}
            for k in light_transaction_metadata: old_lmd[k] = light_transaction_metadata[k]

            old_hmd = {}
            for k in heavy_transaction_metadata: old_hmd[k] = heavy_transaction_metadata[k]

            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, light_transaction_metadata['name'] + '_light_model_metadata.pickle'), 'rb') as fp:
                light_transaction_metadata = pickle.load(fp)

            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, heavy_transaction_metadata['name'] + '_heavy_model_metadata.pickle'), 'rb') as fp:
                heavy_transaction_metadata= pickle.load(fp)

            for k in ['data_preparation', 'rebuild_model', 'data_source', 'type', 'ignore_columns', 'sample_margin_of_error', 'sample_confidence_level', 'stop_training_in_x_seconds', 'stop_training_in_accuracy']:
                if old_lmd[k] is not None: light_transaction_metadata[k] = old_lmd[k]

            for k in ['from_data', 'test_from_data']:
                if old_hmd[k] is not None: heavy_transaction_metadata[k] = old_hmd[k]

        Transaction(session=self, light_transaction_metadata=light_transaction_metadata, heavy_transaction_metadata=heavy_transaction_metadata, logger=self.log, breakpoint=breakpoint)


    def predict(self, when={}, when_data = None, update_cached_model = False):
        """
        You have a mind trained already and you want to make a prediction

        :param when: use this if you have certain conditions for a single prediction
        :param when_data: (optional) use this when you have data in either a file, a pandas data frame, or url to a file that you want to predict from
        :param update_cached_model: (optional, default:False) when you run predict for the first time, it loads the latest model in memory, you can force it to do this on this run by flipping it to True

        :return: TransactionOutputData object
        """

        transaction_type = TRANSACTION_PREDICT
        breakpoint = CONFIG.DEBUG_BREAK_POINT
        when_ds = None if when_data is None else getDS(when_data)


        # lets turn into lists: when
        when = [when] if type(when) in [type(None), type({})] else when

        heavy_transaction_metadata = {}
        if when_ds is None:
            heavy_transaction_metadata['when_data'] = None
        else:
            heavy_transaction_metadata['when_data'] = when_ds
        heavy_transaction_metadata['model_when_conditions'] = when
        heavy_transaction_metadata['name'] = self.name

        light_transaction_metadata = {}
        light_transaction_metadata['name'] = self.name
        light_transaction_metadata['type'] = transaction_type
        light_transaction_metadata['data_preparation'] = {}

        transaction = Transaction(session=self, light_transaction_metadata=light_transaction_metadata, heavy_transaction_metadata=heavy_transaction_metadata, breakpoint=breakpoint)

        return transaction.output_data
