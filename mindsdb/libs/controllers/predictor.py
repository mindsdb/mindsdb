import shutil
import zipfile
import os
import uuid
import traceback
import pickle

from mindsdb.libs.data_types.mindsdb_logger import MindsdbLogger
from mindsdb.libs.helpers.multi_data_source import getDS
from mindsdb.__about__ import __version__

from mindsdb.config import CONFIG
from mindsdb.libs.controllers.transaction import Transaction
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import check_for_updates

from pathlib import Path

class Predictor:

    def __init__(self, name, root_folder=CONFIG.MINDSDB_STORAGE_PATH, log_level=CONFIG.DEFAULT_LOG_LEVEL):
        """
        This controller defines the API to a MindsDB 'mind', a mind is an object that can learn and predict from data

        :param name: the namespace you want to identify this mind instance with
        :param root_folder: the folder where you want to store this mind or load from
        :param log_level: the desired log level

        """

        # initialize variables
        self.name = name
        self.root_folder = root_folder
        self.uuid = str(uuid.uuid1())
        # initialize log
        self.log = MindsdbLogger(log_level=log_level, uuid=self.uuid)

        if CONFIG.CHECK_FOR_UPDATES:
            try:
                check_for_updates()
            except:
                self.log.warning('Could not check for updates !')

        if not CONFIG.SAGEMAKER:
            # If storage path is not writable, raise an exception as this can no longer be
            if not os.access(CONFIG.MINDSDB_STORAGE_PATH, os.W_OK):
                error_message = '''Cannot write into storage path, please either set the config variable mindsdb.config.set('MINDSDB_STORAGE_PATH',<path>) or give write access to {folder}'''
                self.log.warning(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))
                raise ValueError(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))


            # If storage path is not writable, raise an exception as this can no longer be
            if not os.access(CONFIG.MINDSDB_STORAGE_PATH, os.R_OK):
                error_message = '''Cannot read from storage path, please either set the config variable mindsdb.config.set('MINDSDB_STORAGE_PATH',<path>) or give write access to {folder}'''
                self.log.warning(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))
                raise ValueError(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))

    def get_models(self):
        models = []
        for fn in os.listdir(CONFIG.MINDSDB_STORAGE_PATH):
            if '_light_model_metadata.pickle' in fn:
                model_name = fn.replace('_light_model_metadata.pickle','')
                try:
                    amd = self.get_model_data(model_name)
                    model = {}
                    for k in ['name', 'version', 'is_active', 'data_source', 'predict',
                    'status', 'train_end_at', 'updated_at', 'created_at','current_phase', 'accuracy']:
                        if k in amd:
                            model[k] = amd[k]
                        else:
                            model[k] = None

                    models.append(model)
                except Exception as e:
                    print(e)
                    print(traceback.format_exc())
                    print(f"Can't adapt metadata for model: '{model_name}' when calling `get_models()`")

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
                simple_description = "A low value indicates the data is not very consistent, it's either missing a lot of valus or the type (e.g. number, text, category, date) of values varries quite a lot."
                metrics.append({
                      "type": "score",
                      "name": "Type Distribution",
                      "score": col_stats['data_type_distribution_score'],
                      #"description": col_stats['data_type_distribution_score_description'],
                      "description": "A low value indicates that we can't consistently determine a single data type (e.g. number, text, category, date) for most values in this column",
                      "warning": col_stats['data_type_distribution_score_warning']
                })
                metrics.append({
                      "type": "score",
                      "score": col_stats['empty_cells_score'],
                      "name": "Empty Cells",
                      #"description": col_stats['empty_cells_score_description'],
                      "description": "A low value indicates that a lot of the values in this column are empty or null. A value of 10 means no cell is missing data, a value of 0 means no cell has any data.",
                      "warning": col_stats['empty_cells_score_warning']
                })
                if 'duplicates_score' in col_stats:
                    metrics.append({
                          "type": "score",
                          "name": "Value Duplication",
                          "score": col_stats['duplicates_score'],
                          #"description": col_stats['duplicates_score_description'],
                          "description": "A low value indicates that a lot of the values in this columns are duplicates, as in, the same value shows up more than once in the column. This is not necessarily bad and could be normal for certain data types.",
                          "warning": col_stats['duplicates_score_warning']
                    })

            if score == 'variability_score':
                simple_description = "A low value indicates a high possibility of some noise affecting your data collection process. This could mean that the values for this column are not collected or processed correctly."
                if 'lof_based_outlier_score' in col_stats and 'z_test_based_outlier_score' in col_stats:
                    metrics.append({
                          "type": "score",
                          "name": "Z Outlier Score",
                          "score": col_stats['lof_based_outlier_score'],
                          #"description": col_stats['lof_based_outlier_score_description'],
                          "description": "A low value indicates a large number of outliers in your dataset. This is based on distance from the center of 20 clusters as constructed via KNN.",
                          "warning": col_stats['lof_based_outlier_score_warning']
                    })
                    metrics.append({
                          "type": "score",
                          "name": "Z Outlier Score",
                          "score": col_stats['z_test_based_outlier_score'],
                          #"description": col_stats['z_test_based_outlier_score_description'],
                          "description": "A low value indicates a large number of data points are more than 3 standard deviations away from the mean value of this column. This means that this column likely has a large amount of outliers",
                          "warning": col_stats['z_test_based_outlier_score_warning']
                    })
                metrics.append({
                      "type": "score",
                      "name":"Value Distribution",
                      "score": col_stats['value_distribution_score'],
                      #"description": col_stats['value_distribution_score_description'],
                      "description": "A low value indicates the possibility of a large number of outliers, the clusters in which your data is distributed aren't evenly sized.",
                      "warning": col_stats['value_distribution_score_warning']
                })

            if score == 'redundancy_score':
                # CLF based score to be included here once we find a faster way of computing it...
                similarity_score_based_most_correlated_column = col_stats['most_similar_column_name']

                simple_description = f"A low value indicates that the data in this column is highly redundant (useless) for making any sort of prediction. You should make sure that values heavily related to this column are not already expressed in the \"{similarity_score_based_most_correlated_column}\" column (e.g. if this column is a timestamp, make sure you don't have another column representing the exact same time in ISO datetime format)"


                metrics.append({
                      "type": "score",
                      "name": "Matthews Correlation Score",
                      "score": col_stats['similarity_score'],
                      #"description": col_stats['similarity_score_description'],
                      "description": f"A low value indicates a large number of values in this column are similar to values in the \"{similarity_score_based_most_correlated_column}\" column",
                      "warning": col_stats['similarity_score_warning']
                })

            icm[score.replace('_score','')] = {
                "score": col_stats[score],
                "metrics": metrics,
                #"description": col_stats[f'{score}_description'],
                "description": simple_description,
                "warning": col_stats[f'{score}_warning']
            }

        return icm

    def get_model_data(self, model_name=None, lmd=None):
        if model_name is None:
            model_name = self.name

        if lmd is None:
            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, f'{model_name}_light_model_metadata.pickle'), 'rb') as fp:
                lmd = pickle.load(fp)
        # ADAPTOR CODE
        amd = {}

        if 'stats_v2' in lmd:
            amd['data_analysis_v2'] = lmd['stats_v2']

        if lmd['current_phase'] == MODEL_STATUS_TRAINED:
            amd['status'] = 'complete'
        elif lmd['current_phase'] == MODEL_STATUS_ERROR:
            amd['status'] = 'error'
        else:
            amd['status'] = 'training'

        # Shared keys
        for k in ['name', 'version', 'is_active', 'data_source', 'predict', 'current_phase',
        'train_end_at', 'updated_at', 'created_at','data_preparation', 'validation_set_accuracy']:
            if k == 'predict':
                amd[k] = lmd['predict_columns']
            elif k in lmd:
                amd[k] = lmd[k]
                if k == 'validation_set_accuracy':
                    if lmd['validation_set_accuracy'] is not None:
                        amd['accuracy'] = round(lmd['validation_set_accuracy'],3)
                    else:
                        amd['accuracy'] = None
            else:
                amd[k] = None

        amd['data_analysis'] = {
            'target_columns_metadata': []
            ,'input_columns_metadata': []
        }

        amd['model_analysis'] = []

        for col in lmd['model_columns_map'].keys():
            if col in lmd['columns_to_ignore']:
                continue

            try:
                icm = self._adapt_column(lmd['column_stats'][col],col)
            except Exception as e:
                icm = {'column_name': col}
                #continue

            amd['force_vectors'] = {}
            if col in lmd['predict_columns']:
                # Histograms for plotting the force vectors
                if 'all_columns_prediction_distribution' in lmd and lmd['all_columns_prediction_distribution'] is not None:
                    amd['force_vectors'][col] = {}
                    amd['force_vectors'][col]['normal_data_distribution'] = lmd['all_columns_prediction_distribution'][col]
                    amd['force_vectors'][col]['normal_data_distribution']['type'] = 'categorical'

                    amd['force_vectors'][col]['missing_data_distribution'] = {}
                    for missing_column in lmd['columnless_prediction_distribution'][col]:
                        amd['force_vectors'][col]['missing_data_distribution'][missing_column] = lmd['columnless_prediction_distribution'][col][missing_column]
                        amd['force_vectors'][col]['missing_data_distribution'][missing_column]['type'] = 'categorical'

                    icm['importance_score'] = None
                amd['data_analysis']['target_columns_metadata'].append(icm)

                if 'confusion_matrices' in lmd and col in lmd['confusion_matrices']:
                    confusion_matrix = lmd['confusion_matrices'][col]
                else:
                    confusion_matrix = None

                if 'accuracy_samples' in lmd and col in lmd['accuracy_samples']:
                    accuracy_samples = lmd['accuracy_samples'][col]
                else:
                    accuracy_samples = None



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
                  ,"confusion_matrix": confusion_matrix
                  ,"accuracy_samples": accuracy_samples
                }


                # This is a check to see if model analysis has run on this data
                if 'model_accuracy' in lmd and lmd['model_accuracy'] is not None and 'train' in lmd['model_accuracy'] and 'combined' in lmd['model_accuracy']['train'] and lmd['model_accuracy']['train']['combined'] is not None:
                    train_acc = lmd['model_accuracy']['train']['combined']
                    test_acc = lmd['model_accuracy']['test']['combined']

                    for i in range(0,len(train_acc)):
                        mao['train_accuracy_over_time']['x'].append(i)
                        mao['train_accuracy_over_time']['y'].append(train_acc[i])

                    for i in range(0,len(test_acc)):
                        mao['test_accuracy_over_time']['x'].append(i)
                        mao['test_accuracy_over_time']['y'].append([i])

                if 'model_accuracy' in lmd and lmd['model_accuracy'] is not None and lmd['column_importances'] is not None:
                    mao['accuracy_histogram']['x'] = [f'{x}' for x in lmd['accuracy_histogram'][col]['buckets']]
                    mao['accuracy_histogram']['y'] = lmd['accuracy_histogram'][col]['accuracies']

                    if lmd['columns_buckets_importances'] is not None and col in lmd['columns_buckets_importances']:
                        for output_col_bucket in lmd['columns_buckets_importances'][col]:
                            x_explained_member = []
                            for input_col in lmd['columns_buckets_importances'][col][output_col_bucket]:
                                stats = lmd['columns_buckets_importances'][col][output_col_bucket][input_col]
                                adapted_sub_incol = self._adapt_column(stats, input_col)
                                x_explained_member.append(adapted_sub_incol)
                            mao['accuracy_histogram']['x_explained'].append(x_explained_member)

                    for icol in lmd['model_columns_map'].keys():
                        if icol in lmd['columns_to_ignore']:
                            continue
                        if icol not in lmd['predict_columns']:
                            try:
                                mao['overall_input_importance']['x'].append(icol)
                                mao['overall_input_importance']['y'].append(round(lmd['column_importances'][icol],1))
                            except:
                                print(f'No column importances found for {icol} !')

                amd['model_analysis'].append(mao)
            else:
                if 'column_importances' in lmd and lmd['column_importances'] is not None:
                    icm['importance_score'] = lmd['column_importances'][col]
                amd['data_analysis']['input_columns_metadata'].append(icm)

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

    def export_model(self, model_name=None):
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
                for file_name in [model_name + '_heavy_model_metadata.pickle', model_name + '_light_model_metadata.pickle', model_name + '_lightwood_data']:
                    full_path = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, file_name)
                    zip_fp.write(full_path, os.path.basename(full_path))

                # If the backend is ludwig, save the ludwig files
                try:
                    ludwig_model_path = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, model_name + '_ludwig_data')
                    for root, dirs, files in os.walk(ludwig_model_path):
                        for file in files:
                            full_path = os.path.join(root, file)
                            zip_fp.write(full_path, full_path[len(CONFIG.MINDSDB_STORAGE_PATH):])
                except:
                    pass

            print(f'Exported model to {storage_file}')
            return True
        except Exception as e:
            print(e)
            return False

    def load(self, model_archive_path):
        """
        If you want to import a mindsdb instance storage from a file

        :param mindsdb_storage_dir: full_path that contains your mindsdb predictor zip file
        :return: bool (True/False) True if mind was importerd successfully
        """
        previous_models = os.listdir(CONFIG.MINDSDB_STORAGE_PATH)
        shutil.unpack_archive(model_archive_path, extract_dir=CONFIG.MINDSDB_STORAGE_PATH)

        new_model_files = set(os.listdir(CONFIG.MINDSDB_STORAGE_PATH)) - set(previous_models)
        model_names = []
        for file in new_model_files:
            if '_light_model_metadata.pickle' in file:
                model_name = file.replace('_light_model_metadata.pickle', '')
                model_names.append(model_name)


        for model_name in model_names:
            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, model_name + '_light_model_metadata.pickle'), 'rb') as fp:
                lmd = pickle.load(fp)

            if 'ludwig_data' in lmd and 'ludwig_save_path' in lmd['ludwig_data']:
                lmd['ludwig_data']['ludwig_save_path'] = str(os.path.join(CONFIG.MINDSDB_STORAGE_PATH,os.path.basename(lmd['ludwig_data']['ludwig_save_path'])))

            if 'lightwood_data' in lmd and 'save_path' in lmd['lightwood_data']:
                lmd['lightwood_data']['save_path'] = str(os.path.join(CONFIG.MINDSDB_STORAGE_PATH,os.path.basename(lmd['lightwood_data']['save_path'])))

            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, model_name + '_light_model_metadata.pickle'), 'wb') as fp:
                pickle.dump(lmd, fp,protocol=pickle.HIGHEST_PROTOCOL)


    def load_model(self, model_archive_path=None):
        """
        If you want to load a model to a file

        :param model_archive_path: this is the path to the archive where your model resides
        :return: bool (True/False) True if mind was importerd successfully
        """
        self.load(model_archive_path)

    def rename_model(self, old_model_name, new_model_name):
        """
        If you want to export a model to a file

        :param old_model_name: this is the name of the model you wish to rename
        :param new_model_name: this is the new name of the model
        :return: bool (True/False) True if mind was exported successfully
        """

        if old_model_name == new_model_name:
            return True

        moved_a_backend = False
        for extension in ['_lightwood_data', '_ludwig_data']:
            try:
                shutil.move(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, old_model_name + extension), os.path.join(CONFIG.MINDSDB_STORAGE_PATH, new_model_name + extension))
                moved_a_backend = True
            except:
                pass

        if not moved_a_backend:
            return False

        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, old_model_name + '_light_model_metadata.pickle'), 'rb') as fp:
            lmd = pickle.load(fp)

        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, old_model_name + '_heavy_model_metadata.pickle'), 'rb') as fp:
            hmd = pickle.load(fp)

        lmd['name'] = new_model_name
        hmd['name'] = new_model_name

        renamed_one_backend = False
        try:
            lmd['ludwig_data']['ludwig_save_path'] = lmd['ludwig_data']['ludwig_save_path'].replace(old_model_name, new_model_name)
            renamed_one_backend = True
        except:
            pass

        try:
            lmd['lightwood_data']['save_path'] = lmd['lightwood_data']['save_path'].replace(old_model_name, new_model_name)
            renamed_one_backend = True
        except:
            pass

        if not renamed_one_backend:
            return False

        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, new_model_name + '_light_model_metadata.pickle'), 'wb') as fp:
            pickle.dump(lmd, fp,protocol=pickle.HIGHEST_PROTOCOL)

        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, new_model_name + '_heavy_model_metadata.pickle'), 'wb') as fp:
            pickle.dump(hmd, fp,protocol=pickle.HIGHEST_PROTOCOL)


        os.remove(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, old_model_name + '_light_model_metadata.pickle'))
        os.remove(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, old_model_name + '_heavy_model_metadata.pickle'))
        return True


    def delete_model(self, model_name=None):
        """
        If you want to export a model to a file

        :param model_name: this is the name of the model you wish to export (defaults to the name of the current Predictor)
        :return: bool (True/False) True if mind was exported successfully
        """

        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, model_name + '_light_model_metadata.pickle'), 'rb') as fp:
            lmd = pickle.load(fp)

            try:
                os.remove(lmd['lightwood_data']['save_path'])
            except:
                pass

            try:
                shutil.rmtree(lmd['ludwig_data']['ludwig_save_path'])
            except:
                pass

        if model_name is None:
            model_name = self.name
        try:
            for file_name in [model_name + '_heavy_model_metadata.pickle', model_name + '_light_model_metadata.pickle']:
                os.remove(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, file_name))
            return True
        except Exception as e:
            print(e)
            return False

    def analyse_dataset(self, from_data, sample_margin_of_error=0.005):
        """
        Analyse the particular dataset being given
        """

        from_ds = getDS(from_data)
        transaction_type = TRANSACTION_ANALYSE
        sample_confidence_level = 1 - sample_margin_of_error

        heavy_transaction_metadata = {}
        heavy_transaction_metadata['name'] = self.name
        heavy_transaction_metadata['from_data'] = from_ds

        light_transaction_metadata = {}
        light_transaction_metadata['version'] = str(__version__)
        light_transaction_metadata['name'] = self.name
        light_transaction_metadata['model_columns_map'] = from_ds._col_map
        light_transaction_metadata['type'] = transaction_type
        light_transaction_metadata['sample_margin_of_error'] = sample_margin_of_error
        light_transaction_metadata['sample_confidence_level'] = sample_confidence_level
        light_transaction_metadata['model_is_time_series'] = False
        light_transaction_metadata['model_group_by'] = []
        light_transaction_metadata['model_order_by'] = []
        light_transaction_metadata['columns_to_ignore'] = []
        light_transaction_metadata['data_preparation'] = {}
        light_transaction_metadata['predict_columns'] = []
        light_transaction_metadata['empty_columns'] = []

        light_transaction_metadata['handle_foreign_keys'] = True
        light_transaction_metadata['force_categorical_encoding'] = []
        light_transaction_metadata['handle_text_as_categorical'] = False

        Transaction(session=self, light_transaction_metadata=light_transaction_metadata, heavy_transaction_metadata=heavy_transaction_metadata, logger=self.log)
        return self.get_model_data(model_name=None, lmd=light_transaction_metadata)


    def learn(self, to_predict, from_data, test_from_data=None, group_by=None, window_size=None, order_by=None, sample_margin_of_error=0.005, ignore_columns=None, stop_training_in_x_seconds=None, stop_training_in_accuracy=None, backend='lightwood', rebuild_model=True, use_gpu=None, disable_optional_analysis=False, equal_accuracy_for_all_output_categories=True, output_categories_importance_dictionary=None, unstable_parameters_dict=None):
        """
        Learn to predict a column or columns from the data in 'from_data'

        Mandatory arguments:
        :param to_predict: what column or columns you want to predict
        :param from_data: the data that you want to learn from, this can be either a file, a pandas data frame, or url or a mindsdb data source

        Optional arguments:
        :param test_from_data: If you would like to test this learning from a different data set

        Optional Time series arguments:
        :param order_by: this order by defines the time series, it can be a list. By default it sorts each sort by column in ascending manner, if you want to change this pass a touple ('column_name', 'boolean_for_ascending <default=true>')
        :param group_by: This argument tells the time series that it should learn by grouping rows by a given id
        :param window_size: The number of samples to learn from in the time series

        Optional data transformation arguments:
        :param ignore_columns: mindsdb will ignore this column

        Optional sampling parameters:
        :param sample_margin_of_error (DEFAULT 0): Maximum expected difference between the true population parameter, such as the mean, and the sample estimate.

        Optional debug arguments:
        :param stop_training_in_x_seconds: (default None), if set, you want training to finish in a given number of seconds

        :return:
        """

        if ignore_columns is None:
            ignore_columns = []

        if group_by is None:
            group_by = []

        if order_by is None:
            order_by = []

        # lets turn into lists: predict, ignore, group_by, order_by
        predict_columns = to_predict if isinstance(to_predict, list) else [to_predict]
        ignore_columns = ignore_columns if isinstance(ignore_columns, list) else [ignore_columns]
        group_by = group_by if isinstance(group_by, list) else [group_by]
        order_by = order_by if isinstance(order_by, list) else [order_by]

        # lets turn order by into list of tuples if not already
        # each element ('column_name', 'boolean_for_ascending <default=true>')
        order_by = [col_name if isinstance(col_name, tuple) else (col_name, True) for col_name in order_by]

        if unstable_parameters_dict is None:
            unstable_parameters_dict = {}

        from_ds = getDS(from_data)

        test_from_ds = None if test_from_data is None else getDS(test_from_data)

        transaction_type = TRANSACTION_LEARN
        sample_confidence_level = 1 - sample_margin_of_error

        if len(predict_columns) == 0:
            error = 'You need to specify a column to predict'
            self.log.error(error)
            raise ValueError(error)

        is_time_series = True if len(order_by) > 0 else False

        '''
        We don't implement "name" as a concept in mindsdbd data sources, this is only available for files,
        the server doesn't handle non-file data sources at the moment, so this shouldn't prove an issue,
        once we want to support datasources such as s3 and databases for the server we need to add name as a concept (or, preferably, before that)
        '''
        data_source_name = from_data if isinstance(from_data, str) else 'Unkown'

        heavy_transaction_metadata = {}
        heavy_transaction_metadata['name'] = self.name
        heavy_transaction_metadata['from_data'] = from_ds
        heavy_transaction_metadata['test_from_data'] = test_from_ds
        heavy_transaction_metadata['bucketing_algorithms'] = {}
        heavy_transaction_metadata['predictions'] = None
        heavy_transaction_metadata['model_backend'] = backend

        light_transaction_metadata = {}
        light_transaction_metadata['version'] = str(__version__)
        light_transaction_metadata['name'] = self.name
        light_transaction_metadata['data_preparation'] = {}
        light_transaction_metadata['predict_columns'] = predict_columns
        light_transaction_metadata['model_columns_map'] = from_ds._col_map
        light_transaction_metadata['model_group_by'] = group_by
        light_transaction_metadata['model_order_by'] = order_by
        light_transaction_metadata['model_is_time_series'] = is_time_series
        light_transaction_metadata['data_source'] = data_source_name
        light_transaction_metadata['type'] = transaction_type
        light_transaction_metadata['window_size'] = window_size
        light_transaction_metadata['sample_margin_of_error'] = sample_margin_of_error
        light_transaction_metadata['sample_confidence_level'] = sample_confidence_level
        light_transaction_metadata['stop_training_in_x_seconds'] = stop_training_in_x_seconds
        light_transaction_metadata['rebuild_model'] = rebuild_model
        light_transaction_metadata['model_accuracy'] = {'train': {}, 'test': {}}
        light_transaction_metadata['column_importances'] = None
        light_transaction_metadata['columns_buckets_importances'] = None
        light_transaction_metadata['columnless_prediction_distribution'] = None
        light_transaction_metadata['all_columns_prediction_distribution'] = None
        light_transaction_metadata['use_gpu'] = use_gpu
        light_transaction_metadata['columns_to_ignore'] = ignore_columns
        light_transaction_metadata['disable_optional_analysis'] = disable_optional_analysis
        light_transaction_metadata['validation_set_accuracy'] = None
        light_transaction_metadata['lightwood_data'] = {}
        light_transaction_metadata['ludwig_data'] = {}
        light_transaction_metadata['weight_map'] = {}
        light_transaction_metadata['confusion_matrices'] = {}
        light_transaction_metadata['empty_columns'] = []

        light_transaction_metadata['equal_accuracy_for_all_output_categories'] = equal_accuracy_for_all_output_categories
        light_transaction_metadata['output_categories_importance_dictionary'] = output_categories_importance_dictionary if output_categories_importance_dictionary is not None else {}

        if 'skip_model_training' in unstable_parameters_dict:
            light_transaction_metadata['skip_model_training'] = unstable_parameters_dict['skip_model_training']
        else:
            light_transaction_metadata['skip_model_training'] = False

        if 'skip_stats_generation' in unstable_parameters_dict:
            light_transaction_metadata['skip_stats_generation'] = unstable_parameters_dict['skip_stats_generation']
        else:
            light_transaction_metadata['skip_stats_generation'] = False

        if 'optimize_model' in unstable_parameters_dict:
            light_transaction_metadata['optimize_model'] = unstable_parameters_dict['optimize_model']
        else:
            light_transaction_metadata['optimize_model'] = False

        if 'force_disable_cache' in unstable_parameters_dict:
            light_transaction_metadata['force_disable_cache'] = unstable_parameters_dict['force_disable_cache']
        else:
            light_transaction_metadata['force_disable_cache'] = False

        if 'force_categorical_encoding' in unstable_parameters_dict:
            light_transaction_metadata['force_categorical_encoding'] = unstable_parameters_dict['force_categorical_encoding']
        else:
            light_transaction_metadata['force_categorical_encoding'] = []

        if 'handle_foreign_keys' in unstable_parameters_dict:
            light_transaction_metadata['handle_foreign_keys'] = unstable_parameters_dict['handle_foreign_keys']
        else:
            light_transaction_metadata['handle_foreign_keys'] = False

        if 'handle_text_as_categorical' in unstable_parameters_dict:
            light_transaction_metadata['handle_text_as_categorical'] = unstable_parameters_dict['handle_text_as_categorical']
        else:
            light_transaction_metadata['handle_text_as_categorical'] = False

        if 'use_selfaware_model' in unstable_parameters_dict:
            light_transaction_metadata['use_selfaware_model'] = unstable_parameters_dict['use_selfaware_model']
        else:
            light_transaction_metadata['use_selfaware_model'] = True


        if rebuild_model is False:
            old_lmd = {}
            for k in light_transaction_metadata: old_lmd[k] = light_transaction_metadata[k]

            old_hmd = {}
            for k in heavy_transaction_metadata: old_hmd[k] = heavy_transaction_metadata[k]

            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, light_transaction_metadata['name'] + '_light_model_metadata.pickle'), 'rb') as fp:
                light_transaction_metadata = pickle.load(fp)

            with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, heavy_transaction_metadata['name'] + '_heavy_model_metadata.pickle'), 'rb') as fp:
                heavy_transaction_metadata= pickle.load(fp)

            for k in ['data_preparation', 'rebuild_model', 'data_source', 'type', 'columns_to_ignore', 'sample_margin_of_error', 'sample_confidence_level', 'stop_training_in_x_seconds']:
                if old_lmd[k] is not None: light_transaction_metadata[k] = old_lmd[k]

            for k in ['from_data', 'test_from_data']:
                if old_hmd[k] is not None: heavy_transaction_metadata[k] = old_hmd[k]
        Transaction(session=self, light_transaction_metadata=light_transaction_metadata, heavy_transaction_metadata=heavy_transaction_metadata, logger=self.log)

    def test(self, when_data, accuracy_score_functions, score_using='predicted_value', predict_args=None):
        """
        :param when_data: use this when you have data in either a file, a pandas data frame, or url to a file that you want to predict from
        :param accuracy_score_functions: a single function or  a dictionary for the form `{f'{target_name}': acc_func}` for when we have multiple targets
        :param score_using: what values from the `explanation` of the target to use in the score function, defaults to the
        :param predict_args: dictionary of arguments to be passed to `predict`, e.g: `predict_args={'use_gpu': True}`

        :return: a dictionary for the form `{f'{target_name}_accuracy': accuracy_func_return}`, e.g. {'rental_price_accuracy':0.99}
        """
        if predict_args is None:
            predict_args = {}

        predictions = self.predict(when_data=when_data, **predict_args)

        with open(os.path.join(CONFIG.MINDSDB_STORAGE_PATH, f'{self.name}_light_model_metadata.pickle'), 'rb') as fp:
            lmd = pickle.load(fp)

        accuracy_dict = {}
        for col in lmd['predict_columns']:
            if isinstance(accuracy_score_functions, dict):
                acc_f = accuracy_score_functions[col]
            else:
                acc_f = accuracy_score_functions

            accuracy_dict[f'{col}_accuracy'] = acc_f([x[f'__observed_{col}'] for x in predictions], [x.explanation[col][score_using] for x in predictions])

        return accuracy_dict


    def predict(self, when=None, when_data=None, update_cached_model = False, use_gpu=None, unstable_parameters_dict=None, backend=None, run_confidence_variation_analysis=False):
        """
        You have a mind trained already and you want to make a prediction

        :param when: use this if you have certain conditions for a single prediction
        :param when_data: use this when you have data in either a file, a pandas data frame, or url to a file that you want to predict from
        :param update_cached_model: (optional, default:False) when you run predict for the first time, it loads the latest model in memory, you can force it to do this on this run by flipping it to True
        :param run_confidence_variation_analysis: Run a confidence variation analysis on each of the given input column, currently only works when making single predictions via `when`

        :return: TransactionOutputData object
        """

        if unstable_parameters_dict is None:
            unstable_parameters_dict = {}

        if run_confidence_variation_analysis is True and when_data is not None:
            error_msg = 'run_confidence_variation_analysis=True is a valid option only when predicting a single data point via `when`'
            self.log.error(error_msg)
            raise ValueError(error_msg)

        transaction_type = TRANSACTION_PREDICT
        when_ds = None if when_data is None else getDS(when_data)

        # lets turn into lists: when
        when = [when] if isinstance(when, dict) else when if when is not None else []

        heavy_transaction_metadata = {}
        if when_ds is None:
            heavy_transaction_metadata['when_data'] = None
        else:
            heavy_transaction_metadata['when_data'] = when_ds
        heavy_transaction_metadata['model_when_conditions'] = when
        heavy_transaction_metadata['name'] = self.name

        if backend is not None:
            heavy_transaction_metadata['model_backend'] = backend

        light_transaction_metadata = {}
        light_transaction_metadata['name'] = self.name
        light_transaction_metadata['type'] = transaction_type
        light_transaction_metadata['use_gpu'] = use_gpu
        light_transaction_metadata['data_preparation'] = {}
        light_transaction_metadata['run_confidence_variation_analysis'] = run_confidence_variation_analysis

        if 'force_disable_cache' in unstable_parameters_dict:
            light_transaction_metadata['force_disable_cache'] = unstable_parameters_dict['force_disable_cache']
        else:
            light_transaction_metadata['force_disable_cache'] = False

        transaction = Transaction(session=self, light_transaction_metadata=light_transaction_metadata, heavy_transaction_metadata=heavy_transaction_metadata)

        return transaction.output_data
