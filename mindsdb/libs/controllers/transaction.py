from mindsdb.libs.helpers.general_helpers import unpickle_obj
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import *
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.data_types.transaction_output_data import PredictTransactionOutputData, TrainTransactionOutputData
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.helpers.probabilistic_validator import ProbabilisticValidator
from mindsdb.config import CONFIG

import time
import _thread
import traceback
import importlib
import copy
import pickle
import datetime
import sys
from copy import deepcopy
import pandas as pd
import numpy as np

class Transaction:

    def __init__(self, session, light_transaction_metadata, heavy_transaction_metadata, logger =  log):
        """
        A transaction is the interface to start some MindsDB operation within a session

        :param session:
        :type session: utils.controllers.session_controller.SessionController
        :param transaction_type:
        :param transaction_metadata:
        :type transaction_metadata: dict
        :type heavy_transaction_metadata: dict
        """

        self.session = session
        self.lmd = light_transaction_metadata
        self.lmd['created_at'] = str(datetime.datetime.now())
        self.hmd = heavy_transaction_metadata

        # variables to de defined by setup
        self.error = None
        self.errorMsg = None

        self.input_data = TransactionData()
        self.output_data = TrainTransactionOutputData()

        # variables that can be persisted


        self.log = logger

        self.run()


    def load_metadata(self):
        try:
            import resource
            resource.setrlimit(resource.RLIMIT_STACK, [0x10000000, resource.RLIM_INFINITY])
            sys.setrecursionlimit(0x100000)
        except:
            pass


        fn = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.lmd['name'] + '_light_model_metadata.pickle')
        try:
            with open(fn, 'rb') as fp:
                self.lmd = pickle.load(fp)
        except Exception as e:
            self.log.error(e)
            self.log.error(f'Could not load mindsdb light metadata from the file: {fn}')

        fn = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.hmd['name'] + '_heavy_model_metadata.pickle')
        try:
            with open(fn, 'rb') as fp:
                self.hmd = pickle.load(fp)
        except Exception as e:
            self.log.error(e)
            self.log.error(f'Could not load mindsdb heavy metadata in the file: {fn}')


    def save_metadata(self):
        fn = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.lmd['name'] + '_light_model_metadata.pickle')
        self.lmd['updated_at'] = str(datetime.datetime.now())
        try:
            with open(fn, 'wb') as fp:
                pickle.dump(self.lmd, fp,protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            self.log.error(e)
            self.log.error(f'Could not save mindsdb light metadata in the file: {fn}')

        fn = os.path.join(CONFIG.MINDSDB_STORAGE_PATH, self.hmd['name'] + '_heavy_model_metadata.pickle')
        save_hmd = {}
        null_out_fields = ['test_from_data', 'from_data']
        for k in null_out_fields:
            save_hmd[k] = None


        for k in self.hmd:
            if k not in null_out_fields:
                save_hmd[k] = self.hmd[k]
            if k == 'model_backend' and not isinstance(self.hmd['model_backend'], str):
                save_hmd[k] = None

        try:
            with open(fn, 'wb') as fp:
                # Don't save data for now
                pickle.dump(save_hmd, fp,protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as e:
            self.log.error(e)
            self.log.error(f'Could not save mindsdb heavy metadata in the file: {fn}')

    def _call_phase_module(self, module_name, **kwargs):
        """
        Loads the module and runs it

        :param module_name:
        :return:
        """

        self.lmd['is_active'] = True
        module_path = convert_cammelcase_to_snake_string(module_name)
        module_full_path = 'mindsdb.libs.phases.{module_path}.{module_path}'.format(module_path=module_path)
        try:
            main_module = importlib.import_module(module_full_path)
            module = getattr(main_module, module_name)
            return module(self.session, self)(**kwargs)
        except:
            error = 'Could not load module {module_name}'.format(module_name=module_name)
            self.log.error('Could not load module {module_name}'.format(module_name=module_name))
            self.log.error(traceback.format_exc())
            raise Exception(error)
        finally:
            self.lmd['is_active'] = False

    def _execute_analyze(self):
        self.lmd['current_phase'] = MODEL_STATUS_PREPARING
        self.save_metadata()

        self._call_phase_module(module_name='DataExtractor')
        self.save_metadata()

        self._call_phase_module(module_name='DataCleaner', stage=0)
        self.save_metadata()

        self.lmd['current_phase'] = MODEL_STATUS_DATA_ANALYSIS
        self._call_phase_module(module_name='StatsGenerator', input_data=self.input_data, hmd=self.hmd)
        self.save_metadata()

        self.lmd['current_phase'] = MODEL_STATUS_DONE
        self.save_metadata()
        return

    def _execute_learn(self):
        """
        :return:
        """
        try:
            self.lmd['current_phase'] = MODEL_STATUS_PREPARING
            self.save_metadata()

            self._call_phase_module(module_name='DataExtractor')
            self.save_metadata()

            self._call_phase_module(module_name='DataCleaner', stage=0)
            self.save_metadata()

            self.lmd['current_phase'] = MODEL_STATUS_DATA_ANALYSIS
            self._call_phase_module(module_name='StatsGenerator', input_data=self.input_data, hmd=self.hmd)
            self.save_metadata()

            self._call_phase_module(module_name='DataCleaner', stage=0)
            self.save_metadata()
            
            self._call_phase_module(module_name='DataSplitter')

            self._call_phase_module(module_name='DataTransformer', input_data=self.input_data)
            self.lmd['current_phase'] = MODEL_STATUS_TRAINING
            self.save_metadata()
            self._call_phase_module(module_name='ModelInterface', mode='train')

            self.lmd['current_phase'] = MODEL_STATUS_ANALYZING
            self.save_metadata()
            self._call_phase_module(module_name='ModelAnalyzer')

            self.lmd['current_phase'] = MODEL_STATUS_TRAINED
            self.save_metadata()
            return

        except Exception as e:
            self.lmd['is_active'] = False
            self.lmd['current_phase'] = MODEL_STATUS_ERROR
            self.lmd['error_msg'] = traceback.print_exc()
            self.log.error(str(e))
            raise e


    def _execute_predict(self):
        """
        :return:
        """
        old_lmd = {}
        for k in self.lmd: old_lmd[k] = self.lmd[k]

        old_hmd = {}
        for k in self.hmd: old_hmd[k] = self.hmd[k]
        self.load_metadata()

        for k in old_lmd:
            if old_lmd[k] is not None:
                self.lmd[k] = old_lmd[k]
            else:
                if k not in self.lmd:
                    self.lmd[k] = None

        for k in old_hmd:
            if old_hmd[k] is not None:
                self.hmd[k] = old_hmd[k]
            else:
                if k not in self.hmd:
                    self.hmd[k] = None

        if self.lmd is None:
            self.log.error('No metadata found for this model')
            return

        self._call_phase_module(module_name='DataExtractor')

        if self.input_data.data_frame.shape[0] <= 0:
            self.log.error('No input data provided !')
            return
        if self.lmd['model_is_time_series']:
            self._call_phase_module(module_name='DataSplitter')

        # @TODO Maybe move to a separate "PredictionAnalysis" phase ?
        if self.lmd['run_confidence_variation_analysis']:
            nulled_out_data = []
            nulled_out_columns = []
            for column in self.input_data.columns:
                # Only adapted for a single `when`
                if self.input_data.data_frame.iloc[0][column] is not None:
                    nulled_out_data.append(self.input_data.data_frame.iloc[0].copy())
                    nulled_out_data[-1][column] = None
                    nulled_out_columns.append(column)

            nulled_out_data = pd.DataFrame(nulled_out_data)

        for mode in ['predict', 'analyze_confidence']:
            if mode == 'analyze_confidence':
                if not self.lmd['run_confidence_variation_analysis']:
                    continue
                else:
                    self.input_data.data_frame = nulled_out_data

            self._call_phase_module(module_name='DataTransformer', input_data=self.input_data)

            self._call_phase_module(module_name='ModelInterface', mode='predict')

            output_data = {col: [] for col in self.lmd['columns']}

            for column in self.input_data.columns:
                if column in self.lmd['predict_columns']:
                    output_data[f'__observed_{column}'] = list(self.input_data.data_frame[column])
                else:
                    output_data[column] = list(self.input_data.data_frame[column])

            for predicted_col in self.lmd['predict_columns']:
                output_data[predicted_col] = list(self.hmd['predictions'][predicted_col])
                for extra_column in [f'{predicted_col}_model_confidence', f'{predicted_col}_confidence_range']:
                    if extra_column in self.hmd['predictions']:
                        output_data[extra_column] = self.hmd['predictions'][extra_column]

                probabilistic_validator = unpickle_obj(self.hmd['probabilistic_validators'][predicted_col])
                output_data[f'{predicted_col}_confidence'] = [None] * len(output_data[predicted_col])

                output_data[f'model_{predicted_col}'] = deepcopy(output_data[predicted_col])
                for row_number, predicted_value in enumerate(output_data[predicted_col]):

                    # Compute the feature existance vector
                    input_columns = [col for col in self.input_data.columns if col not in self.lmd['predict_columns']]
                    features_existance_vector = [False if  str(output_data[col][row_number]) in ('None', 'nan', '', 'Nan', 'NAN', 'NaN') else True for col in input_columns if col not in self.lmd['columns_to_ignore']]

                    # Create the probabilsitic evaluation
                    probability_true_prediction = probabilistic_validator.evaluate_prediction_accuracy(features_existence=features_existance_vector, predicted_value=predicted_value)

                    output_data[f'{predicted_col}_confidence'][row_number] = probability_true_prediction

            if mode == 'predict':
                self.output_data = PredictTransactionOutputData(transaction=self, data=output_data)
            else:
                nulled_out_predictions = PredictTransactionOutputData(transaction=self, data=output_data)

        if self.lmd['run_confidence_variation_analysis']:
            input_confidence = {}
            extra_insights = {}

            for predicted_col in self.lmd['predict_columns']:
                input_confidence[predicted_col] = {}
                extra_insights[predicted_col] = {'if_missing':[]}

                actual_confidence = self.output_data[0].explanation[predicted_col]['confidence']

                for i, nulled_col_name in enumerate(nulled_out_columns):
                    nulled_out_predicted_value = nulled_out_predictions[i].explanation[predicted_col]['predicted_value']
                    nulled_confidence = nulled_out_predictions[i].explanation[predicted_col]['confidence']
                    print(actual_confidence - nulled_confidence, actual_confidence, nulled_confidence)
                    confidence_variation = actual_confidence - nulled_confidence

                    input_confidence[predicted_col][nulled_col_name] = round(confidence_variation,3)
                    extra_insights[predicted_col]['if_missing'].append({nulled_col_name: nulled_out_predicted_value})

            self.output_data.input_confidence = input_confidence
            self.output_data.extra_insights = extra_insights
        return


    def run(self):
        """

        :return:
        """

        if self.lmd['type'] == TRANSACTION_BAD_QUERY:
            self.log.error(self.errorMsg)
            self.error = True
            return

        if self.lmd['type'] == TRANSACTION_LEARN:
            if CONFIG.EXEC_LEARN_IN_THREAD == False:
                self._execute_learn()
            else:
                _thread.start_new_thread(self._execute_learn, ())
            return

        if self.lmd['type'] == TRANSACTION_ANALYSE:
            self._execute_analyze()

        elif self.lmd['type'] == TRANSACTION_PREDICT:
            self._execute_predict()
        elif self.lmd['type'] == TRANSACTION_NORMAL_SELECT:
            self._execute_normal_select()
