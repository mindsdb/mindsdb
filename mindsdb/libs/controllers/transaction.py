from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import *
from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.data_entities.persistent_model_metadata import PersistentModelMetadata
from mindsdb.libs.data_entities.persistent_ml_model_info import PersistentMlModelInfo
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.data_types.transaction_output_data import PredictTransactionOutputData, TrainTransactionOutputData
from mindsdb.libs.data_types.model_data import ModelData
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.backends.ludwig import LudwigBackend
from mindsdb.libs.ml_models.probabilistic_validator import ProbabilisticValidator
from mindsdb.config import CONFIG

import time
import _thread
import traceback
import importlib
import copy


class Transaction:

    def __init__(self, session, transaction_metadata, logger =  log, breakpoint = PHASE_END):
        """
        A transaction is the interface to start some MindsDB operation within a session

        :param session:
        :type session: utils.controllers.session_controller.SessionController
        :param transaction_type:
        :param transaction_metadata:
        :type transaction_metadata: TransactionMetadata
        :param breakpoint:
        """


        self.breakpoint = breakpoint
        self.session = session
        self.metadata = transaction_metadata #type: TransactionMetadata

        self.train_metadata = None # type: TransactionMetadata
        # the metadata generated on train (useful also in predict), this will be populated by the data extractor

        # variables to de defined by setup
        self.error = None
        self.errorMsg = None

        self.input_data = TransactionData()
        self.output_data = TrainTransactionOutputData()
        self.model_data = ModelData()

        # variables that can be persisted
        self.persistent_model_metadata = PersistentModelMetadata()
        self.persistent_model_metadata.model_name = self.metadata.model_name
        self.persistent_ml_model_info = PersistentMlModelInfo()
        self.persistent_ml_model_info.model_name = self.metadata.model_name


        self.log = logger

        self.run()


    def _call_phase_module(self, module_name):
        """
        Loads the module and runs it

        :param module_name:
        :return:
        """

        module_path = convert_cammelcase_to_snake_string(module_name)
        module_full_path = 'mindsdb.libs.phases.{module_path}.{module_path}'.format(module_path=module_path)
        try:
            main_module = importlib.import_module(module_full_path)
            module = getattr(main_module, module_name)
            return module(self.session, self)()
        except:
            error = 'Could not load module {module_name}'.format(module_name=module_name)
            self.log.error('Could not load module {module_name}'.format(module_name=module_name))
            self.log.error(traceback.format_exc())
            raise ValueError(error)
            return None


    def _execute_learn(self):
        """

        :return:
        """

        self._call_phase_module('DataExtractor')
        if len(self.input_data.data_array) <= 0 or len(self.input_data.data_array[0]) <=0:
            self.type = TRANSACTION_BAD_QUERY
            self.errorMsg = "No results for this query."
            return

        try:
            # make sure that we remove all previous data about this model
            info = self.persistent_ml_model_info.find_one(self.persistent_model_metadata.getPkey())
            if info is not None:
                info.deleteFiles()
            self.persistent_model_metadata.delete()
            self.persistent_ml_model_info.delete()

            # start populating data
            self.persistent_model_metadata.model_backend = self.metadata.model_backend
            self.persistent_model_metadata.train_metadata = self.metadata.getAsDict()
            self.persistent_model_metadata.current_phase = MODEL_STATUS_ANALYZING
            self.persistent_model_metadata.columns = self.input_data.columns # this is populated by data extractor
            self.persistent_model_metadata.predict_columns = self.metadata.model_predict_columns
            self.persistent_model_metadata.model_order_by = self.metadata.model_order_by
            self.persistent_model_metadata.model_group_by = self.metadata.model_group_by
            self.persistent_model_metadata.window_size_seconds = self.metadata.window_size_seconds
            self.persistent_model_metadata.window_size_samples = self.metadata.window_size_samples

            self._call_phase_module('StatsGenerator')
            self.persistent_model_metadata.current_phase = MODEL_STATUS_TRAINING

            if self.persistent_model_metadata.model_backend == 'ludwig':
                self.model_backend = LudwigBackend(self)
                self.model_backend.train()

            self._call_phase_module('ModelAnalyzer')

            self.persistent_model_metadata.insert()
            self.persistent_model_metadata.update()
            
            return
        except Exception as e:
            self.persistent_model_metadata.current_phase = MODEL_STATUS_ERROR
            self.persistent_model_metadata.error_msg = traceback.print_exc()
            self.log.error(str(e))
            raise e


    def _execute_drop_model(self):
        """
        Make sure that we remove all previous data about this model

        :return:
        """

        self.persistent_model_metadata.delete()
        self.persistent_model_stats.delete()

        self.output_data.data_array = [['Model '+self.metadata.model_name+' deleted.']]
        self.output_data.columns = ['Status']

        return



    def _execute_predict(self):
        """

        :return:
        """

        self._call_phase_module('StatsLoader')
        if self.persistent_model_metadata is None:
            self.log.error('No metadata found for this model')
            return

        self.metadata.model_predict_columns = self.persistent_model_metadata.predict_columns
        self.metadata.model_columns_map = self.persistent_model_metadata.train_metadata['model_columns_map']
        #self.metadata.model_when_conditions = {key if key not in self.metadata.model_columns_map else self.metadata.model_columns_map[key] : self.metadata.model_when_conditions[key] for key in self.metadata.model_when_conditions }

        self._call_phase_module('DataExtractor')

        if len(self.input_data.data_array[0]) <= 0:
            self.output_data = self.input_data
            return

        self.output_data = PredictTransactionOutputData(transaction=self)

        if self.persistent_model_metadata.model_backend == 'ludwig':
            self.model_backend = LudwigBackend(self)
            predictions = self.model_backend.predict()

        # self.transaction.persistent_model_metadata.predict_columns
        self.output_data.data = {col: [] for i, col in enumerate(self.input_data.columns)}
        input_columns = [col for col in self.input_data.columns if col not in self.persistent_model_metadata.predict_columns]

        for row in self.input_data.data_array:
            for index, cell in enumerate(row):
                col = self.input_data.columns[index]
                self.output_data.data[col].append(cell)

        for predicted_col in self.persistent_model_metadata.predict_columns:
            probabilistic_validator = ProbabilisticValidator.unpickle(self.persistent_model_metadata.probabilistic_validators[predicted_col])

            predicted_values = predictions[predicted_col]
            self.output_data.data[predicted_col] = predicted_values
            confidence_column_name = "{col}_confidence".format(col=predicted_col)
            self.output_data.data[confidence_column_name] = [None] * len(predicted_values)
            self.output_data.evaluations[predicted_col] = [None] * len(predicted_values)

            for row_number, predicted_value in enumerate(predicted_values):
                features_existance_vector = [False if self.output_data.data[col][row_number] is None else True for col in input_columns]
                prediction_evaluation = probabilistic_validator.evaluate_prediction_accuracy(features_existence=features_existance_vector, predicted_value=predicted_value)
                self.output_data.data[confidence_column_name][row_number] = prediction_evaluation
                #output_data[col][row_number] = prediction_evaluation.most_likely_value Huh, is this correct, are we replacing the predicted value with the most likely one ? Seems... wrong
                self.output_data.evaluations[predicted_col][row_number] = prediction_evaluation

        return


    def run(self):
        """

        :return:
        """

        if self.metadata.type == TRANSACTION_BAD_QUERY:
            self.log.error(self.errorMsg)
            self.error = True
            return

        if self.metadata.type == TRANSACTION_DROP_MODEL:
            self._execute_drop_model()
            return


        if self.metadata.type == TRANSACTION_LEARN:
            self.output_data.data_array = [['Model ' + self.metadata.model_name + ' training.']]
            self.output_data.columns = ['Status']

            if CONFIG.EXEC_LEARN_IN_THREAD == False:
                self._execute_learn()
            else:
                _thread.start_new_thread(self._execute_learn, ())
            return

        elif self.metadata.type == TRANSACTION_PREDICT:
            self._execute_predict()
        elif self.metadata.type == TRANSACTION_NORMAL_SELECT:
            self._execute_normal_select()
