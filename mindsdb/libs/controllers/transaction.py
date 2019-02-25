from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import *
from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.data_entities.persistent_model_metadata import PersistentModelMetadata
from mindsdb.libs.data_entities.persistent_ml_model_info import PersistentMlModelInfo
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.data_types.transaction_output_data import TransactionOutputData
from mindsdb.libs.data_types.model_data import ModelData
from mindsdb.libs.data_types.mindsdb_logger import log
from mindsdb.libs.backends.ludwig import LudwigBackend
from mindsdb.config import CONFIG

import pandas as pd

import _thread
import traceback
import importlib


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
        self.output_data = TransactionOutputData(predicted_columns=self.metadata.model_predict_columns)

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
            return module(self.session, self)
        except:
            error = 'Could not load module {module_name}'.format(module_name=module_name)
            self.log.error('Could not load module {module_name}'.format(module_name=module_name))
            self.log.error(traceback.format_exc())
            raise ValueError(error)
            return None

        module = self._get_phase_instance(module_name)
        return module()


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
            self.persistent_model_metadata.train_metadata = self.metadata.getAsDict()
            self.persistent_model_metadata.current_phase = MODEL_STATUS_ANALYZING
            self.persistent_model_metadata.columns = self.input_data.columns # this is populated by data extractor
            self.persistent_model_metadata.predict_columns = self.metadata.model_predict_columns
            self.persistent_model_metadata.insert()


            self._call_phase_module('StatsGenerator')
            self.persistent_model_metadata.current_phase = MODEL_STATUS_PREPARING
            self.persistent_model_metadata.update()

            model_backend = LudwigBackend(self)
            model_backend.train()

            # self._call_phase_module('DataVectorizer')
            # self.persistent_model_metadata.current_phase = MODEL_STATUS_TRAINING
            # self.persistent_model_metadata.update()

            # self.callPhaseModule('DataEncoder')
            # self._call_phase_module('ModelTrainer')

            # self._call_phase_module('ModelAnalyzer')
            # TODO: Loop over all stats and when all stats are done, then we can mark model as MODEL_STATUS_TRAINED

            return
        except Exception as e:

            self.persistent_model_metadata.current_phase = MODEL_STATUS_ERROR
            self.persistent_model_metadata.error_msg = traceback.print_exc()
            self.persistent_model_metadata.update()
            self.log.error(self.persistent_model_metadata.error_msg)
            self.log.error(e)
            raise e


    def _execute_drop_model(self):
        """

        :return:
        """

        # make sure that we remove all previous data about this model
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
        if len(self.input_data.data_array[0])<=0:
            self.output_data = self.input_data
            return

        self._call_phase_module('DataVectorizer')
        self._call_phase_module('ModelPredictor')

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
