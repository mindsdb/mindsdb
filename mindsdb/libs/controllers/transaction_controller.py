"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.helpers.general_helpers import *
from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.data_entities.persistent_model_metadata import PersistentModelMetadata
from mindsdb.libs.data_entities.persistent_ml_model_info import PersistentMlModelInfo
from mindsdb.libs.data_types.transaction_data import TransactionData
from mindsdb.libs.data_types.transaction_output_data import TransactionOutputData
from mindsdb.libs.data_types.model_data import ModelData

import mindsdb.config as CONFIG
# import logging

import _thread
import traceback
import importlib


class TransactionController:

    def __init__(self, session, transaction_metadata, breakpoint = PHASE_END):
        """
        A transaction is the interface to start some MindsDB operation within a session

        :param session:
        :type session: utils.controllers.session_controller.SessionController
        :param transaction_type:
        :param transaction_metadata:
        :type transaction_metadata: TransactionMetadata
        :param breakpoint:
        """

        self.session = session
        self.breakpoint = breakpoint
        self.session.current_transaction = self
        self.metadata = transaction_metadata #type: TransactionMetadata


        # variables to de defined by setup
        self.error = None
        self.errorMsg = None

        self.input_data = TransactionData()
        self.output_data = TransactionOutputData()

        self.model_data = ModelData()

        # variables that can be persisted
        self.persistent_model_metadata = PersistentModelMetadata()
        self.persistent_model_metadata.model_name = self.metadata.model_name
        self.persistent_ml_model_info = PersistentMlModelInfo()
        self.persistent_ml_model_info.model_name = self.metadata.model_name


        self.run()



    def getPhaseInstance(self, module_name, **kwargs):
        """
        Loads the module that we want to start for

        :param module_name:
        :param kwargs:
        :return:
        """

        module_path = convert_cammelcase_to_snake_string(module_name)
        module_full_path = 'mindsdb.libs.phases.{module_path}.{module_path}'.format(module_path=module_path)
        try:
            main_module = importlib.import_module(module_full_path)
            module = getattr(main_module, module_name)
            return module(self.session, self, **kwargs)
        except:
            self.session.logging.error('Could not load module {module_name}'.format(module_name=module_name))
            self.session.logging.error(traceback.format_exc())
            return None


    def callPhaseModule(self, module_name):
        """

        :param module_name:
        :return:
        """
        module = self.getPhaseInstance(module_name)
        return module()


    def executeLearn(self):
        """

        :return:
        """

        self.callPhaseModule('DataExtractor')
        if len(self.input_data.data_array) <= 0 or len(self.input_data.data_array[0]) <=0:
            self.type = TRANSACTION_BAD_QUERY
            self.errorMsg = "No results for this query."
            return

        try:
            # make sure that we remove all previous data about this model
            self.persistent_model_metadata.delete()
            self.persistent_ml_model_info.delete()
            # TODO: Clean the storage elements of this model too

            # start populating data
            self.persistent_model_metadata.train_metadata = self.metadata.getAsDict()
            self.persistent_model_metadata.current_phase = MODEL_STATUS_ANALYZING
            self.persistent_model_metadata.columns = self.input_data.columns # this is populated by data extractor
            self.persistent_model_metadata.predict_columns = self.metadata.model_predict_columns
            self.persistent_model_metadata.insert()


            self.callPhaseModule('StatsGenerator')
            self.persistent_model_metadata.current_phase = MODEL_STATUS_PREPARING
            self.persistent_model_metadata.update()


            self.callPhaseModule('DataVectorizer')
            self.persistent_model_metadata.current_phase = MODEL_STATUS_TRAINING
            self.persistent_model_metadata.update()

            # self.callPhaseModule('DataEncoder')
            self.callPhaseModule('ModelTrainer')
            # TODO: Loop over all stats and when all stats are done, then we can mark model as MODEL_STATUS_TRAINED

            return
        except Exception as e:

            self.persistent_model_metadata.current_phase = MODEL_STATUS_ERROR
            self.persistent_model_metadata.error_msg = traceback.print_exc()
            self.persistent_model_metadata.update()
            self.session.logging.error(self.persistent_model_metadata.error_msg)
            self.session.logging.error(e)

            return


    def executeDropModel(self):
        """

        :return:
        """

        # make sure that we remove all previous data about this model
        self.persistent_model_metadata.delete()
        self.persistent_model_stats.delete()

        self.output_data.data_array = [['Model '+self.metadata.model_name+' deleted.']]
        self.output_data.columns = ['Status']

        return


    def executeNormalSelect(self):
        """

        :return:
        """

        self.callPhaseModule('DataExtractor')
        self.output_data = self.input_data
        return


    def executePredict(self):
        """

        :return:
        """

        self.callPhaseModule('StatsLoader')
        self.callPhaseModule('DataExtractor')
        if len(self.input_data.data_array[0])<=0:
            self.output_data = self.input_data
            return

        self.callPhaseModule('DataVectorizer')
        self.callPhaseModule('ModelPredictor')

        return


    def run(self):
        """

        :return:
        """

        if self.metadata.type == TRANSACTION_BAD_QUERY:
            self.session.logging.error(self.errorMsg)
            self.error = True
            return

        if self.metadata.type == TRANSACTION_DROP_MODEL:
            self.executeDropModel()
            return


        if self.metadata.type == TRANSACTION_LEARN:
            self.output_data.data_array = [['Model ' + self.metadata.model_name + ' training.']]
            self.output_data.columns = ['Status']

            if CONFIG.EXEC_LEARN_IN_THREAD == False:
                self.executeLearn()
            else:
                _thread.start_new_thread(self.executeLearn, ())
            return

        elif self.metadata.type == TRANSACTION_PREDICT:
            self.executePredict()
        elif self.metadata.type == TRANSACTION_NORMAL_SELECT:
            self.executeNormalSelect()


