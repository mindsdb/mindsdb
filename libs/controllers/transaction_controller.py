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

from libs.constants.mindsdb import *
from libs.helpers.general_helpers import *

import config as CONFIG
import re
# import logging
from libs.helpers.logging import logging

import _thread
import traceback
import importlib

from bson.objectid import ObjectId

class TransactionController:

    def __init__(self, session, type,  transaction_metadata, breakpoint = PHASE_END):
        """
        A transaction is the interface to start some MindsDB operation within a session

        :param session:
        :param type:
        :param transaction_metadata:
        :param breakpoint:
        """

        self.session = session
        self.breakpoint = breakpoint
        self.session.current_transaction = self

        self.transaction_metadata = transaction_metadata
        self.type = type

        # variables to de defined by setup
        self.error = None
        self.errorMsg = None


        self.input_data_sql = None # This is the query that can be used to extract actual data needed for this transaction
        self.input_data_array = None # [[]]
        self.input_test_data_array = None
        self.input_metadata = None

        self.predict_metadata = None # This is the metadata of a predict statement

        self.model_data = {KEYS.TRAIN_SET:{}, KEYS.TEST_SET:{}, KEYS.ALL_SET: {}, KEYS.PREDICT_SET:{}}
        self.model_data_input_array_map = {KEYS.TRAIN_SET:{}, KEYS.TEST_SET:{}, KEYS.ALL_SET: {}, KEYS.PREDICT_SET:{}}
        self.input_vectors_group_by = False


        self.model_stats = None

        # this defines what data model to use
        self.data_model_framework = FRAMEWORKS.PYTORCH
        self.data_model_predictor = 'DefaultPredictor'

        self.output_data_array = None
        self.output_metadata = None
        self.run()



    def getPhaseInstance(self, module_name, **kwargs):
        """
        Loads the module that we want to start for

        :param module_name:
        :param kwargs:
        :return:
        """

        module_path = convert_cammelcase_to_snake_string(module_name)
        module_full_path = 'libs.phases.{module_path}.{module_path}'.format(module_path=module_path)
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
        if not self.input_data_array or len(self.input_data_array[0])<=0:
            self.type = TRANSACTION_BAD_QUERY
            self.errorMsg = "No results for this query."
            return

        model_name = self.transaction_metadata[KEY_MODEL_NAME]

        try:
            model_stats = self.session.mongo.mindsdb.model_stats

            model_stats.insert({'model_name': model_name,
                                'submodel_name': None,
                                "model_metadata":self.transaction_metadata,
                                "status": MODEL_STATUS_ANALYZING,
                                "_id": str(ObjectId())
                                })

            self.callPhaseModule('StatsGenerator')
            model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                                   {'$set': {
                                       "status": MODEL_STATUS_PREPARING
                                   }})
            stats = model_stats.find_one({'model_name': model_name, 'submodel_name': None})['stats']
            self.model_stats = stats

            self.callPhaseModule('DataVectorizer')
            model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                                   {'$set': {
                                       "status": MODEL_STATUS_TRAINING
                                   }})

            # self.callPhaseModule('DataEncoder')
            # model_stats.update_one({'model_name': model_name, 'submodel_name': None},
            #                        {'$set': {
            #                            "status": MODEL_STATUS_TRAINING
            #                        }})

            # self.callPhaseModule('DataDevectorizer')
            self.callPhaseModule('ModelTrainer')
            model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                                   {'$set': {
                                       "status": MODEL_STATUS_TRAINED
                                   }})
            return
        except Exception as e:
            self.session.logging.error(traceback.print_exc())
            self.session.logging.error(e)
            model_stats.update_one({'model_name': model_name, 'submodel_name': None},
                                   {'$set': {
                                       "status": MODEL_STATUS_ERROR
                                   }})
            return


    def executeDropModel(self):
        """

        :return:
        """

        #TODO: remove model directory, remove drill view
        model_name =self.transaction_metadata[KEY_MODEL_NAME]
        model_stats = self.session.mongo.mindsdb.model_stats
        model_train_stats = self.session.mongo.mindsdb.model_train_stats
        model = model_stats.find_one({'model_name': model_name})
        if not model:
            self.type = TRANSACTION_BAD_QUERY
            self.errorMsg = "Model '" + model_name + "' not found."
            return

        model_stats.delete_one({'model_name': model_name})
        model_train_stats.delete_one({'model_name': model_name})
        self.output_data_array = [['Model '+model_name+' deleted.']]
        self.output_metadata = {
            KEY_COLUMNS: ['Status']
        }
        return


    def executeNormalSelect(self):
        """

        :return:
        """

        self.callPhaseModule('DataExtractor')
        self.output_data_array = self.input_data_array
        self.output_metadata = self.transaction_metadata
        return


    def executePredict(self):
        """

        :return:
        """

        self.callPhaseModule('StatsLoader')
        self.callPhaseModule('DataExtractor')
        if not self.input_data_array or len(self.input_data_array[0])<=0:
            self.output_data_array = self.input_data_array
            self.output_metadata = self.transaction_metadata
            return

        self.callPhaseModule('DataVectorizer')
        self.callPhaseModule('ModelPredictor')
        self.output_data_array = self.input_data_array
        self.output_metadata = self.transaction_metadata
        return


    def run(self):
        """

        :return:
        """

        if self.type == TRANSACTION_BAD_QUERY:
            self.session.logging.error(self.errorMsg)
            self.error = True
            return

        if self.type == TRANSACTION_DROP_MODEL:
            self.executeDropModel()
            return


        if self.type == TRANSACTION_LEARN:
            self.output_data_array = [['Model ' + self.model_metadata[KEY_MODEL_NAME] + ' training.']]
            self.output_metadata = {
                KEY_COLUMNS: ['Status']
            }
            if CONFIG.EXEC_LEARN_IN_THREAD == False:
                self.executeLearn()
            else:
                _thread.start_new_thread(self.executeLearn, ())
            return

        elif self.type == TRANSACTION_PREDICT:
            self.executePredict()
        elif self.type == TRANSACTION_NORMAL_SELECT:
            self.executeNormalSelect()


