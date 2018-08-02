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

    def __init__(self, session, sql_query, breakpoint = PHASE_END):

        self.session = session
        self.breakpoint = breakpoint
        self.session.current_transaction = self
        self.sql_query = sql_query # This is the query as it comes from the client

        # variables to de defined by setup
        self.type = None
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

        self.model_metadata = None
        self.model_stats = None

        # this defines what data model to use
        self.data_model_framework = FRAMEWORKS.PYTORCH
        self.data_model_predictor = 'DefaultPredictor'

        self.output_data_array = None
        self.output_metadata = None
        self.setup()

    def setTransactionTypeAndDataQuery(self):
        """
        sets most of the transaction query related data
        :return: None
        """

        if self.sql_query[-1] == ';':
            self.sql_query = self.sql_query[:-1]

        self.sql_query = re.sub(' +', ' ', self.sql_query)
        logging.info(self.sql_query)
        lower_case_query = str(self.sql_query).lower()


        # CREATE MODEL QUERY
        regex = r"CREATE[ ]+MODEL[ ]+FROM[ ]+(\(.*(?=)\))[ ]+AS"
        create_model_query = re.search(regex, self.sql_query, re.IGNORECASE | re.MULTILINE)
        prediction_model = False
        if create_model_query:

            create_model_query = create_model_query.group(1)
            create_model_query = create_model_query[1:]
            create_model_query = create_model_query[:-1]
            self.type = TRANSACTION_LEARN
            self.input_data_sql = create_model_query
            clean_query = self.sql_query.replace(create_model_query,'')
            regex = r" AS (\w+\.*)+"
            model_name = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)

            if not model_name:
                self.type =TRANSACTION_BAD_QUERY
                self.errorMsg = "Missing parameter MODEL NAME, query should have this structure: CREATE MODEL FROM ( [view] ) AS [model_name] PREDICT [column]"
                return

            model_name = model_name.group(1)
            model_stats = self.session.mongo.mindsdb.model_stats

            if CONFIG.TEST_OVERWRITE_MODEL == True:
                model = False
            else:
                model = model_stats.find_one({'model_name': model_name})

            if model:
                if 'status' in model and model['status'] == MODEL_STATUS_TRAINED:
                    # model_stats.delete_one({'model_name': model_name})
                    self.type = TRANSACTION_BAD_QUERY
                    self.errorMsg = "Model '"+model_name+"' already exists."
                    return
                else:
                    self.type = TRANSACTION_BAD_QUERY
                    self.errorMsg = "Model '"+model_name+"' is being trained."
                    return

            regex = r" GROUP BY (\w+)"
            group_by = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)
            if group_by:
                group_by = group_by.group(1)

            regex = r" ORDER BY (\w+)"
            order_by = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)
            if order_by:
                order_by = order_by.group(1)

            predictor = PREDICTORS.DEFAULT
            regex = r" PREDICTOR (\w+)"
            predictor = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)
            if predictor:
                predictor = predictor.group(1)
                if predictor.lower() ==  PREDICTORS.DEFAULT:
                    predictor =PREDICTORS.DEFAULT
                if predictor.lower() ==  PREDICTORS.CUSTOM:
                    predictor = PREDICTORS.CUSTOM

            regex = r" PREDICT (\w+)"
            predict_columns = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)
            # TODO: Support multiple columns, take into account * as all column
            if not predict_columns:
                self.type =TRANSACTION_BAD_QUERY
                self.errorMsg = "Missing parameter PREDICT, query should have this structure: CREATE MODEL FROM ( [view] ) AS [model_name] PREDICT [column]"
                return
            predict_columns = predict_columns.group(1)
            predict_columns = predict_columns.split(',')

            regex = r" UPDATE (\w+)"
            update_frecuency = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)
            if update_frecuency:
                update_frecuency = update_frecuency.group(1)

            regex = r" CACHE (\w+)"
            cache = re.search(regex, clean_query, re.IGNORECASE | re.MULTILINE)
            if cache:
                cache = cache.group(1)

            regex = r" TEST FROM \((.*)\)"
            test_query = re.search(regex, self.sql_query, re.IGNORECASE | re.MULTILINE)
            if test_query:
                test_query = test_query.group(1)

            # TODO: change this for a struct
            self.model_metadata = {
                KEY_MODEL_NAME: model_name,
                KEY_MODEL_QUERY: create_model_query,
                KEY_MODEL_PREDICT_COLUMNS: predict_columns,
                KEY_MODEL_GROUP_BY: group_by,
                KEY_MODEL_ORDER_BY: order_by,
                KEY_MODEL_UPDATE_FREQUENCY: update_frecuency,
                KEY_MODEL_CACHE:cache,
                KEY_MODEL_MODEL_TYPE:predictor,
                KEY_MODEL_TEST_QUERY: test_query
            }

            extra_sql = ' '

            # on train get only the values that have no null on the target
            # TODO: Better do this as predict can be *
            #if ' WHERE ' not in self.input_data_sql.upper():
            #    extra_sql += ' WHERE '
            #statements = []
            # for col_to_predict in self.model_metadata[KEY_MODEL_PREDICT_COLUMNS]:
            #     statements += [" ( {col} is not NULL AND {col} > '' ) ".format(col=col_to_predict)]
            #statement = " AND ".join(statements)
            #extra_sql += statement

            # Add the order by if group by and order in the create model
            # TODO: if its a nested query this should be fixed
            if ' ORDER ' not in self.input_data_sql.upper() and (
                self.model_metadata[KEY_MODEL_ORDER_BY] or self.model_metadata[
                KEY_MODEL_GROUP_BY]):

                extra_sql += ' ORDER BY '

                if self.model_metadata[KEY_MODEL_GROUP_BY]:
                    extra_sql += ' {order_by} '.format(
                        order_by=self.model_metadata[KEY_MODEL_GROUP_BY])
                if self.model_metadata[KEY_MODEL_ORDER_BY]:
                    extra_sql += ', {order_by} ASC'.format(
                        order_by=self.model_metadata[KEY_MODEL_ORDER_BY])

            self.input_data_sql += extra_sql
            if test_query:
                test_query += extra_sql
                self.model_metadata[KEY_MODEL_TEST_QUERY] = test_query

            return


        # DROP MODEL QUERY
        regex = r"DROP[ ]+MODEL[ ]"
        drop_model_query = re.search(regex, self.sql_query, re.IGNORECASE | re.MULTILINE)
        if drop_model_query:
            regex = r"DROP[ ]+MODEL[ ](\w+)"
            model_name = re.search(regex, self.sql_query, re.IGNORECASE | re.MULTILINE)
            if not model_name:
                self.type = TRANSACTION_BAD_QUERY
                self.errorMsg = "Missing parameter MODEL NAME, query should have this structure: DROP MODEL [model_name]"
                return

            model_name = model_name.group(1)
            self.type = TRANSACTION_DROP_MODEL
            self.model_metadata = {
                KEY_MODEL_NAME: model_name
            }
            return

        notReadActions = ['create', 'insert',
                          'delete', 'update', 'alter', 'drop']


        if any(action in lower_case_query for action in notReadActions):
            if " predict " in lower_case_query or " cluster " in lower_case_query:
                # Create, insert, delete, update, alter or drop mixed with AI
                # keywords
                self.type =TRANSACTION_BAD_QUERY
                self.errorMsg = "Bad Query"
            else:
                self.type = TRANSACTION_NORMAL_MODIFY
        else:
            if " predict " in lower_case_query:
                self.type = TRANSACTION_PREDICT
            elif " cluster " in lower_case_query:
                self.type = TRANSACTION_CLUSTER
            else:
                self.type = TRANSACTION_NORMAL_SELECT

        columns_to_predict = False
        when_conditions = False
        return_cols = []
        regular_query = self.sql_query
        if self.type == TRANSACTION_PREDICT:
            regular_query = re.search(r'.+(?= PREDICT )', self.sql_query, re.IGNORECASE)
            regular_query = regular_query.group(0)

            columns_to_predict = re.search(r'(?<= PREDICT\s)(\w+)', self.sql_query, re.IGNORECASE)
            if columns_to_predict:
                columns_to_predict = columns_to_predict.group(0)
                columns_to_predict = columns_to_predict.split(',')
                # TODO: Fix regular expression to support more than one

            when_conditions = re.search(r'(?<= WHEN\s).*', self.sql_query, re.IGNORECASE)
            if when_conditions:
                when_conditions = when_conditions.group(0)
            else:
                when_conditions = False

            if when_conditions:
                when_conditions = when_conditions.replace(';', '')
                when_conditions = when_conditions.replace(' ', '')
                when_conditions = when_conditions.split(',')
                for i, condition in enumerate(when_conditions):
                    when_conditions[i] = condition.split('=')

            prediction_model = re.search(r'(?<= USING\s)(\w+)', self.sql_query, re.IGNORECASE)
            if prediction_model:
                prediction_model = prediction_model.group(0)
            else:
                self.type = TRANSACTION_BAD_QUERY
                self.errorMsg = "Please provide a model name."


        if self.type == TRANSACTION_CLUSTER:
            regular_query = re.search(r'.+(?= CLUSTER )', self.sql_query, re.IGNORECASE)
            regular_query = regular_query.group(0)
            #TODO: CLUSTER INFORMATION

        table_name = re.search(r'(?<= FROM\s)(\w+\.*)+', self.sql_query, re.IGNORECASE)
        if table_name:
            table_name = table_name.group(0)
        else:
            table_name = False

        cols = re.search(
            r'(?<=SELECT )(.*)(?= FROM)', self.sql_query, re.IGNORECASE)
        if cols:
            cols = cols.group(0)
            return_cols = []
            cols = "".join(cols.split())
            cols = cols.split(',')
            for col in cols:
                return_cols.append(col)

        select_all_query = re.sub(r'(?<=SELECT )(.*)(?= FROM)', '*', self.sql_query)

        self.input_data_sql = regular_query
        self.predict_metadata = {
            KEY_COLUMNS: return_cols,
            KEY_MODEL_NAME: prediction_model,
            KEY_MODEL_QUERY: select_all_query,
            KEY_MODEL_PREDICT_COLUMNS:columns_to_predict ,
            KEY_MODEL_PREDICT_FROM_TABLE: table_name,
            KEY_MODEL_WHEN_CONDITIONS: when_conditions,
            KEY_MODEL_GROUP_BY: None,
            KEY_MODEL_ORDER_BY: None,

        }

        return

    def getModuleInstance(self, module_name, **kwargs):
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
        module = self.getModuleInstance(module_name)
        return module()


    def executeLearn(self):
        self.callPhaseModule('DataExtractor')
        if not self.input_data_array or len(self.input_data_array[0])<=0:
            self.type = TRANSACTION_BAD_QUERY
            self.errorMsg = "No results for this query."
            return

        model_name = self.model_metadata[KEY_MODEL_NAME]

        try:
            model_stats = self.session.mongo.mindsdb.model_stats

            model_stats.insert({'model_name': model_name,
                                'submodel_name': None,
                                "model_metadata":self.model_metadata,
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
        #TODO: remove model directory, remove drill view
        model_name =self.model_metadata[KEY_MODEL_NAME]
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
        self.callPhaseModule('DataExtractor')
        self.output_data_array = self.input_data_array
        self.output_metadata = self.input_metadata
        return

    def executePredict(self):
        self.callPhaseModule('StatsLoader')
        self.callPhaseModule('DataExtractor')
        if not self.input_data_array or len(self.input_data_array[0])<=0:
            self.output_data_array = self.input_data_array
            self.output_metadata = self.input_metadata
            return

        self.callPhaseModule('DataVectorizer')
        self.callPhaseModule('ModelPredictor')
        self.output_data_array = self.input_data_array
        self.output_metadata = self.input_metadata
        return

    def setup(self):
        self.setTransactionTypeAndDataQuery()
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


