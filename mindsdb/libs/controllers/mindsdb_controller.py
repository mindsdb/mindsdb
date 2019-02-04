import sqlite3
import pandas
import requests
import os
import platform
import _thread
import uuid
import traceback
import urllib
import pandas as pd

import mindsdb.libs.helpers.log as log
from mindsdb.libs.helpers.multi_data_source import getDS
from mindsdb.config import SQLITE_FILE
import mindsdb.config as CONFIG
from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.controllers.session_controller import SessionController
from mindsdb.libs.constants.mindsdb import *

from mindsdb.version import mindsdb_version as MINDSDB_VERSION

from pathlib import Path

class MindsDBController:

    def __init__(self, log_level=1, log_url='http://localhost:35261', send_logs=True):
        """
        :param file:
        """

        self.setConfigs()
        controller_uuid = str(uuid.uuid1())
        log.initialize(controller_uuid, log_level, log_url, send_logs)

        _thread.start_new_thread(MindsDBController.checkForUpdates, ())
        self.session = SessionController()


    def setConfigs(self):
        """
        This sets the config settings for this mindsdb instance
        :return:
        """

        # set the mindsdb storage folder
        storage_ok = True # default state

        # if it does not exist try to create it
        if not os.path.exists(CONFIG.MINDSDB_STORAGE_PATH):
            try:
                log.info('{folder} does not exist, creating it now'.format(folder=CONFIG.MINDSDB_STORAGE_PATH))
                path = Path(CONFIG.MINDSDB_STORAGE_PATH)
                path.mkdir(exist_ok=True, parents=True)
            except:
                log.info(traceback.format_exc())
                storage_ok = False
                log.error('MindsDB storage foldler: {folder} does not exist and could not be created'.format(folder=CONFIG.MINDSDB_STORAGE_PATH))

        # If storage path is not writable, raise an exception as this can no longer be
        if not os.access(CONFIG.MINDSDB_STORAGE_PATH, os.W_OK) or storage_ok == False:
            error_message = '''Cannot write into storage path, please either set the config variable mindsdb.config.set('MINDSDB_STORAGE_PATH',<path>) or give write access to {folder}'''
            raise ValueError(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))





    def setUserEmail(self, email):
        """

        :param email:
        :return:
        """
        email_file = CONFIG.MINDSDB_STORAGE_PATH + '/../email.mdb_base'


        try:
            open(email_file, 'w').write(email)
            return True
        except:
            log.warning('Cannot store token, Please add write permissions to file:' + email_file)
            return False

    def getUserEmail(self):
        """

        :return:
        """
        email_file = CONFIG.MINDSDB_STORAGE_PATH + '/../email.mdb_base'
        email_file_path  = Path(email_file)

        try:
            if email_file_path.is_file():
                email = open(email_file, 'r').read()
                return email
            else:
                return None
        except:
            log.warning('Cannot read email, Please add write permissions to file:' + email_file)
            return None

    def learn(self, predict, from_data = None, model_name='mdsb_model', test_from_data=None, group_by = None, window_size = MODEL_GROUP_BY_DEAFAULT_LIMIT, order_by = [], sample_margin_of_error = CONFIG.DEFAULT_MARGIN_OF_ERROR, sample_confidence_level = CONFIG.DEFAULT_CONFIDENCE_LEVEL, breakpoint = PHASE_END, ignore_columns = [], rename_strange_columns = False):
        """
        This method is the one that defines what to learn and from what, under what contraints

        Mandatory arguments:
        :param predict: what column you want to predict
        :param from_data: the data that you want to learn from, this can be either a file, a pandas data frame, or url to a file

        Optional arguments:
        :param model_name: the name you want to give to this model
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
        :param sample_confidence_level (DEFAULT 0.98): number in the interval (0, 1) If we were to draw a large number of equal-size samples from the population, the true population parameter should lie within this percentage of the intervals (sample_parameter - e, sample_parameter + e) where e is the margin_error.

        Optional debug arguments:
        :param breakpoint: If you want the learn process to stop at a given 'PHASE' checkout libs/phases


        :return:
        """


        from_ds = getDS(from_data)
        test_from_ds = test_from_data if test_from_data is None else getDS(test_from_data)

        transaction_type = TRANSACTION_LEARN

        predict_columns_map = {}

        # lets turn into lists: predict, order_by and group by
        predict_columns = [predict] if type(predict) != type([]) else predict
        group_by = group_by if type(group_by) == type([]) else [group_by] if group_by else []
        order_by = order_by if type(order_by) == type([]) else [order_by] if group_by else []

        if len(predict_columns) == 0:
            error = 'You need to specify a column to predict'
            log.error(error)
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
            log.warning('Note that after version 1.0, the default value for argument rename_strange_columns in MindsDB().learn, will be flipped from True to False, this means that if your data has columns with special characters, MindsDB will not try to rename them by default.')

        transaction_metadata = TransactionMetadata()
        transaction_metadata.model_name = model_name
        transaction_metadata.model_predict_columns = predict_columns
        transaction_metadata.model_columns_map = {} if rename_strange_columns else from_ds._col_map
        transaction_metadata.model_group_by = group_by
        transaction_metadata.model_order_by = order_by
        transaction_metadata.model_is_time_series = is_time_series
        transaction_metadata.window_size = window_size
        transaction_metadata.type = transaction_type
        transaction_metadata.from_data = from_ds
        transaction_metadata.test_from_data = test_from_ds
        transaction_metadata.ignore_columns = ignore_columns
        transaction_metadata.sample_margin_of_error = sample_margin_of_error
        transaction_metadata.sample_confidence_level = sample_confidence_level

        self.startInfoServer()
        self.session.newTransaction(transaction_metadata, breakpoint)


    def startInfoServer(self):
        pass

    def predict(self, when={}, from_data = None, model_name='mdsb_model', breakpoint= PHASE_END):
        """

        :param predict:
        :param when:
        :param model_name:
        :return:
        """



        transaction_type = TRANSACTION_PREDICT

        from_ds = None if from_data is None else getDS(from_data)

        transaction_metadata = TransactionMetadata()
        transaction_metadata.model_name = model_name

        # lets turn into lists: when
        when = when if type(when) in [type(None), type({})] else [when]


        # This will become irrelevant as if we have trained a model with a predict we just need to pass when or from_data
        # predict_columns = [predict] if type(predict) != type([]) else predict
        # transaction_metadata.model_predict_columns = predict_columns

        transaction_metadata.model_when_conditions = when
        transaction_metadata.type = transaction_type
        transaction_metadata.from_data = from_ds

        transaction = self.session.newTransaction(transaction_metadata, breakpoint)

        return transaction.output_data

    @staticmethod
    def checkForUpdates():
        # tmp files
        uuid_file = CONFIG.MINDSDB_STORAGE_PATH + '/../uuid.mdb_base'
        mdb_file = CONFIG.MINDSDB_STORAGE_PATH + '/start.mdb_base'

        uuid_file_path = Path(uuid_file)
        if uuid_file_path.is_file():
            uuid_str = open(uuid_file).read()
        else:
            uuid_str = str(uuid.uuid4())
            try:
                open(uuid_file, 'w').write(uuid_str)
            except:
                log.warning('Cannot store token, Please add write permissions to file:' + uuid_file)
                uuid_str = uuid_str + '.NO_WRITE'

        file_path = Path(mdb_file)
        if file_path.is_file():
            token = open(mdb_file).read()
        else:
            token = '{system}|{version}|{uid}'.format(system=platform.system(), version=MINDSDB_VERSION, uid=uuid_str)
            try:
                open(mdb_file,'w').write(token)
            except:
                log.warning('Cannot store token, Please add write permissions to file:'+mdb_file)
                token = token+'.NO_WRITE'
        extra = urllib.parse.quote_plus(token)
        try:
            r = requests.get('http://mindsdb.com/updates/check/{extra}'.format(extra=extra), headers={'referer': 'http://check.mindsdb.com/?token={token}'.format(token=token)})
        except:
            log.warning('Could not check for updates')
            return
        try:
            # TODO: Extract version, compare with version in version.py
            ret = r.json()

            if 'version' in ret and ret['version']!= MINDSDB_VERSION:
                pass
                #log.warning("There is a new version of MindsDB {version}, please do:\n    pip3 uninstall mindsdb\n    pip3 install mindsdb --user".format(version=ret['version']))
            else:
                log.debug('MindsDB is up to date!')

        except:

            log.warning('could not check for MindsDB updates')

        # zhihua
    def read_csv(self, filepath, delimiter=',', header='infer', encoding=None):
        data_df = pd.read_csv(filepath, delimiter=delimiter, encoding=encoding, header=header)
        self._from_data = data_df
        return self
