import sqlite3
import pandas
import requests
import logging
import os
import platform
import _thread
import uuid
import traceback
import urllib

from mindsdb.libs.helpers.sqlite_helpers import *
from mindsdb.libs.helpers.multi_data_source import getDS
from mindsdb.config import SQLITE_FILE
import mindsdb.config as CONFIG

from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.controllers.session_controller import SessionController
from mindsdb.libs.constants.mindsdb import *

from mindsdb.version import mindsdb_version as MINDSDB_VERSION

from pathlib import Path

class MindsDBController:

    def __init__(self, file=SQLITE_FILE):
        """

        :param file:
        """

        self.setConfigs()

        _thread.start_new_thread(MindsDBController.checkForUpdates, ())
        self.session = SessionController()
        self.storage_file = file
        self.conn = sqlite3.connect(file)
        self.conn.create_aggregate("first_value", 1, FirstValueAgg)
        self.conn.create_aggregate("array_agg_json", 2, ArrayAggJSON)

    def setConfigs(self):
        """
        This sets the config settings for this mindsdb instance
        :return:
        """
        # set logging settings
        logging.basicConfig(**CONFIG.PROXY_LOG_CONFIG)

        # set the mindsdb storage folder
        storage_ok = True # default state

        # if it does not exist try to create it
        if not os.path.exists(CONFIG.MINDSDB_STORAGE_PATH):
            try:
                logging.info('{folder} does not exist, creating it now'.format(folder=CONFIG.MINDSDB_STORAGE_PATH))
                path = Path(CONFIG.MINDSDB_STORAGE_PATH)
                path.mkdir(exist_ok=True, parents=True)
            except:
                logging.info(traceback.format_exc())
                storage_ok = False
                logging.error('MindsDB storate foldler: {folder} does not exist and could not be created'.format(folder=CONFIG.MINDSDB_STORAGE_PATH))

        # If storage path is not writable, raise an exception as this can no longer be
        if not os.access(CONFIG.MINDSDB_STORAGE_PATH, os.W_OK) or storage_ok == False:
            error_message = '''Cannot write into storage path, please either set the config variable mindsdb.config.set('MINDSDB_STORAGE_PATH',<path>) or give write access to {folder}'''
            raise ValueError(error_message.format(folder=CONFIG.MINDSDB_STORAGE_PATH))


    def addTable(self, ds, as_table):
        """

        :param ds:
        :param as_table:
        :return:
        """

        ds.df.to_sql(as_table, self.conn, if_exists='replace', index=False)

    def query(self, query):
        """

        :param query:
        :return:
        """

        cur = self.conn.cursor()
        return cur.execute(query)

    def queryToDF(self, query):
        """

        :param query:
        :return:
        """

        return pandas.read_sql_query(query, self.conn)


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
            logging.warning('Cannot store token, Please add write permissions to file:' + email_file)
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
            logging.warning('Cannot read email, Please add write permissions to file:' + email_file)
            return None

    def learn(self, predict, from_file=None, from_data = None, model_name='mdsb_model', test_from_data=None, group_by = None, window_size = MODEL_GROUP_BY_DEAFAULT_LIMIT, order_by = [], breakpoint = PHASE_END, ignore_columns = []):
        """

        :param from_query:
        :param predict:
        :param model_name:
        :param test_query:
        :return:
        """

        from_ds = getDS(from_data) if from_file is None else getDS(from_file)
        test_from_ds = test_from_data if test_from_data is None else getDS(test_from_data)

        transaction_type = TRANSACTION_LEARN

        predict_columns = [predict] if type(predict) != type([]) else predict

        transaction_metadata = TransactionMetadata()
        transaction_metadata.model_name = model_name
        transaction_metadata.model_predict_columns = predict_columns
        transaction_metadata.model_group_by = group_by
        transaction_metadata.model_order_by = order_by if type(order_by) == type([]) else [order_by]
        transaction_metadata.window_size = window_size
        transaction_metadata.type = transaction_type
        transaction_metadata.from_data = from_ds
        transaction_metadata.test_from_data = test_from_ds
        transaction_metadata.ignore_columns = ignore_columns

        self.startInfoServer()
        self.session.newTransaction(transaction_metadata, breakpoint)


    def startInfoServer(self):
        pass

    def predict(self, predict, from_data = None, when={}, model_name='mdsb_model', breakpoint= PHASE_END):
        """

        :param predict:
        :param when:
        :param model_name:
        :return:
        """

        transaction_type = TRANSACTION_PREDICT

        from_ds = None if from_data is None else getDS(from_data)

        predict_columns = [predict] if type(predict) != type([]) else predict

        transaction_metadata = TransactionMetadata()
        transaction_metadata.model_name = model_name
        transaction_metadata.model_predict_columns = predict_columns
        transaction_metadata.model_when_conditions = when
        transaction_metadata.type = transaction_type
        transaction_metadata.storage_file = self.storage_file
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
                logging.warning('Cannot store token, Please add write permissions to file:' + uuid_file)
                uuid_str = uuid_str + '.NO_WRITE'

        file_path = Path(mdb_file)
        if file_path.is_file():
            token = open(mdb_file).read()
        else:
            token = '{system}|{version}|{uid}'.format(system=platform.system(), version=MINDSDB_VERSION, uid=uuid_str)
            try:
                open(mdb_file,'w').write(token)
            except:
                logging.warning('Cannot store token, Please add write permissions to file:'+mdb_file)
                token = token+'.NO_WRITE'
        extra = urllib.parse.quote_plus(token)
        try:
            r = requests.get('http://mindsdb.com/updates/check/{extra}'.format(extra=extra), headers={'referer': 'http://check.mindsdb.com/?token={token}'.format(token=token)})
        except:
            logging.warning('Could not check for updates')
            return
        try:
            # TODO: Extract version, compare with version in version.py
            ret = r.json()

            if 'version' in ret and ret['version']!= MINDSDB_VERSION:
                logging.warning("There is a new version of MindsDB {version}, please do:\n    pip3 uninstall mindsdb\n    pip2 install mindsdb --user".format(version=ret['version']))
            else:
                logging.debug('MindsDB is up to date!')

        except:

            logging.warning('could not check for MindsDB updates')


