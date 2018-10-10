import sqlite3
import pandas
import requests
import logging
import os
import platform
import _thread
import uuid
import traceback

from mindsdb.libs.helpers.sqlite_helpers import *
from mindsdb.config import SQLITE_FILE
import mindsdb.config as CONFIG

from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.controllers.session_controller import SessionController
from mindsdb.libs.constants.mindsdb import *
from pathlib import Path

from mindsdb.libs.data_types.data_source import DataSource
from mindsdb.libs.data_sources.csv_file_ds import CSVFileDS
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
                os.mkdir(CONFIG.MINDSDB_STORAGE_PATH)
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


    def learn(self, predict, from_file=None, from_data = None, model_name='mdsb_model', test_query=None, group_by = None, window_size = MODEL_GROUP_BY_DEAFAULT_LIMIT, order_by = [], breakpoint = PHASE_END):
        """

        :param from_query:
        :param predict:
        :param model_name:
        :param test_query:
        :return:
        """

        from_ds = None
        from_query = None

        # automatically detect if what type of data this is
        # TODO: Do the same with test_from_data
        if from_data is not None:
            if isinstance(from_data, DataSource):
                from_ds = from_data
            elif Path(from_data).is_file():
                from_file = from_data
            else: # assume is a query
                from_query = from_data

        # if its a file or a data frame load it and build a from_query
        if from_file is not None or from_ds is not None:
            if from_file is not None:
                from_file_dest = os.path.basename(from_file).split('.')[0]
                # TODO: Support from datasource or dataframe and skip table creation if possible
                ds = CSVFileDS(from_file)
            else:
                from_file_dest = model_name
                ds = from_ds
            predict=ds.getColNameAsInDF(predict)
            self.addTable(ds, from_file_dest)
            ds = None
            if from_query is None:
                from_query = 'select * from {from_file_dest}'.format(from_file_dest=from_file_dest)
                logging.info('setting up custom learn query for file. '+from_query)

        transaction_type = TRANSACTION_LEARN

        predict_columns = [predict] if type(predict) != type([]) else predict

        transaction_metadata = TransactionMetadata()
        transaction_metadata.model_name = model_name
        transaction_metadata.model_query = from_query
        transaction_metadata.model_predict_columns = predict_columns
        transaction_metadata.model_test_query = test_query
        transaction_metadata.model_group_by = group_by
        transaction_metadata.model_order_by = order_by if type(order_by) == type([]) else [order_by]
        transaction_metadata.window_size = window_size
        transaction_metadata.type = transaction_type
        #transaction_metadata.from_data = from_data

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

        predict_columns = [predict] if type(predict) != type([]) else predict

        transaction_metadata = TransactionMetadata()
        transaction_metadata.model_name = model_name
        transaction_metadata.model_predict_columns = predict_columns
        transaction_metadata.model_when_conditions = when
        transaction_metadata.type = transaction_type
        transaction_metadata.storage_file = self.storage_file
        transaction_metadata.from_data = from_data

        transaction = self.session.newTransaction(transaction_metadata, breakpoint)

        return transaction.output_data

    @staticmethod
    def checkForUpdates():
        # tmp files
        mdb_file = CONFIG.MINDSDB_STORAGE_PATH + '/start.mdb_base'
        file_path = Path(mdb_file)
        if file_path.is_file():
            token = open(mdb_file).read()
        else:
            token = '{system}|{version}|{uid}'.format(system=platform.system(), version=MDB_VERSION, uid=str(uuid.uuid4()))
            try:
                open(mdb_file,'w').write(token)
            except:
                logging.warn('Cannot store token, Please add write permissions to file:'+mdb_file)
                token = token+'.NO_WRITE'

        r = requests.get('https://github.com/mindsdb/main/blob/master/version.py', headers={'referer': 'http://check.mindsdb.com/?token={token}'.format(token=token)})
        try:
            # TODO: Extract version, compare with version in version.py
            # ret = r.json()
            # if 'new_version' in ret:
            #     logging.warn('There is an update available for mindsdb, please go: pip install mindsdb --upgrade')
            pass
        except:
            logging.warning('could not check for MindsDB updates')


