import sqlite3
import pandas
import requests
import logging
import os
import platform
import _thread
import uuid

from mindsdb.libs.helpers.sqlite_helpers import *
from mindsdb.config import SQLITE_FILE
import mindsdb.config as CONFIG

from mindsdb.libs.data_types.transaction_metadata import TransactionMetadata
from mindsdb.libs.controllers.session_controller import SessionController
from mindsdb.libs.constants.mindsdb import *
from pathlib import Path

from mindsdb.libs.data_sources.csv_file_ds import CSVFileDS

class MindsDBController:

    def __init__(self, file=SQLITE_FILE):
        """

        :param file:
        """

        logging.basicConfig(**CONFIG.PROXY_LOG_CONFIG)

        _thread.start_new_thread(MindsDBController.checkForUpdates, ())
        self.session = SessionController()
        self.storage_file = file
        self.conn = sqlite3.connect(file)
        self.conn.create_aggregate("first_value", 1, FirstValueAgg)
        self.conn.create_aggregate("array_agg_json", 2, ArrayAggJSON)

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


    def learn(self, predict, from_query=None, from_file=None, model_name='mdsb_model', test_query=None, group_by = None, group_by_limit = MODEL_GROUP_BY_DEAFAULT_LIMIT, order_by = [], breakpoint = PHASE_END):
        """

        :param from_query:
        :param predict:
        :param model_name:
        :param test_query:
        :return:
        """

        if from_file is not None:
            from_file_dest = os.path.basename(from_file).split('.')[0]
            self.addTable(CSVFileDS(from_file), from_file_dest)
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
        transaction_metadata.model_group_by_limit = group_by_limit
        transaction_metadata.type = transaction_type

        self.startInfoServer()
        self.session.newTransaction(transaction_metadata, breakpoint)


    def startInfoServer(self):
        pass

    def predict(self, predict, when={}, model_name='mdsb_model', breakpoint= PHASE_END):
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

        r = requests.get('http://mindsdb.com/checkupdates?token={token}'.format(token=token))
        try:
            ret = r.json()
            if 'new_version' in ret:
                logging.warn('There is an update available for mindsdb, please go: pip install mindsdb --upgrade')
        except:
            logging.warning('could not check for MindsDB updates')
