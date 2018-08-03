import sqlite3
import pandas
from libs.helpers.sqlite_helpers import *
from config import SQLITE_FILE
from libs.data_sources import *
from libs.controllers.session_controller import SessionController
from libs.constants.mindsdb import *

class MindsDB:

    def __init__(self, file=SQLITE_FILE):
        """

        :param file:
        """

        self.session = SessionController()
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


    def learn(self, from_query, predict, model_name='mdsb_model', test_query=None):
        """

        :param from_query:
        :param predict:
        :param model_name:
        :param test_query:
        :return:
        """

        type = TRANSACTION_LEARN

        predict_columns = [predict] if type(predict) != type([]) else predict

        transaction_metadata = {
            KEY_MODEL_NAME: model_name,
            KEY_MODEL_QUERY: from_query,
            KEY_MODEL_PREDICT_COLUMNS: predict_columns,
            KEY_MODEL_TEST_QUERY: test_query

        }

        self.session.newTransaction(type, transaction_metadata)


    def predict(self, predict, when={}, model_name='mdsb_model'):
        """

        :param predict:
        :param when:
        :param model_name:
        :return:
        """

        type = TRANSACTION_PREDICT

        predict_columns = [predict] if type(predict) != type([]) else predict

        transaction_metadata = {

            KEY_MODEL_NAME: model_name,
            KEY_MODEL_PREDICT_COLUMNS: predict_columns,
            KEY_MODEL_WHEN_CONDITIONS: when

        }

        self.session.newTransaction(type, transaction_metadata)
