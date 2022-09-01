import os
import unittest
import tempfile
import json
from pathlib import Path

from lightwood.mixer import LightGBM

temp_dir = tempfile.mkdtemp(dir='/tmp/', prefix='lightwood_handler_test_')
os.environ['MINDSDB_STORAGE_DIR'] = temp_dir
os.environ['MINDSDB_DB_CON'] = 'sqlite:///' + os.path.join(os.environ['MINDSDB_STORAGE_DIR'], 'mindsdb.sqlite3.db') + '?check_same_thread=False&timeout=30'

from mindsdb.migrations import migrate
migrate.migrate_to_head()

from mindsdb.utilities.config import Config
from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.utils import load_predictor
# from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from mindsdb.integrations.handlers.postgres_handler import Handler as PGHandler
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper
from mindsdb.integrations.libs.response import RESPONSE_TYPE


PG_CONNECTION_DATA = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
}

PG_HANDLER_NAME = 'test_handler'


class HandlerControllerMock:
    def __init__(self):
        self.handlers = {
            PG_HANDLER_NAME: PGHandler(
                PG_HANDLER_NAME,
                **{"connection_data": PG_CONNECTION_DATA}
            )
        }

    def get_handler(self, name):
        return self.handlers[name]

    def get(self, name):
        return {
            'id': 0,
            'name': PG_HANDLER_NAME
        }


# TODO: drop all models and tables when closing tests
class LightwoodHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        handler_controller = HandlerControllerMock()

        cls.handler = LightwoodHandler(
            'lightwood',
            handler_controller=handler_controller,
            fs_store=WithKWArgsWrapper(FsStore(), company_id=None),
            model_controller=WithKWArgsWrapper(ModelController(), company_id=None)
        )
        cls.config = Config()

        cls.target_1 = 'rental_price'
        cls.data_table_1 = 'demo_data.home_rentals'
        cls.test_model_1 = 'test_lightwood_home_rentals'
        cls.test_model_1b = 'test_lightwood_home_rentals_custom'

        cls.target_2 = 'Traffic'
        cls.data_table_2 = 'demo_data.house_sales'
        cls.test_model_2 = 'test_lightwood_house_sales'

    def test_00_check_connection(self):
        conn = self.handler.check_connection()
        assert conn['status'] == '200'

    def test_01_drop_predictor(self):
        if self.test_model_1 not in self.handler.get_tables().data_frame.values:
            # TODO: seems redundant because of test_02
            query = f"""
                CREATE PREDICTOR {self.test_model_1}
                FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_1} limit 50)
                PREDICT rental_price
            """
            self.handler.native_query(query)
        response = self.handler.native_query(f"DROP PREDICTOR {self.test_model_1}")
        self.assertTrue(response.type == RESPONSE_TYPE.OK)

    def test_02_train_predictor(self):
        query = f"""
            CREATE PREDICTOR {self.test_model_1}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_1} limit 50)
            PREDICT rental_price
        """
        response = self.handler.native_query(query)
        self.assertTrue(response.type == RESPONSE_TYPE.OK)

    def test_03_retrain_predictor(self):
        query = f"RETRAIN {self.test_model_1}"
        response = self.handler.native_query(query)
        self.assertTrue(response.type == RESPONSE_TYPE.OK)

    def test_04_query_predictor_single_where_condition(self):
        query = f"""
            SELECT target
            from {self.test_model_1}
            WHERE sqft=100
        """
        response = self.handler.native_query(query)
        self.assertTrue(response.type == RESPONSE_TYPE.TABLE)
        self.assertTrue(len(response.data_frame) == 1)
        self.assertTrue(response.data_frame['sqft'][0] == 100)
        self.assertTrue(response.data_frame['rental_price'][0] is not None)

    def test_05_query_predictor_multi_where_condition(self):
        query = f"""
            SELECT target
            from {self.test_model_1}
            WHERE sqft=100
                  AND number_of_rooms=2
                  AND number_of_bathrooms=1
        """
        response = self.handler.native_query(query)
        self.assertTrue(response.type == RESPONSE_TYPE.TABLE)
        self.assertTrue(len(response.data_frame) == 1)
        self.assertTrue(response.data_frame['number_of_rooms'][0] == 2)
        self.assertTrue(response.data_frame['number_of_bathrooms'][0] == 1)

    def test_06_train_predictor_custom_jsonai(self):
        # TODO: turn this into a decorator?
        if self.test_model_1b in self.handler.get_tables().data_frame.values:  # TODO this accesor feels weird, maybe rethink output format?
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_1b}")

        using_str = 'model.args={"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": true}}]}'
        query = f"""
            CREATE PREDICTOR {self.test_model_1b}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_1} limit 50)
            PREDICT rental_price
            USING {using_str}
        """
        response = self.handler.native_query(query)
        self.assertTrue(response.type == RESPONSE_TYPE.OK)
        # TODO assert
        # m = load_predictor(self.handler.storage.get('models')[self.test_model_1b], self.test_model_1b)
        # assert len(m.ensemble.mixers) == 1
        # assert isinstance(m.ensemble.mixers[0], LightGBM)

    def test_07_list_tables(self):
        response = self.handler.get_tables()
        self.assertTrue(response.type == RESPONSE_TYPE.TABLE)
        print(response.data_frame)

    def test_08_get_columns(self):
        response = self.handler.get_columns(f'{self.test_model_1}')
        self.assertTrue(response.type == RESPONSE_TYPE.TABLE)
        print(response.data_frame)

    # TODO
    # def test_09_join_predictor_into_table(self):
    #     into_table = 'test_join_into_lw'
    #     query = f"SELECT tb.{self.target_1} as predicted, ta.{self.target_1} as truth, ta.sqft from {PG_HANDLER_NAME}.{self.data_table_1} AS ta JOIN {self.test_model_1} AS tb LIMIT 10"
    #     parsed = self.handler.parser(query, dialect=self.handler.dialect)
    #     predicted = self.handler.join(parsed, self.data_handler, into=into_table)

    #     # checks whether `into` kwarg does insert into the table or not
    #     q = f"SELECT * FROM {into_table}"
    #     qp = self.handler.parser(q, dialect='mysql')
    #     assert len(self.data_handler.query(qp).data_frame) > 0

    def test_10_train_ts_predictor_multigby_hor4(self):
        # TODO: handle cap/uncapped column name returned from data handler? Had to rename 'MA' -> 'ma' for test to pass
        query = f"""
            CREATE PREDICTOR {self.test_model_2}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_2})
            PREDICT ma
            ORDER BY saledate
            GROUP BY bedrooms, type
            WINDOW 8
            HORIZON 4
        """
        if self.test_model_2 not in self.handler.get_tables().data_frame.values:
            response = self.handler.native_query(query)
        else:
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_2}")
            response = self.handler.native_query(query)

        self.assertTrue(response.type == RESPONSE_TYPE.OK)

        # TODO: reactivate and add to the rest of the TS tests once cache is back on
        # p = self.handler.storage.get('models')
        # m = load_predictor(p[self.test_model_2], self.test_model_2)
        # assert m.problem_definition.timeseries_settings.is_timeseries

    def test_12_train_ts_predictor_multigby_hor1(self):
        query = f"""
            CREATE PREDICTOR {self.test_model_2}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_2})
            PREDICT ma
            ORDER BY saledate
            GROUP BY bedrooms, type
            WINDOW 8
            HORIZON 1
        """
        if self.test_model_2 not in self.handler.get_tables().data_frame.values:
            self.handler.native_query(query)
        else:
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_2}")
            self.handler.native_query(query)

    def test_13_train_ts_predictor_no_gby_hor1(self):
        query = f"""
            CREATE PREDICTOR {self.test_model_2}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_2})
            PREDICT ma
            ORDER BY saledate
            WINDOW 8
            HORIZON 1
        """
        if self.test_model_2 not in self.handler.get_tables().data_frame.values:
            self.handler.native_query(query)
        else:
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_2}")
            self.handler.native_query(query)

    def test_14_train_ts_predictor_no_gby_hor4(self):
        query = f"""
            CREATE PREDICTOR {self.test_model_2}
            FROM {PG_HANDLER_NAME} (SELECT * FROM {self.data_table_2})
            PREDICT ma
            ORDER BY saledate
            WINDOW 8
            HORIZON 4
        """
        if self.test_model_2 not in self.handler.get_tables().data_frame.values:
            self.handler.native_query(query)
        else:
            self.handler.native_query(f"DROP PREDICTOR {self.test_model_2}")
            self.handler.native_query(query)

    # TODO
    # def test_15_join_predictor_ts_into(self):
    #     query = f"""
    #         SELECT m.saledate as date,
    #                m.ma as forecast
    #         FROM mindsdb.{self.test_model_2} m JOIN {PG_HANDLER_NAME}.demo_data.house_sales t
    #         WHERE t.saledate > LATEST
    #               AND t.type = 'house'
    #               AND t.bedrooms = 2
    #         LIMIT 10
    #     """
    #     response = self.handler.native_query(query)
    #     self.assertTrue(response.type == RESPONSE_TYPE.TABLE)

    # def test_16_join_predictor_ts_model_left(self):
    #     # TODO: is this one needed?
    #     target = 'Traffic'
    #     oby = 'T'
    #     query = f"SELECT tb.{target} as predicted, ta.{target} as truth, ta.{oby} from mindsdb.{self.test_tsmodel_name_1} AS tb JOIN {self.sql_handler_name}.{self.data_table_name_2} AS ta ON 1=1 WHERE ta.{oby} > LATEST LIMIT 10"
    #     parsed = self.handler.parser(query, dialect=self.handler.dialect)
    #     predicted = self.handler.join(parsed, self.data_handler)  # , into=self.model_2_into_table) # TODO: restore when we add support for SQLite and other handlers for `into`


if __name__ == "__main__":
    unittest.main(failfast=True)