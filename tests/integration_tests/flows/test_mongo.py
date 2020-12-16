import unittest
import csv
from pathlib import Path

from pymongo import MongoClient

from mindsdb.state.config import Config
from common import (
    USE_EXTERNAL_DB_SERVER,
    DATASETS_COLUMN_TYPES,
    check_prediction_values,
    TEST_CONFIG,
    run_environment,
    open_ssh_tunnel,
    DATASETS_PATH
)

from mindsdb.utilities.ps import wait_port

# +++ define test data
TEST_DATASET = 'concrete_strength'

DB_TYPES_MAP = {
    int: 'int',
    float: 'float',
    str: 'text'
}

TO_PREDICT = {
    'concrete_strength': float,
    'cement': float
}
CONDITION = {
    'water': 162.5,
    'age': 28
}
# ---

TEST_DATA_TABLE = TEST_DATASET
TEST_PREDICTOR_NAME = f'{TEST_DATASET}_predictor'
EXTERNAL_DS_NAME = f'{TEST_DATASET}_external'

config = Config(TEST_CONFIG)

MINDSDB_DATABASE = f"mindsdb_{config['api']['mongodb']['port']}" if USE_EXTERNAL_DB_SERVER else 'mindsdb'


class MongoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment(
            config,
            apis=['mongodb'],
            override_integration_config={
                'default_mongodb': {
                    'publish': True,
                    'port': 27002,
                    'host': '127.0.0.1',
                    'type': 'mongodb',
                    'user': '',
                    'password': ''
                }
            },
            mindsdb_database=MINDSDB_DATABASE
        )
        cls.mdb = mdb

        if USE_EXTERNAL_DB_SERVER:
            open_ssh_tunnel(27002, direction='L')   # 27002 - mongos port
            wait_port(27002, timeout=10)

        cls.mongos_client = MongoClient('mongodb://127.0.0.1:27002/')
        mdb_shard = f"127.0.0.1:{config['api']['mongodb']['port']}"
        try:
            cls.mongos_client.admin.command('removeShard', mdb_shard)
        except Exception:
            # its ok if shard not exiss
            pass

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        if not USE_EXTERNAL_DB_SERVER:
            test_csv_path = Path(DATASETS_PATH).joinpath(TEST_DATASET).joinpath('data.csv')

            db = cls.mongos_client['test_data']
            colls = db.list_collection_names()

            if TEST_DATASET not in colls:
                print('creatating test data')
                with open(test_csv_path) as f:
                    csvf = csv.reader(f)
                    data = []
                    DS = DATASETS_COLUMN_TYPES[TEST_DATASET]
                    for i, row in enumerate(csvf):
                        if i == 0:
                            continue
                        data.append({
                            column[0]: column[1](row[i])
                            for i, column in enumerate(DS)
                        })
                    db[TEST_DATASET].insert_many(data)
                print('done')

        cls.mongos_client.admin.command('addShard', mdb_shard)

    def test_1_entitys_exists(self):
        mindsdb = self.mongos_client[MINDSDB_DATABASE]
        mindsdb_collections = mindsdb.list_collection_names()
        self.assertTrue('predictors' in mindsdb_collections)
        self.assertTrue('commands' in mindsdb_collections)
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_collections)

        test_data = self.mongos_client['test_data']
        test_data_collections = test_data.list_collection_names()
        self.assertTrue(TEST_DATASET in test_data_collections)

        records_cunt = test_data[TEST_DATASET].count_documents({})
        self.assertTrue(records_cunt > 0)

    def test_2_learn_predictor(self):
        mindsdb = self.mongos_client[MINDSDB_DATABASE]
        mindsdb.predictors.insert_one({
            'name': TEST_PREDICTOR_NAME,
            'predict': list(TO_PREDICT.keys()),
            'select_data_query': {
                'database': 'test_data',
                'collection': TEST_DATASET,
                'find': {}
            },
            'training_options': {
                'join_learn_process': True,
                'stop_training_in_x_seconds': 3
            }
        })

        predictors = list(mindsdb.predictors.find())
        self.assertTrue(TEST_PREDICTOR_NAME in [x['name'] for x in predictors])

        mindsdb_collections = mindsdb.list_collection_names()
        self.assertTrue(TEST_PREDICTOR_NAME in mindsdb_collections)

    def test_3_predict(self):
        mindsdb = self.mongos_client[MINDSDB_DATABASE]

        result = mindsdb[TEST_PREDICTOR_NAME].find_one(CONDITION)
        self.assertIsInstance(result, dict)
        self.assertTrue(check_prediction_values(result, TO_PREDICT))

    def test_4_remove(self):
        mindsdb = self.mongos_client[MINDSDB_DATABASE]

        mindsdb.predictors.delete_one({'name': TEST_PREDICTOR_NAME})

        predictors = list(mindsdb.predictors.find())
        self.assertTrue(TEST_PREDICTOR_NAME not in [x['name'] for x in predictors])

        mindsdb_collections = mindsdb.list_collection_names()
        self.assertTrue(TEST_PREDICTOR_NAME not in mindsdb_collections)


if __name__ == "__main__":
    try:
        unittest.main(failfast=True)
        print('Tests passed!')
    except Exception as e:
        print(f'Tests Failed!\n{e}')
