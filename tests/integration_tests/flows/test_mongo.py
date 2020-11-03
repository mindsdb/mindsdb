import unittest
import csv

from pymongo import MongoClient

from mindsdb.utilities.config import Config
from common import (
    run_environment,
    get_test_csv,
    run_container,
    TEST_CONFIG,
    USE_EXTERNAL_DB_SERVER,
    open_ssh_tunnel
)

from mindsdb.utilities.ps import wait_port

TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

EXTERNAL_DS_NAME = 'test_external'

config = Config(TEST_CONFIG)

MINDSDB_DATABASE = f"mindsdb_{config['api']['mongodb']['port']}" if USE_EXTERNAL_DB_SERVER else 'mindsdb'


class MongoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        mdb, datastore = run_environment(
            config,
            apis=['mongodb'],
            run_docker_db=[],
            override_integration_config={
                'default_mongodb': {
                    'enabled': True,
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
        else:
            run_container('mongo-cluster')
            wait_port(config['api']['mongodb']['port'], timeout=90)

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

        test_csv_path = get_test_csv(TEST_CSV['name'], TEST_CSV['url'])

        db = cls.mongos_client['test_data']
        colls = db.list_collection_names()

        if 'home_rentals' not in colls:
            print('creatating test data')
            with open(test_csv_path) as f:
                csvf = csv.reader(f)
                data = []
                for i, row in enumerate(csvf):
                    if i > 0:
                        data.append(dict(
                            number_of_rooms=int(row[0]),
                            number_of_bathrooms=int(row[1]),
                            sqft=int(float(row[2].replace(',', '.'))),
                            location=str(row[3]),
                            days_on_market=int(row[4]),
                            initial_price=int(row[5]),
                            neighborhood=str(row[6]),
                            rental_price=int(float(row[7]))
                        ))
                db['home_rentals'].insert_many(data)
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
        self.assertTrue('home_rentals' in test_data_collections)

        records_cunt = test_data['home_rentals'].count_documents({})
        self.assertTrue(records_cunt > 0)

    def test_2_learn_predictor(self):
        mindsdb = self.mongos_client[MINDSDB_DATABASE]
        mindsdb.predictors.insert_one({
            'name': TEST_PREDICTOR_NAME,
            'predict': 'rental_price',
            'select_data_query': {
                'database': 'test_data',
                'collection': 'home_rentals',
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

        result = mindsdb[TEST_PREDICTOR_NAME].find_one({'sqft': 1000})
        self.assertTrue(
            isinstance(result['rental_price'], int) or isinstance(result['rental_price'], float)
        )
        self.assertTrue(result['sqft'] == 1000)
        self.assertTrue('rental_price_min' in result)
        self.assertTrue('rental_price_max' in result)
        self.assertTrue('rental_price_explain' in result)
        self.assertTrue('rental_price_confidence' in result)

    def test_4_remove(self):
        mindsdb = self.mongos_client[MINDSDB_DATABASE]

        mindsdb.predictors.remove({'name': TEST_PREDICTOR_NAME})

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
