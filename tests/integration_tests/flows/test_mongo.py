import unittest
import csv

from pymongo import MongoClient

from mindsdb.utilities.config import Config
from common import (
    run_environment,
    get_test_csv,
    TEST_CONFIG,
    run_container,
    wait_port
)

TEST_CSV = {
    'name': 'home_rentals.csv',
    'url': 'https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv'
}
TEST_DATA_TABLE = 'home_rentals'
TEST_PREDICTOR_NAME = 'test_predictor'

EXTERNAL_DS_NAME = 'test_external'

config = Config(TEST_CONFIG)

DOCKER_TIMEOUT = 60


class MongoTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_container('mongo-config')
        run_container('mongo-instance')
        ready = wait_port(27000, DOCKER_TIMEOUT)
        assert ready
        ready = wait_port(27001, DOCKER_TIMEOUT)
        assert ready

        cls.config_client = MongoClient('mongodb://localhost:27000/')
        cls.instance_client = MongoClient('mongodb://localhost:27001/')

        try:
            r = cls.config_client.admin.command('replSetInitiate', {
                '_id': 'replconf',
                'members': [
                    {'_id': 0, 'host': '127.0.0.1:27000'}
                ]
            })
        except Exception as e:
            if str(e) == 'already initialized':
                r = {'ok': 1}

        if bool(r['ok']) is not True:
            assert False

        try:
            r = cls.instance_client.admin.command('replSetInitiate', {
                '_id': 'replmain',
                'members': [
                    {'_id': 0, 'host': '127.0.0.1:27001'}
                ]
            })
        except Exception as e:
            if str(e) == 'already initialized':
                r = {'ok': 1}

        if bool(r['ok']) is not True:
            assert False

        mdb, datastore = run_environment('mongo', config, run_apis='mongo_only')
        cls.mdb = mdb

        models = cls.mdb.get_models()
        models = [x['name'] for x in models]
        if TEST_PREDICTOR_NAME in models:
            cls.mdb.delete_model(TEST_PREDICTOR_NAME)

        test_csv_path = get_test_csv(TEST_CSV['name'], TEST_CSV['url'])

        db = cls.instance_client['test_data']
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

        run_container('mongo-mongos')
        ready = wait_port(27002, DOCKER_TIMEOUT)
        assert ready
        cls.mongos_client = MongoClient('mongodb://localhost:27002/')

        cls.mongos_client.admin.command('addShard', 'replmain/127.0.0.1:27001')
        cls.mongos_client.admin.command('addShard', f"127.0.0.1:{config['api']['mongodb']['port']}")

    def test_1_entitys_exists(self):
        databases = self.mongos_client.list_database_names()
        self.assertTrue('test_data' in databases)
        self.assertTrue('mindsdb' in databases)

        mindsdb = self.mongos_client['mindsdb']
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
        mindsdb = self.mongos_client['mindsdb']
        mindsdb.predictors.insert({
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
        mindsdb = self.mongos_client['mindsdb']

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
        mindsdb = self.mongos_client['mindsdb']

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
