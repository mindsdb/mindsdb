from data_generators import *
import traceback
import sys
import os
import itertools
import logging
from colorlog import ColoredFormatter
import time

import mindsdb
from mindsdb import CONST

types_that_work = ['int','float','date','datetime','timestamp','ascii']

logger = None

def setup_testing_logger():
    global logger
    formatter = ColoredFormatter(
        "%(log_color)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'black,bg_white',
            'INFO':     'blue,bg_white',
            'WARNING':  'orange,bg_white',
            'ERROR':    'red,bg_white',
            'CRITICAL': 'red,bg_white',
        }
    )

    logger = logging.getLogger('mindsdb_integration_testing')
    logger.handlers = []
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

def run_tests():
    logger.info('Starting one-label test')
    separator = ','
    train_file_name = 'train_data.csv'
    test_file_name = 'test_data.csv'
    data_len = 8000

    # Create the full dataset
    logger.debug(f'Creating one-labe test datasets and saving them to {train_file_name} and {test_file_name}, total dataset size will be {data_len} rows')

    try:
        features = generate_value_cols(types_that_work,data_len, separator)
        labels = [generate_labels_2(features, separator)]

        feature_headers = list(map(lambda col: col[0], features))
        label_headers = list(map(lambda col: col[0], labels))

        # Create the training dataset and save it to a file
        columns_train = list(map(lambda col: col[1:int(len(col)*3/4)], features))
        columns_train.extend(list(map(lambda col: col[1:int(len(col)*3/4)], labels)))
        columns_to_file(columns_train, train_file_name, separator, headers=[*feature_headers,*label_headers])

        # Create the testing dataset and save it to a file
        columns_test = list(map(lambda col: col[int(len(col)*3/4):], features))
        columns_to_file(columns_test, test_file_name, separator, headers=feature_headers)
        logger.debug(f'Datasets generate and saved to files successfully')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed to generate datasets !')
        exit(1)

    # Train
    mdb = None
    try:
        mdb = mindsdb.Predictor(name='test_one_label_prediction')
        logger.debug(f'Succesfully create mindsdb Predictor')
    except:
        logger.error(f'Failed to create mindsdb Predictor')
        exit(1)


    try:
        mdb.learn(from_data=train_file_name, to_predict=label_headers)
        logger.info(f'--------------- Learning ran succesfully ---------------')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed during the training !')
        exit(1)

    # Predict
    try:
        mdb = mindsdb.Predictor(name='test_one_label_prediction')
        logger.debug(f'Succesfully create mindsdb Predictor')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed to create mindsdb Predictor')
        exit(1)

    try:
        results = mdb.predict(when_data=test_file_name)
        for row in results:
            expect_columns = [label_headers[0] ,label_headers[0] + '_confidence']
            for col in expect_columns:
                if col not in row:
                    logger.error(f'Prediction failed to return expected column: {col}')
                    logger.debug('Got row: {}'.format(row))
                    exit(1)

        logger.info(f'--------------- Predicting ran succesfully ---------------')

        models = mdb.get_models()
        print(models)
    except:
        print(traceback.format_exc())
        logger.error(f'Failed whilst predicting')
        exit(1)

    logger.info('Travis CLI Tests ran succesfully !')


setup_testing_logger()
run_tests()
