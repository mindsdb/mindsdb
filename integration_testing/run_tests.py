from data_generators import *
import traceback
import sys
import os
import itertools
import logging
from colorlog import ColoredFormatter
import time

# Not working for some reason, we need mindsdb in PYPATH for now
#sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')
#print(os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')

import mindsdb
from mindsdb import CONST

types_that_fail = ['str','ascii']
types_that_work = ['int','float','date','datetime','timestamp']

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

def test_timeseries():
    logger.info('Starting timeseries test !')
    ts_hours = 12
    separator = ','
    data_len = 1200
    train_file_name = 'train_data.csv'
    test_file_name = 'test_data.csv'

    # Create the full dataset
    logger.debug(f'Creating timeseries test datasets and saving them to {train_file_name} and {test_file_name}, total dataset size will be {data_len} rows')

    try:
        features = generate_value_cols(['datetime','int','float','ascii','ascii'],data_len, separator, ts_hours * 3600)
        features[3] = list(map(lambda x: str(x[0]) if len(x) > 0 else 'Nrmm', features[3]))
        labels = [generate_labels_1(features, separator)]

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
        mdb = mindsdb.Predictor(name='test_date_timeseries')
        logger.debug(f'Succesfully create mindsdb Predictor')
    except:
        logger.error(f'Failed to create mindsdb Predictor')
        exit(1)


    try:
        mdb.learn(
            from_data=train_file_name,
            to_predict=label_headers
            # timeseries specific argsw
            ,order_by=feature_headers[0]
            ,window_size_seconds=ts_hours* 3600 * 1.5
            ,group_by = feature_headers[3]
        )
        logger.info(f'--------------- Learning ran succesfully ---------------')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed during the training !')
        exit(1)

    # Predict
    try:
        mdb = mindsdb.Predictor(name='test_date_timeseries')
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
    except:
        print(traceback.format_exc())
        logger.error(f'Failed whilst predicting')
        exit(1)

    logger.info('Timeseries test ran succesfully !')


def test_one_label_prediction():
    logger.info('Starting one-label test')
    separator = ','
    train_file_name = 'train_data.csv'
    test_file_name = 'test_data.csv'
    data_len = 8000

    # Create the full dataset
    logger.debug(f'Creating one-labe test datasets and saving them to {train_file_name} and {test_file_name}, total dataset size will be {data_len} rows')

    try:
        features = generate_value_cols(['int','float','ascii','ascii'],data_len, separator)
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
    except:
        print(traceback.format_exc())
        logger.error(f'Failed whilst predicting')
        exit(1)

    logger.info('One-label prediction test ran succesfully !')

def test_multilabel_prediction():
    logger.info('Starting multilabel prediction test')
    separator = ','
    train_file_name = 'train_data.csv'
    test_file_name = 'test_data.csv'
    data_len = 600

    # Create the full dataset
    logger.debug(f'Creating multilabel test datasets and saving them to {train_file_name} and {test_file_name}, total dataset size will be {data_len} rows')

    try:
        features = generate_value_cols(['int','float','int','float'],data_len, separator)
        labels = []
        labels.append(generate_labels_2(features, separator))
        labels.append(generate_labels_2(features, separator))

        feature_headers = list(map(lambda col: col[0], features))
        label_headers = list(map(lambda col: col[0], labels))

        # Create the training dataset and save it to a file
        columns_train = list(map(lambda col: col[1:int(len(col)*3/4)], features))
        columns_train.extend(list(map(lambda col: col[1:int(len(col)*3/4)], labels)))
        columns_to_file(columns_train, train_file_name, separator, headers=[*feature_headers,*label_headers])

        # Create the testing dataset and save it to a file
        columns_test = list(map(lambda col: col[int(len(col)*3/4):], features))
        columns_to_file(columns_test, test_file_name, separator, headers=feature_headers)
        logger.debug(f'Multilabel datasets generate and saved to files successfully')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed to generate datasets !')
        exit(1)

    # Train
    mdb = None
    try:
        mdb = mindsdb.Predictor(name='test_multilabel_prediction')
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
        mdb = mindsdb.Predictor(name='test_multilabel_prediction')
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
    except:
        print(traceback.format_exc())
        logger.error(f'Failed whilst predicting')
        exit(1)

    logger.info('Multilabel predict test ran succesfully !')

    separator = ','
    data_file_name = 'test_data.csv'
    data_len = 10000


setup_testing_logger()
test_multilabel_prediction()
#test_timeseries()
#test_one_label_prediction()
