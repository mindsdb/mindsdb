from data_generators import *
import traceback
import sys
import os
import itertools
import logging
from colorlog import ColoredFormatter

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
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

def test_timeseries():
    logger.info('Starting timeseries test !')
    ts_hours = 12
    separator = ','
    data_len = 4200
    train_file_name = 'train_data.csv'
    test_file_name = 'test_data.csv'

    # Create the full dataset
    logger.debug(f'Creating timeseries test datasets and saving them to {train_file_name} and {test_file_name}, total dataset size will be {data_len} rows')

    try:
        fetrues = generate_value_cols(['date','int','float'],data_len, separator, ts_hours * 3600)
        labels = [generate_labels_1(fetrues, separator)]

        feature_headers = list(map(lambda col: col[0], fetrues))
        label_headers = list(map(lambda col: col[0], labels))

        # Create the training dataset and save it to a file
        columns_train = list(map(lambda col: col[1:int(len(col)*3/4)], fetrues))
        columns_train.extend(list(map(lambda col: col[1:int(len(col)*3/4)], labels)))
        columns_to_file(columns_train, train_file_name, separator, headers=[*label_headers,*feature_headers])

        # Create the testing dataset and save it to a file
        columns_test = list(map(lambda col: col[int(len(col)*3/4):], fetrues))
        columns_to_file(columns_test, test_file_name, separator, headers=feature_headers)
        logger.debug(f'Datasets generate and saved to files successfully')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed to generate datasets !')
        exit()

    # Train
    try:
        mdb = mindsdb.Predictor(name='test_datetime_timeseries')
        logger.debug(f'Succesfully create mindsdb Predictor')
    except:
        logger.errror(f'Failed to create mindsdb Predictor')
        exit()


    try:
        mdb.learn(
            from_data=train_file_name,
            to_predict=label_headers
            # timeseries specific argsw
            ,order_by=feature_headers[0]
            ,window_size=ts_hours*(data_len/100)
            #,group_by = columns[0][0]
        )
        logger.info(f'--------------- Learning ran succesfully ---------------')
    except:
        print(traceback.format_exc())
        logger.error(f'Failed during the training !')
        exit()

    # Predict
    try:
        mdb = mindsdb.Predictor(name='test_datetime_timeseries')
        logger.debug(f'Succesfully create mindsdb Predictor')
    except:
        print(traceback.format_exc())
        logger.errror(f'Failed to create mindsdb Predictor')
        exit()

    try:
        results = mdb.predict(when_data=test_file_name)
        for row in results:
            logger.debug(row)
        logger.info(f'--------------- Predicting ran succesfully ---------------')
    except:
        print(traceback.format_exc())
        logger.errror(f'Failed whilst predicting')
        exit()

    logger.info('Timeseries test ran succesfully !')

# Keep whilst testing timeseries speicifc stuff, comment or remove in production builds
setup_testing_logger()
test_timeseries()
exit()

def test_one_label_prediction():
    separator = ','
    train_file_name = 'train_data.csv'
    test_file_name = 'test_data.csv'
    data_len = 800

    columns = generate_value_cols(['int','float'],data_len, separator)
    labels = generate_labels_2(columns, separator)

    label_name = labels[0]
    columns.append(labels)
    columns_train = list(map(lambda col: col[0:int(len(col)*3/4)], columns))
    columns_test = list(map(lambda col: col[int(len(col)*3/4):], columns))
    columns_to_file(columns_train, train_file_name, separator)
    columns_to_file(columns_test, test_file_name, separator)

    mdb = mindsdb.Predictor(name='test_one_label_prediction', log_level=mindsdb.CONST.INFO_LOG_LEVEL)
    mdb.learn(
        from_data=train_file_name,
        to_predict=label_name
    )
    print('!-------------  Learning ran successfully  -------------!')
    exit(0)

    mdb = mindsdb.MindsDB()
    results = mdb.predict(when_data=test_file_name)
    print('!-------------  Prediction from file ran successfully  -------------!')

    '''
    for i in range(len(columns_test[0])):
        features = {}
        for n in range(len(columns_test)):
            features[columns[n][0]] = columns_test[n][i]
        result = mdb.predict(when=features, model_name='test_one_label_prediction')
    '''


def test_dual_label_prediction():
    separator = ','
    data_file_name = 'test_data.csv'
    data_len = 1000

    columns = generate_value_cols(['int','float','int','int','float'],data_len, separator)
    labels1 = generate_labels_2(columns, separator)
    labels2 = generate_labels_2(columns, separator)

    label_names = [labels1[0],labels2[0]]
    columns.append(labels1)
    columns.append(labels2)
    columns_to_file(columns, data_file_name, separator)

    mdb = mindsdb.Predictor(name='test_dual_label_prediction')
    mdb.learn(
        from_data=data_file_name,
        to_predict=label_names
    )


def run_all_test():
    test_dual_label_prediction()
    test_one_label_prediction()

def run_all_test_that_should_work():
    test_one_label_prediction()

#run_all_test()
run_all_test_that_should_work()
