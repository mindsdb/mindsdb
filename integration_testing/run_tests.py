from data_generators import *
import sys
import os
import itertools
import logging

# Not working for some reason, we need mindsdb in PYPATH for now
#sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')
#print(os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')

import mindsdb
from mindsdb import CONST

types_that_fail = ['str','ascii']
types_that_work = ['int','float','date','datetime','timestamp']

'''
for i in range(1,len(types_that_work)+1):
    for combination in itertools.combinations(types_that_work, i)
'''
def test_timeseries():
    ts_hours = 36
    separator = ','
    data_len = 601

    columns = generate_value_cols(['date','int','float','date'],data_len, separator, ts_hours * 3600)
    labels = generate_labels_1(columns, separator)

    data_file_name = 'test_data.csv'
    label_name = labels[0]
    columns.append(labels)

    columns_to_file(columns, data_file_name, separator)
    mdb = mindsdb.Predictor(name='test_datetime_timeseries')
    mdb.learn(
        from_data=data_file_name,
        to_predict=label_name


        # timeseries specific args

        ,order_by = columns[2][0]
        ,window_size=ts_hours*(data_len/10)
        #,group_by = columns[0][0]
    )

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
