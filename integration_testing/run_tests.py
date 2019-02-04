from data_generators import *
import sys
import os
import itertools

# Not working for some reason, we need mindsdb in PYPATH for now
#sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')
#print(os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')

import mindsdb


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
    mdb = mindsdb.MindsDB(check_for_updates=False)
    mdb.learn(
        from_data=data_file_name,
        predict=label_name,
        model_name='test_datetime_timeseries'

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


    mdb = mindsdb.MindsDB(check_for_updates=False)

    mdb.learn(
        from_data=train_file_name,
        predict=label_name,
        model_name='test_one_label_prediction',
    )

    results = mdb.predict(from_data=test_file_name, model_name='test_one_label_prediction')
    result_predict = results.predicted_values[0][label_name]
    if result_predict is None:
        raise ValueError("Prediction failed!")
    


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

    mdb = mindsdb.MindsDB(check_for_updates=False)
    mdb.learn(
        from_data=data_file_name,
        predict=label_names,
        model_name='test_dual_label_prediction'
        )

def run_tests():
    test_one_label_prediction()

if __name__ == "__main__":
    run_tests()
