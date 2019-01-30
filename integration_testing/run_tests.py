from data_generators import *
import sys
import os

# Not working for some reason, we need mindsdb in PYPATH for now
#sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')
#print(os.path.dirname(os.path.realpath(__file__)) + '/../mindsdb/__init__.py')

import mindsdb


mdb = mindsdb.MindsDB()
separator = ','

columns = generate_value_cols(['int','float','int'],20, separator)
labels = generate_labels_3(columns, separator)

data_file_name = 'test_data.csv'
label_name = labels[0]
columns.append(labels)

# columns_to_file(columns, data_file_name, separator)

mdb.learn(
    from_data=data_file_name,
    predict=label_name,
    model_name='test_model')
