from run_example import run_example
from generated_data_tests import *
import multiprocessing


datasets = ['default_of_credit','home_rentals']

for dataset in datasets:
    run_example(dataset)

test_one_label_prediction_wo_strings()
test_timeseries()
test_multilabel_prediction()
test_one_label_prediction()

#with multiprocessing.Pool(max(len(datasets),6)) as pool:
#    pool.map(run_example,datasets)
