from run_example import run_example
from generated_data_tests import *
import multiprocessing
import os


# Run the CI tests
os.system('cd ..; cd ci_tests; python3 full_test.py')

# Run the example datassts
datasets = [{
    'name':'default_of_credit',
    'sample':True,
    'expect_accuracy_above':77
},{
    'name':'imdb_movie_review',
    'sample':False,
    'expect_accuracy_above':84
},{
    'name':'cifar_100',
    'sample':True,
    'expect_accuracy_above': 40 # For full dataset: 69
}]


for dataset in datasets:
    dataset_name = dataset['name']

    res = run_example(dataset_name, sample=dataset['sample'])

    acc = res['accuracy']
    ex_acc = dataset['expect_accuracy_above']

    if acc < ex_acc:
        print('\n\n\n============WARNING===============\n\n\n')
        print(f'Expected an accuracy above {ex_acc} for dataset {dataset_name}.')
        print(f'Got accuracy of {acc} instead.')
        print('\n\n\n==================================\n\n\n')
    else:
        print('\n\n\n============SUCCESS===============\n\n\n')
        print(f'Example dataset {dataset_name}, ran with success')
        print(f'Got accuracy of {acc} !')
        print('\n\n\n==================================\n\n\n')

# Run the generated data tests
test_one_label_prediction_wo_strings()
test_timeseries()
test_multilabel_prediction()
test_one_label_prediction()

#with multiprocessing.Pool(max(len(datasets),6)) as pool:
#    pool.map(run_example,datasets)
