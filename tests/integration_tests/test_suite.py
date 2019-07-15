from run_example import run_example
import multiprocessing


datasets = ['default_of_credit','home_rentals']

for dataset in datasets:
    run_example(dataset)


#with multiprocessing.Pool(max(len(datasets),6)) as pool:
#    pool.map(run_example,datasets)
