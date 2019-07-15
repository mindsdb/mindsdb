from run_example import run_example
from multiprocessing import Pool


datasets = ['default_of_credit','home_rentals']

with Pool(max(len(datasets),6)) as pool:
    pool.map(run_example,datasets)
