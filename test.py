from mindsdb import *
import sys, os

# First we initiate MindsDB

mdb = MindsDB()

sys.stdout = open(os.devnull, 'w')
sys.stderr = open(os.devnull, 'w')
# We tell mindsDB what we want to learn and from what data
mdb = MindsDB()
mdb.learn(
    from_file='Loan_data_with_error.csv',
    predict='installment', # the column we want to learn to predict given all the data in the file
    model_name='loan_data' # the name of this model
)
sys.stdout = sys.__stdout__
sys.stdout = sys.__stderr__
print('Done')
