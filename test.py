from mindsdb import Predictor
import sys
import pandas as pd
import json

mdb = Predictor(name='usci_texas_gen3_2')

'''
mdb.learn(to_predict='rental_price',from_data="https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/home_rentals.csv",use_gpu=True,stop_training_in_x_seconds=30, backend='ludwig')
p_arr = mdb.predict(when_data='https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/home_rentals.csv')


for p in p_arr:
    exp_s = p.epitomize()
    #exp = p.explain()
    #print(exp)
    print(exp_s)
'''
print(mdb.get_model_data('usci_texas_gen3_2'))
