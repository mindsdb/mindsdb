from mindsdb import Predictor
import sys


mdb = Predictor(name='hrep')
#mdb.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",use_gpu=True,stop_training_in_x_seconds=40)

p_arr = mdb.predict(when_data='https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv')

for p in p_arr:
    exp_s = p.simple_explain()
    exp = p.explain()

    if len(exp['rental_price']) > 1:
        print(len(exp['rental_price']))

    continue
    if exp['rental_price'][0]['confidence'] < 0.3:
        print(exp)
        print(exp_s)
