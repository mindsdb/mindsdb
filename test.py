from mindsdb import Predictor
import sys


mdb = Predictor(name='sensor123')

mdb.learn(to_predict='output',from_data="https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/sensor_data.csv",use_gpu=False,stop_training_in_x_seconds=40)

p_arr = mdb.predict(when_data='https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/sensor_data.csv')

pdct = mdb.predict(when={'sensor 1': 0.5, 'sensor 2': 2, 'sensor 3': 0, 'sensor4': 5})
print(pdct)

for p in p_arr:
    exp_s = p.epitomize()
    exp = p.explain()

    if len(exp['output']) > 0:
        print(exp)
        print(exp_s)
