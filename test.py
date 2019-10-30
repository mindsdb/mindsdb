from mindsdb import Predictor
import sys
import pandas as pd
import json

mdb = Predictor(name='test_predictor')

'''
mdb.learn(to_predict=['rental_price', 'location'],from_data="https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/home_rentals.csv",use_gpu=True,stop_training_in_x_seconds=30, backend='ludwig')
p_arr = mdb.predict(when_data='https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/home_rentals.csv')

for p in p_arr:
    exp_s = p.epitomize()
    #exp = p.explain()
    #print(exp)
    print(exp_s)
'''
print(mdb.predict(when={'number_of_rooms': 3, 'number_of_bathrooms': 2, 'neighborhood': 'south_side', 'sqft':2411}, run_confidence_variation_analysis=True)[0].explain())

#print(json.dumps(mdb.get_model_data('test_predictor')))
