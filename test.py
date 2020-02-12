from mindsdb import Predictor
import sys
import pandas as pd
import json
import time


mdb = Predictor(name='test_predictor')
#'rental_price',
#mdb.learn(to_predict=['neighborhood'],from_data="https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/home_rentals.csv",use_gpu=False,stop_training_in_x_seconds=3000, backend='lightwood', unstable_parameters_dict={'use_selfaware_model':True})

p = mdb.predict(when={'number_of_rooms': 3, 'number_of_bathrooms': 2, 'neighborhood': 'south_side', 'sqft':2411}, run_confidence_variation_analysis=True, use_gpu=True)
e = p[0].explaination
print(e)

p_arr = mdb.predict(when_data='https://mindsdb-example-data.s3.eu-west-2.amazonaws.com/home_rentals.csv', use_gpu=True)

for p in p_arr:
    e = p.explaination

p = mdb.predict(when={'number_of_rooms': 3, 'number_of_bathrooms': 2, 'neighborhood': 'south_side', 'sqft':2411}, run_confidence_variation_analysis=True, use_gpu=True)

for p in p_arr:
    exp_s = p.epitomize()
    exp = p.explaination
    print(exp_s)

    print(p.as_dict())
    print(p.as_list())
    print(p.raw_predictions())
