"""

"""

from mindsdb import *

# Here we use the model to make predictions (NOTE: You need to run train.py first)
result = MindsDB().predict(predict='Main_Engine_Fuel_Consumption_MT_day', model_name='fuel', when_data='fuel_predict.csv')

# you can now print the results
print('The predicted main engine fuel consumption')
print(result.predicted_values)
