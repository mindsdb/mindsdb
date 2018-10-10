"""

This example we will walk you over the basics of MindsDB

The example code objective here is to:

- learn a model to predict the best retal price for a given property.

In order to to this we have a dataset "data_sources/home_rentals.csv"

"""

from mindsdb import *

#


# First we initiate MindsDB
mdb = MindsDB()

# Here we use the model to make predictions (NOTE: You need to run train.py first)
result = mdb.predict(predict='Main_Engine_Fuel_Consumption_MT_day', model_name='fuel')

# you can now print the results
print('The predicted main engine fuel consumption')

print(result)
