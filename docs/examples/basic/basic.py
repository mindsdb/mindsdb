"""

This example we will walk you over the basics of MindsDB

The example code objective here is to:

- learn a model to predict the best retal price for a given property.

In order to to this we have a dataset "data_sources/home_rentals.csv"

"""
from mindsdb import *
import os

# First we initiate MindsDB
mdb = MindsDB()

# We tell mindsDB what we want to learn and from what data
mdb.learn(
    from_file="home_rentals.csv", # the path to the file where we can learn from
    predict='rented_price', # the column we want to learn to predict given all the data in the file
    model_name='home_rentals' # the name of this model
)


# Here we use the model to make predictions
result = mdb.predict(predict='rented_price', when={'number_of_rooms': 2,'number_of_bathrooms':1}, model_name='home_rentals')

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result.predicted_values[0]['rented_price'], conf=result.predicted_values[0]['prediction_confidence']))
