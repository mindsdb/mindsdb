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

# We tell mindsDB what we want to learn and from what data
mdb.learn(
    from_file="home_rentals.csv", # the path to the file where we can learn from
    predict='rented_price', # the column we want to learn to predict given all the data in the file
    model_name='home_rentals' # the name of this model
)

