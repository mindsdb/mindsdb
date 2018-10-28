"""

This example we will walk you over the basics of MindsDB

The example code objective here is to:

- learn a model to predict the best retal price for a given property.

In order to to this we have a dataset "data_sources/home_rentals.csv"

"""

from mindsdb import *

# We tell mindsDB what we want to learn and from what data
MindsDB().learn(
    from_file="https://raw.githubusercontent.com/mindsdb/main/master/docs/examples/basic/home_rentals.csv", # the path to the file where we can learn from
    predict='rental_price', # the column we want to learn to predict given all the data in the file
    model_name='home_rentals' # the name of this model
)

