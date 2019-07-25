"""

This example we will walk you over the basics of MindsDB

The example code objective here is to:

- learn a model to predict the best retal price for a given property.

In order to to this we have a dataset "data_sources/home_rentals.csv" (or download from https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv)

"""

from mindsdb import Predictor


# We tell mindsDB what we want to learn and from what data
Predictor(name='home_rentals_price').learn(
    to_predict='days_on_market', # the column we want to learn to predict given all the data in the file
    from_data="home_rentals.csv" # the path to the file where we can learn from, (note: can be url)
)
