from mindsdb import Predictor


# We tell mindsDB what we want to learn and from what data
Predictor(name='home_rentals_price').learn(
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv" 
# the path to the file where we can learn from, (note: can be url)
)
