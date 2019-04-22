"""

This example we will walk you over the basics of MindsDB

The example code objective here is to predict the best retail price for a given property.

"""

from mindsdb import Predictor

# use the model to make predictions
result = Predictor(name='home_rentals_price').predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})

# you can now print the results
print('The predicted price is ${price} with {conf} confidence'.format(price=result[0]['rental_price'], conf=result[0]['prediction_confidence']))
