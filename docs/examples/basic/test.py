from mindsdb import *

mdb = Predictor(name='home_rentals')

mdb.learn(
    from_data="home_rentals.csv",
    # the path to the file where we can learn from, (note: can be url)
    to_predict='rental_price',  # the column we want to learn to predict given all the data in the file
)

#use the model to make predictions
result = mdb.predict(
    when={"number_of_rooms": 2, "sqft": 1100, 'location': 'great', 'days_on_market': 10, "number_of_bathrooms": 1})

print(result[0]['rental_price'])
print(result[0])
#3306 (5%)
#3837 (37%)
#3836 (26%)
#3559 (12%)
#3559 (4%)
#3837 (14%)
#3306 (4%)

when = {"sqft": 700}
result = mdb.predict(
    when=when)
print(result[0]['rental_price'])
#2205
#828
#3306
#3076
#2677
