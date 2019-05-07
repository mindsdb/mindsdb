import mindsdb

mdb = mindsdb.Predictor(name='real_estate_model') 
mdb.learn(from_data="https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/basic/home_rentals.csv", to_predict='rental_price')
